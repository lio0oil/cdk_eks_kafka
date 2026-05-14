"""Kafka を PySpark Structured Streaming + Trigger.AvailableNow で取得するバッチ consumer。

案 R-1 (テーブル分割 + 並列 streaming query)。1 つの EMR ジョブ内で SCHEMAS の各エントリに
対して独立した StreamingQuery を起動し、それぞれが別 topic を subscribe → 別 Iceberg
テーブルに書き込む構造:

    1 つの EMR ジョブ (= 1 spark-submit プロセス)
    ├ SparkSession (1 つ。全 query が共有する Executor プールのオーナー)
    │   ├ StreamingQuery (SCHEMAS[0])   topic → target_table
    │   ├ StreamingQuery (SCHEMAS[1])   ... (将来追加された場合)
    │   └ ...
    └ Executor プール (全 query で共有)

start() は non-blocking なので、SCHEMAS を for ループで回して全部 start() した時点で
全 query が並列実行中になる。最後に awaitTermination でまとめて完了待ち。

EMR Serverless 7.13.0 / EMR on EKS 7.13.0 / ローカル PySpark 3.5.6 で動作する。
"""

import argparse
import logging

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.protobuf.functions import from_protobuf
from pyspark.sql.streaming import StreamingQuery

from constants import (
    BOOTSTRAP_SERVERS,
    DEFAULT_STARTING_OFFSETS,
    DESCRIPTOR_FILE,
    DLQ_TARGET_TABLE,
    SCHEMAS,
    SchemaConfig,
)

logger = logging.getLogger("consumer")


def build_spark() -> SparkSession:
    """全 StreamingQuery が共有する SparkSession を生成する。

    Executor プールはこのセッションの所有物として 1 ジョブ内に 1 つ。executor 数や
    core 数は spark-submit の --conf で調整する。
    """
    return SparkSession.builder.appName("kafka-batch-consumer").getOrCreate()


def start_query_for(spark: SparkSession, config: SchemaConfig, starting_offsets: str) -> StreamingQuery:
    """1 つの SchemaConfig に対応する独立した StreamingQuery を起動する (non-blocking)。

    プランは「Kafka topic 読み込み → from_protobuf 1 回 → foreachBatch で MERGE」と軽量。
    start() を呼んだ瞬間に query が非同期起動するので、ループで複数呼ぶと並列実行になる。
    """
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", config.topic)
        .option("startingOffsets", starting_offsets)
        .load()
    )

    # PERMISSIVE モードでデシリアライズ失敗時は payload が null になる (DLQ 行き)。
    parsed = raw.select(
        F.col("value").alias("rawdata"),
        from_protobuf(
            F.col("value"),
            config.protobuf_full_name,
            DESCRIPTOR_FILE,
            {"mode": "PERMISSIVE"},
        ).alias("payload"),
    )

    upsert = _build_upsert(config.target_table)

    return (
        parsed.writeStream
        .queryName(f"consumer-{config.schema_name}")
        .option("checkpointLocation", config.checkpoint_location)
        .outputMode("append")
        .trigger(availableNow=True)
        .foreachBatch(upsert)
        .start()
    )


def _build_upsert(target_table: str):
    """target_table 専用の foreachBatch コールバックを返す。

    各 query が別 target_table を持つので、commit は別の snapshot 履歴に記録され
    並列実行可能 (Iceberg の楽観ロック競合なし)。DLQ は全 query 共通の 1 テーブル。
    """

    def _upsert(batch_df: DataFrame, batch_id: int) -> None:
        spark = batch_df.sparkSession
        batch_df.cache()
        try:
            # 成功行: payload を全フィールド展開し、datetime を timestamp 化、id で重複除去
            valid = (
                batch_df.where(F.col("payload").isNotNull())
                .select(
                    F.col("payload.*"),
                    F.col("rawdata"),
                )
                .withColumn("datetime", F.to_timestamp(F.col("datetime")))
                .dropDuplicates(["id"])
            )
            valid.createOrReplaceTempView("_consumer_batch_staging")
            spark.sql(
                f"MERGE INTO {target_table} t "
                "USING _consumer_batch_staging s "
                "ON t.id = s.id "
                "WHEN NOT MATCHED THEN INSERT *"
            )

            # 失敗行: DLQ (全 schema 共通) に append。count > 0 のときだけ書く。
            invalid = batch_df.where(F.col("payload").isNull()).select(
                F.current_timestamp().alias("failed_at"),
                F.col("rawdata"),
            )
            invalid_count = invalid.count()
            if invalid_count > 0:
                logger.warning(
                    "DLQ: target=%s batch_id=%s, %s record(s) → %s",
                    target_table,
                    batch_id,
                    invalid_count,
                    DLQ_TARGET_TABLE,
                )
                invalid.writeTo(DLQ_TARGET_TABLE).append()
            else:
                logger.info("target=%s batch_id=%s upserted", target_table, batch_id)
        finally:
            batch_df.unpersist()

    return _upsert


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Kafka を並列 streaming query で取得し、schema 毎の Iceberg テーブルに書き込む",
    )
    parser.add_argument(
        "--starting-offsets",
        default=DEFAULT_STARTING_OFFSETS,
        choices=["earliest", "latest"],
        help=f"初回起動時のみ参照されるフォールバック (default: {DEFAULT_STARTING_OFFSETS})",
    )
    return parser.parse_args(argv)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = parse_args()

    logger.info("starting consumer with %s schema(s)", len(SCHEMAS))

    spark = build_spark()

    # SCHEMAS をループして各 SchemaConfig に対し独立した StreamingQuery を起動する。
    # start() は non-blocking なので、ループ終了時には全 query が並列実行中になる。
    queries: list[StreamingQuery] = []
    for config in SCHEMAS:
        logger.info(
            "starting query for %s: topic=%s table=%s",
            config.schema_name,
            config.topic,
            config.target_table,
        )
        q = start_query_for(spark, config, args.starting_offsets)
        queries.append(q)

    logger.info("started %s parallel streaming queries", len(queries))

    # 全 query の完了を待つ。AvailableNow なので各 query は処理完了時に自然終了する。
    # 1 query が失敗しても他は走り続けるので、最後に exception を確認する。
    for q in queries:
        q.awaitTermination()
        if q.exception() is not None:
            logger.error("query=%s failed: %s", q.name, q.exception())
        else:
            progress = q.lastProgress
            if progress is not None:
                logger.info(
                    "query=%s finished: numInputRows=%s",
                    q.name,
                    progress.get("numInputRows"),
                )

    spark.stop()


if __name__ == "__main__":
    main()
