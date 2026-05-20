"""Kafka を PySpark Structured Streaming + Trigger.AvailableNow で取得するバッチ consumer。

Envelope 採用後は SCHEMAS が 1 件 (Envelope) のみで、1 ジョブ = 1 StreamingQuery 構成:

    1 つの EMR ジョブ (= 1 spark-submit プロセス)
    └ SparkSession
        └ StreamingQuery (Envelope)   topic → sample_events_event

Envelope は Event を共通フィールドとして持ち、`oneof extra` で追加情報を 1 つだけ含む。
consumer は extra の oneof case 名を extra_type 列に書き込み、どの case にも該当しない
(producer が未知 field 番号で送ってきた) 行は DLQ に振る。oneof case の列挙は起動時に
events.desc から動的に行うため、新しい extra 型を追加しても本ファイルは変更不要。

EMR Serverless 7.13.0 / EMR on EKS 7.13.0 / ローカル PySpark 3.5.6 で動作する。
"""

import argparse
import logging

from google.protobuf import descriptor_pb2
from pyspark.sql import Column, DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.protobuf.functions import from_protobuf
from pyspark.sql.streaming.query import StreamingQuery

from constants import (
    BOOTSTRAP_SERVERS,
    DEFAULT_STARTING_OFFSETS,
    DESCRIPTOR_FILE,
    DLQ_REASON_DESERIALIZE_ERROR,
    DLQ_REASON_MISSING_SCHEMA,
    DLQ_REASON_MISSING_VERSION,
    DLQ_REASON_SCHEMA_MISMATCH,
    DLQ_REASON_UNKNOWN_EXTRA,
    DLQ_REASON_UNSUPPORTED_VERSION,
    DLQ_TARGET_TABLE,
    PROTO_SCHEMA_HEADER_KEY,
    PROTO_VERSION_HEADER_KEY,
    SCHEMAS,
    SchemaConfig,
)

EXTRA_ONEOF_NAME = "extra"

logger = logging.getLogger("consumer")


def build_spark() -> SparkSession:
    """全 StreamingQuery が共有する SparkSession を生成する。

    Executor プールはこのセッションの所有物として 1 ジョブ内に 1 つ。executor 数や
    core 数は spark-submit の --conf で調整する。
    """
    return SparkSession.builder.appName("kafka-batch-consumer").getOrCreate()  # pyright: ignore[reportAttributeAccessIssue]


def discover_oneof_cases(desc_path: str, message_full_name: str, oneof_name: str) -> list[str]:
    """events.desc を読んで指定 message の oneof 配下にある field 名を列挙する。

    Envelope.extra に oneof case を追加しても consumer.py のコード変更が不要になるよう、
    起動時に case 名を動的取得して Spark 側 extra_type 列の計算式を組み立てる。
    対象の message が見つからない場合は RuntimeError。oneof 自体が無い message は [] を返す。
    """
    fds = descriptor_pb2.FileDescriptorSet()
    with open(desc_path, "rb") as f:
        fds.ParseFromString(f.read())
    for fd in fds.file:
        for msg in fd.message_type:
            if f"{fd.package}.{msg.name}" != message_full_name:
                continue
            oneof_idx = next(
                (i for i, o in enumerate(msg.oneof_decl) if o.name == oneof_name),
                None,
            )
            if oneof_idx is None:
                return []
            return [
                field.name
                for field in msg.field
                if field.HasField("oneof_index") and field.oneof_index == oneof_idx
            ]
    raise RuntimeError(f"message '{message_full_name}' not found in {desc_path}")


def _build_extra_type_expr(extra_cases: list[str]) -> Column:
    """payload.<case> のうち非 NULL の最初の case 名を返す Spark 式を組み立てる。

    proto3 oneof は Spark protobuf でそれぞれの case が nullable struct としてフラットに
    展開され、set されていない case の struct 全体が NULL になる。どの case も NULL の
    行 (= producer が未知 field 番号で送ってきた行) は extra_type = NULL となり、
    後段の route 判定で unknown_extra に振り分けられる。
    """
    expr: Column = F.lit(None).cast("string")
    for case in reversed(extra_cases):
        expr = F.when(F.col(f"payload.{case}").isNotNull(), F.lit(case)).otherwise(expr)
    return expr


def start_query_for(
    spark: SparkSession,
    config: SchemaConfig,
    starting_offsets: str,
    extra_cases: list[str],
) -> StreamingQuery:
    """1 つの SchemaConfig に対応する独立した StreamingQuery を起動する (non-blocking)。

    プランは「Kafka topic 読み込み → header から proto-schema / proto-version 抽出 →
    from_protobuf 1 回 → foreachBatch で route 列に応じて append / DLQ」と軽量。
    start() を呼んだ瞬間に query が非同期起動するので、ループで複数呼ぶと並列実行になる。
    """
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", config.topic)
        .option("startingOffsets", starting_offsets)
        .option("includeHeaders", "true")
        .load()
    )

    # PERMISSIVE モードでデシリアライズ失敗時は payload が null になる (DLQ 行き)。
    # headers は array<struct<key:string, value:binary>>。各ヘッダーの value (binary) を
    # UTF-8 string にデコードして cast し、route 判定に使う。
    schema_expr = F.expr(f"filter(headers, h -> h.key = '{PROTO_SCHEMA_HEADER_KEY}')[0].value")
    version_expr = F.expr(f"filter(headers, h -> h.key = '{PROTO_VERSION_HEADER_KEY}')[0].value")
    parsed = raw.select(
        F.col("value").alias("rawdata"),
        from_protobuf(
            F.col("value"),
            config.protobuf_full_name,
            DESCRIPTOR_FILE,
            {"mode": "PERMISSIVE"},
        ).alias("payload"),
        schema_expr.cast("string").alias("proto_schema"),
        version_expr.cast("string").cast("int").alias("proto_version"),
    )

    writer = _build_batch_writer(
        config.target_table,
        config.schema_name,
        config.max_supported_version,
        extra_cases,
    )

    return (
        parsed.writeStream.queryName(f"consumer-{config.schema_name}")
        .option("checkpointLocation", config.checkpoint_location)
        .outputMode("append")
        .trigger(availableNow=True)
        .foreachBatch(writer)
        .start()
    )


def _build_batch_writer(
    target_table: str,
    expected_schema_name: str,
    max_supported_version: int,
    extra_cases: list[str],
):
    """target_table 専用の foreachBatch コールバックを返す。

    DLQ は全 query 共通の 1 テーブル。Envelope 採用後は SCHEMAS が 1 件のため
    target_table も 1 つだが、汎用性のために構造は前構成から踏襲する。

    行の振り分けは次の優先順 (相互排他):
      1. proto_schema IS NULL                 → DLQ (missing_schema)
      2. proto_version IS NULL                → DLQ (missing_version)
      3. proto_schema != expected             → DLQ (schema_mismatch)
      4. proto_version > max_supported        → DLQ (unsupported_version)
      5. payload IS NULL                      → DLQ (deserialize_error)
      6. extra_type IS NULL (どの oneof case にも入らない) → DLQ (unknown_extra)
      7. 上記以外                             → target_table へ append
    """

    def _write(batch_df: DataFrame, batch_id: int) -> None:
        # まず extra_type 列を計算し、続けて route 列を when/otherwise 連鎖で決める。
        enriched = batch_df.withColumn("extra_type", _build_extra_type_expr(extra_cases))
        routed = enriched.withColumn(
            "route",
            F.when(F.col("proto_schema").isNull(), F.lit(DLQ_REASON_MISSING_SCHEMA))
            .when(F.col("proto_version").isNull(), F.lit(DLQ_REASON_MISSING_VERSION))
            .when(
                F.col("proto_schema") != F.lit(expected_schema_name),
                F.lit(DLQ_REASON_SCHEMA_MISMATCH),
            )
            .when(
                F.col("proto_version") > F.lit(max_supported_version),
                F.lit(DLQ_REASON_UNSUPPORTED_VERSION),
            )
            .when(F.col("payload").isNull(), F.lit(DLQ_REASON_DESERIALIZE_ERROR))
            .when(F.col("extra_type").isNull(), F.lit(DLQ_REASON_UNKNOWN_EXTRA))
            .otherwise(F.lit("ok")),
        )
        routed.cache()
        try:
            # 成功行: target_table のスキーマ (event_id, event_datetime, extra_type, rawdata)
            # に合わせて明示 select する。route で OK 判定済みなので extra_type は非 NULL。
            valid = routed.where(F.col("route") == "ok").select(
                F.col("payload.event.id").alias("event_id"),
                F.to_timestamp(F.col("payload.event.datetime")).alias("event_datetime"),
                F.col("extra_type"),
                F.col("rawdata"),
            )
            valid.writeTo(target_table).append()

            # 失敗行: DLQ (全 schema 共通) に append。count > 0 のときだけ書く。
            # schema_name は header 由来の proto_schema をそのまま流す (DLQ 側列は nullable)。
            # missing_schema 行は NULL になるが、reason 列で 'missing_schema' と識別できる。
            invalid = routed.where(F.col("route") != "ok").select(
                F.current_timestamp().alias("failed_at"),
                F.col("proto_schema").alias("schema_name"),
                F.col("rawdata"),
                F.col("route").alias("reason"),
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
                logger.info("target=%s batch_id=%s appended", target_table, batch_id)
        finally:
            routed.unpersist()

    return _write


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
        extra_cases = discover_oneof_cases(DESCRIPTOR_FILE, config.protobuf_full_name, EXTRA_ONEOF_NAME)
        logger.info(
            "starting query for %s: topic=%s table=%s extra_cases=%s",
            config.schema_name,
            config.topic,
            config.target_table,
            extra_cases,
        )
        q = start_query_for(spark, config, args.starting_offsets, extra_cases)
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
