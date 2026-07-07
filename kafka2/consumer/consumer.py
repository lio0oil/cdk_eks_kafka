"""Kafka を PySpark Structured Streaming + Trigger.AvailableNow で取得するバッチ consumer。

    1 つの EMR ジョブ (= 1 spark-submit プロセス)
    └ SparkSession
        └ StreamingQuery   topic → sample-events-event/ (S3, parquet)

Envelope は common / user / content を持ち、content.historyrecords (repeated
HistoryRecord) を 1 レコードずつに分解して classification 列を取り出す。各行の
content.historyrecords はデシリアライズした構造のまま、その行の HistoryRecord 以外を
削除して 1 件だけ残す (common / user は元の Envelope のまま)。classification / year /
month / day (処理時刻由来) で partition した parquet として S3 に書く。日付を処理時刻に
しているのは、再実行・バックフィル時に古い日付 partition へ散らばらず実行日の partition
に集約させるため。classification は重複が前提のため、同じ classification の行は同じ
ディレクトリに複数ファイルとして蓄積される。

EMR Serverless 7.13.0 / EMR on EKS 7.13.0 / ローカル PySpark 3.5.6 で動作する。
"""

import argparse
import logging
import zlib

from pyspark.sql import DataFrame, SparkSession
from pyspark.sql import functions as F
from pyspark.sql.protobuf.functions import from_protobuf
from pyspark.sql.streaming.query import StreamingQuery
from pyspark.sql.types import BinaryType

from constants import (
    BOOTSTRAP_SERVERS,
    CHECKPOINT_LOCATION,
    DEFAULT_STARTING_OFFSETS,
    DESCRIPTOR_FILE,
    DLQ_OUTPUT_PATH,
    DLQ_REASON_DESERIALIZE_ERROR,
    DLQ_REASON_ZLIB_ERROR,
    MIN_PARTITIONS,
    OUTPUT_BUCKET,
    OUTPUT_PATH,
    PROTOBUF_FULL_NAME,
    TOPIC,
)

logger = logging.getLogger("consumer")

# _write_valid / _write_dlq が "Failed to update S3. ..." で書き込み失敗を既にログ済みか
# どうかを main() が判定するためのフラグ。同じ失敗を query.exception() 経由で二重に
# ログしないようにするためだけに使う (S3 書き込み以外の失敗ではこのフラグは立たないため
# main() 側で通常通り query.exception() の内容を出力する)。
_s3_write_failed = False


def _zlib_decompress(data: bytes | None) -> bytes | None:
    """producer が zlib 圧縮した value を展開する。

    壊れたデータ (zlib 展開できない) は None を返す。呼び出し側 (_write_batch) が
    decompressed IS NULL で判定し、zlib_decompress_error として DLQ 行きにする
    (from_protobuf のデコード失敗である deserialize_error とは別の reason に分ける)。
    """
    if data is None:
        return None
    try:
        return zlib.decompress(data)
    except zlib.error:
        return None


zlib_decompress = F.udf(_zlib_decompress, BinaryType())


def build_spark() -> SparkSession:
    """SparkSession を生成する。"""
    return SparkSession.builder.appName("kafka-batch-consumer").getOrCreate()  # pyright: ignore[reportAttributeAccessIssue]


def start_query(spark: SparkSession, starting_offsets: str) -> StreamingQuery:
    """StreamingQuery を起動する (non-blocking)。

    プランは「Kafka topic 読み込み → from_protobuf 1 回 → foreachBatch で
    route 列に応じて append / DLQ」と軽量。producer は header を付けないため、
    Kafka header は読まない。
    """
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", TOPIC)
        .option("startingOffsets", starting_offsets)
        .option("minPartitions", MIN_PARTITIONS)
        .load()
    )

    # producer が zlib 圧縮しているため、from_protobuf に渡す前に展開する。
    # decompressed 列を残しておき、zlib 展開失敗 (decompressed IS NULL) と
    # protobuf デコード失敗 (payload IS NULL) を _write_batch で区別できるようにする。
    # PERMISSIVE モードでデシリアライズ失敗時は payload が null になる (DLQ 行き)。
    parsed = raw.select(
        F.col("value").alias("rawdata"),
        zlib_decompress(F.col("value")).alias("decompressed"),
    ).withColumn(
        "payload",
        from_protobuf(F.col("decompressed"), PROTOBUF_FULL_NAME, DESCRIPTOR_FILE, {"mode": "PERMISSIVE"}),
    )

    return (
        parsed.writeStream.queryName("consumer-envelope")
        .option("checkpointLocation", CHECKPOINT_LOCATION)
        .outputMode("append")
        .trigger(availableNow=True)
        .foreachBatch(_write_batch)
        .start()
    )


def _write_valid(valid: DataFrame) -> None:
    """デコード済み Envelope を classification 毎の HistoryRecord 1 件に分解して書く。

    content.historyrecords (repeated) を explode し、各行につき classification を
    取り出す。envelope 列は payload (Envelope 全体) の content.historyrecords だけを
    「選択した HistoryRecord のみ残したもの」に withField で差し替えて作る。common /
    user に限らず、Envelope に新しいフィールドが増えても手動で列挙し直す必要がない。
    partition は classification / year / month / day (処理日時由来。再実行・バックフィル
    時に古い日付 partition へ散らばらず、実行日の partition に集約されるようにするため
    Envelope.common.datetime ではなく処理時刻を使う)。

    書き込み直前に classification で repartition する。読み込み側の並列数 (タスク数) を
    上げてもファイル数が比例して増えないようにするため。repartition しない場合、1 タスクが
    複数 classification の行を持ち得るため、そのタスクが担当する classification の種類数
    だけファイルを書いてしまう (タスク数 × classification 種類数で最悪ケースが増大する)。
    classification で repartition すれば、同じ classification の行が同じタスクに集約され、
    書き込みファイル数は classification の種類数 (現状 60〜100 程度) 前後に収まる。

    classification 毎に個別の write() として呼ぶ (1 回の write にまとめない)。driver 側で
    ループするため、どの classification の書き込みで失敗したかを確実にログへ残せる
    (executor 側で実行される mapPartitions 等に頼ると、ログ設定が引き継がれず出力されない
    上に、DataFrame の再評価で書き込みが重複するリスクがあるため採用しない)。
    """
    exploded = valid.select(
        F.current_timestamp().alias("processed_at"),
        F.col("payload"),
        F.explode("payload.content.historyrecords").alias("historyrecord"),
    )
    envelope_col = F.col("payload").withField(
        "content",
        F.col("payload.content").withField("historyrecords", F.array(F.col("historyrecord"))),
    )
    partitioned = exploded.select(
        F.col("historyrecord.classification").alias("classification"),
        envelope_col.alias("envelope"),
        F.date_format("processed_at", "yyyy").alias("year"),
        F.date_format("processed_at", "MM").alias("month"),
        F.date_format("processed_at", "dd").alias("day"),
    ).repartition("classification")

    partitioned.cache()
    try:
        classifications = [row["classification"] for row in partitioned.select("classification").distinct().collect()]
        failures: list[Exception] = []
        for classification in classifications:
            subset = partitioned.where(F.col("classification") == classification)
            try:
                subset.write.mode("append").option("compression", "zstd").partitionBy(
                    "classification", "year", "month", "day"
                ).parquet(OUTPUT_PATH)
            except Exception as e:
                logger.error("Failed to update S3. %s,%s,%s", OUTPUT_BUCKET, classification, e)
                failures.append(e)
        # 1 件でも失敗した classification があればバッチ全体を失敗させる (checkpoint を
        # 進めず次回再処理させるため)。ただし他の classification は失敗の有無に関わらず
        # 全件試行してから raise する (1 件目の失敗で残りが未試行のまま終わらないように)。
        if failures:
            global _s3_write_failed
            _s3_write_failed = True
            raise failures[0]
    finally:
        partitioned.unpersist()


def _write_dlq(invalid: DataFrame) -> None:
    """DLQ 行を reason / year / month / day (failed_at 由来) で partition して書く。

    _write_valid と同様、reason 毎に個別の write() として driver 側でループし、
    どの reason の書き込みで失敗したかを確実にログへ残す。
    """
    partitioned = (
        invalid.withColumn("year", F.date_format("failed_at", "yyyy"))
        .withColumn("month", F.date_format("failed_at", "MM"))
        .withColumn("day", F.date_format("failed_at", "dd"))
    )
    partitioned.cache()
    try:
        reasons = [row["reason"] for row in partitioned.select("reason").distinct().collect()]
        failures: list[Exception] = []
        for reason in reasons:
            subset = partitioned.where(F.col("reason") == reason)
            try:
                subset.write.mode("append").option("compression", "zstd").partitionBy(
                    "reason", "year", "month", "day"
                ).parquet(DLQ_OUTPUT_PATH)
            except Exception as e:
                logger.error("Failed to update S3. %s,%s,%s", OUTPUT_BUCKET, reason, e)
                failures.append(e)
        if failures:
            global _s3_write_failed
            _s3_write_failed = True
            raise failures[0]
    finally:
        partitioned.unpersist()


def _write_batch(batch_df: DataFrame, batch_id: int) -> None:
    """foreachBatch コールバック。

    行の振り分けは次の優先順 (相互排他):
      1. decompressed IS NULL → DLQ (zlib_decompress_error)
      2. payload IS NULL      → DLQ (deserialize_error)
      3. 上記以外             → OUTPUT_PATH へ classification 毎に append
    """
    routed = batch_df.withColumn(
        "route",
        F.when(F.col("decompressed").isNull(), F.lit(DLQ_REASON_ZLIB_ERROR))
        .when(F.col("payload").isNull(), F.lit(DLQ_REASON_DESERIALIZE_ERROR))
        .otherwise(F.lit("ok")),
    )
    routed.cache()
    try:
        valid = routed.where(F.col("route") == "ok")
        _write_valid(valid)

        # 失敗行: DLQ に append。count > 0 のときだけ書く。
        invalid = routed.where(F.col("route") != "ok").select(
            F.current_timestamp().alias("failed_at"),
            F.col("rawdata"),
            F.col("route").alias("reason"),
        )
        invalid_count = invalid.count()
        if invalid_count > 0:
            logger.warning(
                "DLQ: batch_id=%s, %s record(s) → %s",
                batch_id,
                invalid_count,
                DLQ_OUTPUT_PATH,
            )
            _write_dlq(invalid)
        else:
            logger.info("batch_id=%s appended", batch_id)
    finally:
        routed.unpersist()


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Kafka を取得し、classification 別 parquet へ書き込む",
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

    spark = build_spark()

    logger.info("starting query: topic=%s output=%s", TOPIC, OUTPUT_PATH)
    query = start_query(spark, args.starting_offsets)

    # AvailableNow なので query は処理完了時に自然終了する。
    query.awaitTermination()
    if query.exception() is not None:
        # S3 書き込み失敗 (_s3_write_failed) は _write_valid / _write_dlq が既に
        # "Failed to update S3. ..." でログ済みなので二重に出力しない。
        # それ以外の失敗 (S3 書き込みに至る前の段階でのエラー等) はここでしかログされない
        # ため、query.exception() の内容をそのまま出力する。
        if not _s3_write_failed:
            logger.error("query=%s failed: %s", query.name, query.exception())
    else:
        progress = query.lastProgress
        if progress is not None:
            logger.info("query=%s finished: numInputRows=%s", query.name, progress.get("numInputRows"))

    spark.stop()


if __name__ == "__main__":
    main()
