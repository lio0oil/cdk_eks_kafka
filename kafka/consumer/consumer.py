"""Kafka を PySpark Structured Streaming + Trigger.AvailableNow で取得するバッチ consumer。

1 つの topic に構造の異なる複数の ProtoBuf 定義が相乗りで流れる構成をサポートする。
1 topic = 1 StreamingQuery で、query 内で Kafka header の proto-schema を見て variant ごとに
from_protobuf を出し分け、各 variant のフィールドを共通カラム (event_id / event_datetime /
extra_type) へ写像して同一テーブルに書く:

    1 つの EMR ジョブ (= 1 spark-submit プロセス)
    └ SparkSession
        └ StreamingQuery (topic=sample-events-event)
            ├ proto-schema=Envelope → from_protobuf(Envelope) → event_id/event_datetime/extra_type
            └ proto-schema=Metric   → from_protobuf(Metric)   → event_id/event_datetime/extra_type
                                       (どちらも sample_events_event へ append)

header の proto-schema がどの variant にも一致しない行や、oneof extra のどの case にも
入らない行は DLQ テーブル sample_events_dlq に reason 付きで隔離する。Envelope の oneof
case 列挙は起動時に events.desc から動的に行うため、Envelope.extra に case を追加しても
本ファイルは変更不要。新しい構造の proto 定義を相乗りさせる場合は constants.py の
TopicConfig.variants に SchemaVariant を 1 件足す (写像元フィールドを指定する)。

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
    TOPICS,
    SchemaVariant,
    TopicConfig,
)

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


def _payload_alias(index: int) -> str:
    return f"payload_{index}"


def _coalesce_by_schema(variants: list[SchemaVariant], value_for, result_type: str) -> Column:
    """proto_schema が一致した variant の値を返す when/otherwise 連鎖を組み立てる。

    value_for(index, variant) は対象 variant の Column を返す callable。どの variant にも
    一致しない行は NULL (cast 済み) になる (route 判定で schema_mismatch に振り分けられる)。
    """
    expr: Column = F.lit(None).cast(result_type)
    for index, variant in reversed(list(enumerate(variants))):
        expr = F.when(
            F.col("proto_schema") == F.lit(variant.schema_name),
            value_for(index, variant),
        ).otherwise(expr)
    return expr


def _extra_type_value(index: int, variant: SchemaVariant, oneof_cases: list[list[str]]) -> Column:
    """1 つの variant の extra_type 値を表す Column を返す。

    extra_oneof を持つ variant (Envelope) は payload.<case> のうち非 NULL の最初の case 名。
    extra_type_field を持つ variant (Metric) はそのフィールド値をそのまま使う。
    """
    alias = _payload_alias(index)
    if variant.extra_oneof is not None:
        inner: Column = F.lit(None).cast("string")
        for case in reversed(oneof_cases[index]):
            inner = F.when(F.col(f"{alias}.{case}").isNotNull(), F.lit(case)).otherwise(inner)
        return inner
    return F.col(f"{alias}.{variant.extra_type_field}").cast("string")


def start_query_for(
    spark: SparkSession,
    topic_config: TopicConfig,
    starting_offsets: str,
    oneof_cases: list[list[str]],
) -> StreamingQuery:
    """1 つの TopicConfig に対応する独立した StreamingQuery を起動する (non-blocking)。

    topic を 1 回 subscribe し、variant ごとに from_protobuf を 1 つずつ出して payload_<i> に
    展開する。proto-schema header で一致した variant の payload だけを採用し、共通カラムへ
    写像して foreachBatch で append / DLQ に振り分ける。
    """
    raw = (
        spark.readStream.format("kafka")
        .option("kafka.bootstrap.servers", BOOTSTRAP_SERVERS)
        .option("subscribe", topic_config.topic)
        .option("startingOffsets", starting_offsets)
        .option("includeHeaders", "true")
        .load()
    )

    # PERMISSIVE モードでデシリアライズ失敗時は payload が null になる (DLQ 行き)。
    # headers は array<struct<key:string, value:binary>>。各ヘッダーの value (binary) を
    # UTF-8 string にデコードして cast し、route 判定に使う。
    schema_expr = F.expr(f"filter(headers, h -> h.key = '{PROTO_SCHEMA_HEADER_KEY}')[0].value")
    version_expr = F.expr(f"filter(headers, h -> h.key = '{PROTO_VERSION_HEADER_KEY}')[0].value")

    # variant 1 つにつき from_protobuf を 1 つ出す。header と一致しない variant の payload は
    # 誤った型でのデコード結果 (多くは null) になるが、後段で proto_schema により採用しない。
    payload_cols = [
        from_protobuf(
            F.col("value"),
            variant.protobuf_full_name,
            DESCRIPTOR_FILE,
            {"mode": "PERMISSIVE"},
        ).alias(_payload_alias(index))
        for index, variant in enumerate(topic_config.variants)
    ]
    parsed = raw.select(
        F.col("value").alias("rawdata"),
        schema_expr.cast("string").alias("proto_schema"),
        version_expr.cast("string").cast("int").alias("proto_version"),
        *payload_cols,
    )

    writer = _build_batch_writer(topic_config.target_table, topic_config.variants, oneof_cases)

    return (
        parsed.writeStream.queryName(f"consumer-{topic_config.topic}")
        .option("checkpointLocation", topic_config.checkpoint_location)
        .outputMode("append")
        .trigger(availableNow=True)
        .foreachBatch(writer)
        .start()
    )


def _build_batch_writer(
    target_table: str,
    variants: list[SchemaVariant],
    oneof_cases: list[list[str]],
):
    """target_table 専用の foreachBatch コールバックを返す。

    DLQ は全 query 共通の 1 テーブル。行の振り分けは次の優先順 (相互排他):
      1. proto_schema IS NULL                       → DLQ (missing_schema)
      2. proto_version IS NULL                      → DLQ (missing_version)
      3. proto_schema がどの variant にも一致しない  → DLQ (schema_mismatch)
      4. proto_version > 一致 variant の max         → DLQ (unsupported_version)
      5. 一致 variant の payload IS NULL            → DLQ (deserialize_error)
      6. extra_type IS NULL (oneof のどれにも入らない) → DLQ (unknown_extra)
      7. 上記以外                                    → target_table へ append
    """
    schema_names = [variant.schema_name for variant in variants]

    def _write(batch_df: DataFrame, batch_id: int) -> None:
        # 一致 variant の値を共通カラムへ coalesce する (どの variant にも一致しなければ NULL)。
        event_id = _coalesce_by_schema(variants, lambda i, v: F.col(f"{_payload_alias(i)}.{v.event_id_field}"), "long")
        event_datetime_str = _coalesce_by_schema(
            variants, lambda i, v: F.col(f"{_payload_alias(i)}.{v.event_datetime_field}"), "string"
        )
        extra_type = _coalesce_by_schema(variants, lambda i, v: _extra_type_value(i, v, oneof_cases), "string")
        matched_payload_null = _coalesce_by_schema(
            variants, lambda i, _v: F.col(_payload_alias(i)).isNull(), "boolean"
        )
        over_version = _coalesce_by_schema(
            variants, lambda _i, v: F.col("proto_version") > F.lit(v.max_supported_version), "boolean"
        )

        enriched = (
            batch_df.withColumn("event_id", event_id)
            .withColumn("event_datetime_str", event_datetime_str)
            .withColumn("extra_type", extra_type)
            .withColumn("matched_payload_null", matched_payload_null)
            .withColumn("over_version", over_version)
        )
        routed = enriched.withColumn(
            "route",
            F.when(F.col("proto_schema").isNull(), F.lit(DLQ_REASON_MISSING_SCHEMA))
            .when(F.col("proto_version").isNull(), F.lit(DLQ_REASON_MISSING_VERSION))
            .when(~F.col("proto_schema").isin(schema_names), F.lit(DLQ_REASON_SCHEMA_MISMATCH))
            .when(F.col("over_version"), F.lit(DLQ_REASON_UNSUPPORTED_VERSION))
            .when(F.col("matched_payload_null"), F.lit(DLQ_REASON_DESERIALIZE_ERROR))
            .when(F.col("extra_type").isNull(), F.lit(DLQ_REASON_UNKNOWN_EXTRA))
            .otherwise(F.lit("ok")),
        )
        routed.cache()
        try:
            # 成功行: target_table のスキーマ (event_id, event_datetime, extra_type, rawdata)
            # に合わせて明示 select する。route で OK 判定済みなので extra_type は非 NULL。
            valid = routed.where(F.col("route") == "ok").select(
                F.col("event_id"),
                F.to_timestamp(F.col("event_datetime_str")).alias("event_datetime"),
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
        description="Kafka を並列 streaming query で取得し、topic 毎の Iceberg テーブルに書き込む",
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

    logger.info("starting consumer with %s topic(s)", len(TOPICS))

    spark = build_spark()

    # TOPICS をループして各 TopicConfig に対し独立した StreamingQuery を起動する。
    # start() は non-blocking なので、ループ終了時には全 query が並列実行中になる。
    queries: list[StreamingQuery] = []
    for topic_config in TOPICS:
        # variant ごとに oneof case を起動時に列挙する (extra_oneof を持たない variant は [])。
        oneof_cases = [
            discover_oneof_cases(DESCRIPTOR_FILE, variant.protobuf_full_name, variant.extra_oneof)
            if variant.extra_oneof is not None
            else []
            for variant in topic_config.variants
        ]
        logger.info(
            "starting query for topic=%s table=%s variants=%s",
            topic_config.topic,
            topic_config.target_table,
            [v.schema_name for v in topic_config.variants],
        )
        q = start_query_for(spark, topic_config, args.starting_offsets, oneof_cases)
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
