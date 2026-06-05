"""consumer の動作設定値。

1 つの topic "sample-events-event" に構造の異なる複数の ProtoBuf 定義が相乗りで流れる
構成をサポートする。各 topic は TopicConfig で表し、その topic に流れる proto 定義を
SchemaVariant のリストで列挙する。consumer は 1 topic = 1 StreamingQuery で読み、query 内で
Kafka header の proto-schema を見て variant ごとに from_protobuf を出し分け、各 variant の
フィールドを共通カラム (event_id / event_datetime / extra_type) へ写像して同一テーブルに書く。

新しい proto 定義を同一 topic に追加する場合:
  1. kafka/proto/ に .proto を追加し、events.desc / *_pb2.py を再生成
  2. producer に ProducerConfig を 1 件追加 (同じ topic / 新しい schema_name)
  3. 本ファイルの該当 TopicConfig.variants に SchemaVariant を 1 件追加
     (event_id / event_datetime / extra_type の写像元フィールドを指定)
  4. event_id / event_datetime / extra_type が同じカラムに収まる限り CDK 変更は不要

Envelope のように extra を oneof で持つ型は extra_oneof に oneof 名を指定すると、
起動時に events.desc から case 名を動的列挙して extra_type に case 名を流す。
Metric のように種別を 1 フィールドで持つ型は extra_type_field にフィールド名を指定する。
"""

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SchemaVariant:
    """1 つの topic に相乗りする 1 つの ProtoBuf 定義と、共通カラムへの写像。

    schema_name: Kafka header の proto-schema 値。これが SchemaVariant の識別キーになる
        (どの from_protobuf を使うか / どの写像を使うかを決める)。header がどの variant の
        schema_name にも一致しない行は DLQ reason=schema_mismatch 行き
    protobuf_full_name: Spark の from_protobuf 第 2 引数に渡す package.message
    max_supported_version: 受信処理する ProtoBuf schema の最大 major version。
        header の proto-version がこれを超える行は DLQ (reason=unsupported_version) 行き
    event_id_field: payload 内で event_id の値を持つフィールドパス (例 "event.id" / "metric_id")
    event_datetime_field: payload 内で event_datetime (ISO8601 文字列) を持つフィールドパス
        (例 "event.datetime" / "observed_at")。to_timestamp でキャストして書き込む
    extra_oneof: extra_type を oneof case 名から導出する場合の oneof 名 (例 "extra")。
        指定時は起動時に case 名を events.desc から動的列挙する。どの case にも入らない行は
        DLQ reason=unknown_extra 行き
    extra_type_field: extra_type を単一フィールドの値から取る場合のフィールドパス (例 "kind")。
        extra_oneof と排他。どちらか一方だけを指定する
    """

    schema_name: str
    protobuf_full_name: str
    max_supported_version: int
    event_id_field: str
    event_datetime_field: str
    extra_oneof: str | None = None
    extra_type_field: str | None = None


@dataclass(frozen=True)
class TopicConfig:
    """1 つの Kafka topic と、その topic に流れる全 ProtoBuf 定義・書き込み先をまとめる。

    topic: Kafka topic 名 (producer 側と一致)
    target_table: Iceberg テーブル名 (CDK の S3TablesStack で作成したもの)。
        topic 内の全 variant が同じカラム (event_id / event_datetime / extra_type / rawdata)
        に収まる前提で 1 テーブルに集約する
    checkpoint_location: Structured Streaming の checkpointLocation (実環境では s3:// に置換)
    variants: この topic に相乗りで流れる ProtoBuf 定義のリスト
    """

    topic: str
    target_table: str
    checkpoint_location: str
    variants: list[SchemaVariant]


# Kafka bootstrap 接続先。実環境では NLB / VPC Endpoint Service の DNS に置き換える。
BOOTSTRAP_SERVERS = "localhost:9094"

# 初回起動時のみ参照される startingOffsets。2 回目以降は checkpointLocation の値が優先される。
DEFAULT_STARTING_OFFSETS = "earliest"

# Spark の from_protobuf 第 3 引数に渡す統合 FileDescriptorSet (kafka/proto/events.desc)。
# 内部に全 message を含み、各 message を messageName 引数で選び分ける。
# 実環境では S3 URI に置き換える。例: "s3://<アーティファクトバケット>/proto/events.desc"
DESCRIPTOR_FILE = str(Path(__file__).resolve().parent / "events.desc")

# DLQ 行きデータの退避先。全 query 共通の 1 テーブル。reason 列に下記の DLQ_REASON_* を入れる。
DLQ_TARGET_TABLE = "s3tablesbucket.events.sample_events_dlq"

# ProtoBuf schema を伝達する Kafka header key。producer 側 (constants.py) と一致させる。
PROTO_VERSION_HEADER_KEY = "proto-version"
PROTO_SCHEMA_HEADER_KEY = "proto-schema"

# DLQ の reason 列に入れる分類値。集計・アラート設定で参照する。
DLQ_REASON_MISSING_SCHEMA = "missing_schema"  # header に proto-schema が無い
DLQ_REASON_MISSING_VERSION = "missing_version"  # header に proto-version が無い
DLQ_REASON_SCHEMA_MISMATCH = "schema_mismatch"  # proto-schema がどの variant にも一致しない
DLQ_REASON_UNSUPPORTED_VERSION = "unsupported_version"  # version が max_supported_version を超える
DLQ_REASON_DESERIALIZE_ERROR = "deserialize_error"  # from_protobuf がデコードできなかった
DLQ_REASON_UNKNOWN_EXTRA = "unknown_extra"  # oneof extra 型で case がどれにも該当しない

# checkpointLocation 用 S3 バケット名。バケット名にアカウント ID と env 名が含まれるため
# リポジトリには持たず .env 経由で受ける (launch.json の envFile で読み込む)。
CHECKPOINT_BUCKET = os.environ["CHECKPOINT_BUCKET"]


# 処理対象 topic のリスト。1 topic に構造の異なる 2 つの ProtoBuf 定義を相乗りさせる例:
#   - Envelope (event.proto):  event(id/datetime) + oneof extra{OrderEvent,UserEvent}
#       -> extra_type は oneof case 名 (order_event / user_event)
#   - Metric   (metric.proto): フラットな metric_id / observed_at / kind / ...
#       -> extra_type は kind フィールドの値 (cpu / memory)
# 両者とも event_id / event_datetime / extra_type / rawdata の同一カラムに収まるため、
# テーブルは sample_events_event 1 つで足り、CDK 変更は不要。
TOPICS: list[TopicConfig] = [
    TopicConfig(
        topic="sample-events-event",
        target_table="s3tablesbucket.events.sample_events_event",
        checkpoint_location=f"s3a://{CHECKPOINT_BUCKET}/sample-events-event/",
        variants=[
            SchemaVariant(
                schema_name="Envelope",
                protobuf_full_name="ekscdk.kafka.Envelope",
                max_supported_version=1,
                event_id_field="event.id",
                event_datetime_field="event.datetime",
                extra_oneof="extra",
            ),
            SchemaVariant(
                schema_name="Metric",
                protobuf_full_name="ekscdk.kafka.Metric",
                max_supported_version=1,
                event_id_field="metric_id",
                event_datetime_field="observed_at",
                extra_type_field="kind",
            ),
        ],
    ),
]
