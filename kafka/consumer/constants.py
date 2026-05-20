"""consumer の動作設定値。

Envelope 採用後は Kafka に流れる proto は常に 1 種類 (Envelope) のため SCHEMAS は
1 件に集約される。Envelope.extra (oneof) の case が増えても本ファイルは変更不要で、
consumer.py 側が extra_type 列に case 名を流すだけ。

新しい extra 型を追加する場合:
  1. kafka/proto/event.proto の Envelope.extra に oneof case を追加し、events.desc /
     event_pb2.py を再生成
  2. producer の _make_envelope_payload の振り分けロジックを拡張
  3. consumer / CDK の定義変更は不要
"""

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SchemaConfig:
    """1 つの ProtoBuf 型に紐付く全リソース名をまとめる。

    schema_name: ProtoBuf message 名。Spark UI / ログ識別用に加え、Kafka header の
        proto-schema との一致確認に使う (不一致行は DLQ reason=schema_mismatch 行き)
    protobuf_full_name: Spark の from_protobuf 第 2 引数に渡す package.message
    topic: Kafka topic 名 (producer 側と一致)
    target_table: Iceberg テーブル名 (CDK の S3TablesStack で作成したもの)
    checkpoint_location: Structured Streaming の checkpointLocation (実環境では s3:// に置換)
    max_supported_version: 受信処理する ProtoBuf schema の最大 major version。
        header の proto-version がこれを超える行は DLQ (reason=unsupported_version) 行き
    """

    schema_name: str
    protobuf_full_name: str
    topic: str
    target_table: str
    checkpoint_location: str
    max_supported_version: int


# Kafka bootstrap 接続先。実環境では NLB / VPC Endpoint Service の DNS に置き換える。
BOOTSTRAP_SERVERS = "localhost:9094"

# 初回起動時のみ参照される startingOffsets。2 回目以降は checkpointLocation の値が優先される。
DEFAULT_STARTING_OFFSETS = "earliest"

# Spark の from_protobuf 第 3 引数に渡す統合 FileDescriptorSet (kafka/proto/events.desc)。
# 内部に全 message を含み、各 message を messageName 引数で選び分ける。
# 実環境では S3 URI に置き換える。例: "s3://<アーティファクトバケット>/proto/events.desc"
DESCRIPTOR_FILE = str(Path(__file__).resolve().parent / "events.desc")

# DLQ 行きデータの退避先。全 schema 共通の 1 テーブル。reason 列に下記の DLQ_REASON_* を入れる。
DLQ_TARGET_TABLE = "s3tablesbucket.events.sample_events_dlq"

# ProtoBuf schema を伝達する Kafka header key。producer 側 (constants.py) と一致させる。
PROTO_VERSION_HEADER_KEY = "proto-version"
PROTO_SCHEMA_HEADER_KEY = "proto-schema"

# DLQ の reason 列に入れる分類値。集計・アラート設定で参照する。
DLQ_REASON_MISSING_SCHEMA = "missing_schema"  # header に proto-schema が無い
DLQ_REASON_MISSING_VERSION = "missing_version"  # header に proto-version が無い
DLQ_REASON_SCHEMA_MISMATCH = "schema_mismatch"  # proto-schema が SchemaConfig.schema_name と一致しない
DLQ_REASON_UNSUPPORTED_VERSION = "unsupported_version"  # version が max_supported_version を超える
DLQ_REASON_DESERIALIZE_ERROR = "deserialize_error"  # from_protobuf がデコードできなかった
DLQ_REASON_UNKNOWN_EXTRA = "unknown_extra"  # Envelope.extra の oneof case がどれにも該当しない

# checkpointLocation 用 S3 バケット名。バケット名にアカウント ID と env 名が含まれるため
# リポジトリには持たず .env 経由で受ける (launch.json の envFile で読み込む)。
CHECKPOINT_BUCKET = os.environ["CHECKPOINT_BUCKET"]


# 処理対象スキーマのリスト。Envelope 採用後は 1 件に集約される。
# Envelope.extra の oneof case を増やしてもこのリストは変更不要 (consumer.py が
# 動的に extra_type 列を埋める)。
SCHEMAS: list[SchemaConfig] = [
    SchemaConfig(
        schema_name="Envelope",
        protobuf_full_name="ekscdk.kafka.Envelope",
        topic="sample-events-event",
        target_table="s3tablesbucket.events.sample_events_event",
        checkpoint_location=f"s3a://{CHECKPOINT_BUCKET}/envelope/",
        max_supported_version=1,
    ),
]
