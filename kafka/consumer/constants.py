"""consumer の動作設定値 (案 R-1)。

各 ProtoBuf 型に対し (schema_name / protobuf_full_name / topic / table / checkpoint) を
SchemaConfig としてまとめ、SCHEMAS リストに並べて指定する。値は動的派生せず固定値で書く
ので、CDK 側のテーブル名と直接対応していることが目視で確認できる。

新しい型を追加する場合:
  1. kafka/proto/ に .proto を追加して events.desc を再生成
  2. 本ファイルの SCHEMAS リストに SchemaConfig を 1 件追加
  3. cdk/ekscdk/s3tables_stack.py にも対応する CfnTable 定義を追加
"""

import os
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class SchemaConfig:
    """1 つの ProtoBuf 型に紐付く全リソース名をまとめる。

    schema_name: ProtoBuf message 名 (Spark UI / ログ識別用)
    protobuf_full_name: Spark の from_protobuf 第 2 引数に渡す package.message
    topic: Kafka topic 名 (producer 側と一致)
    target_table: Iceberg テーブル名 (CDK の S3TablesStack で作成したもの)
    checkpoint_location: Structured Streaming の checkpointLocation (実環境では s3:// に置換)
    """

    schema_name: str
    protobuf_full_name: str
    topic: str
    target_table: str
    checkpoint_location: str


# Kafka bootstrap 接続先。実環境では NLB / VPC Endpoint Service の DNS に置き換える。
BOOTSTRAP_SERVERS = "localhost:9094"

# 初回起動時のみ参照される startingOffsets。2 回目以降は checkpointLocation の値が優先される。
DEFAULT_STARTING_OFFSETS = "earliest"

# Spark の from_protobuf 第 3 引数に渡す統合 FileDescriptorSet (kafka/proto/events.desc)。
# 内部に全 message を含み、各 message を messageName 引数で選び分ける。
# 実環境では S3 URI に置き換える。例: "s3://<アーティファクトバケット>/proto/events.desc"
DESCRIPTOR_FILE = str(Path(__file__).resolve().parent / "events.desc")

# デシリアライズ失敗データの退避先 (Dead Letter Queue)。全 schema 共通の 1 テーブル。
DLQ_TARGET_TABLE = "s3tablesbucket.events.sample_events_dlq"

# checkpointLocation 用 S3 バケット名 (CDK の S3TablesStack で作成: kafka-consumer-checkpoint-<account>-<env>)。
# バケット名にアカウント ID と env 名が含まれるため、リポジトリには持たず環境変数で受ける。
CHECKPOINT_BUCKET = os.environ["CHECKPOINT_BUCKET"]


# 処理対象スキーマのリスト。各 SchemaConfig の値は CDK の S3TablesStack 内のテーブル名と一致させる。
# サンプル実装では Event 1 種類のみ。
SCHEMAS: list[SchemaConfig] = [
    SchemaConfig(
        schema_name="Event",
        protobuf_full_name="ekscdk.kafka.Event",
        topic="sample-events-event",
        target_table="s3tablesbucket.events.sample_events_event",
        checkpoint_location=f"s3a://{CHECKPOINT_BUCKET}/event/",
    ),
    # 新型を追加する場合は以下のように 1 エントリ追加 (例: Notification):
    # SchemaConfig(
    #     schema_name="Notification",
    #     protobuf_full_name="ekscdk.kafka.Notification",
    #     topic="sample-events-notification",
    #     target_table="s3tablesbucket.events.sample_events_notification",
    #     checkpoint_location=f"s3a://{CHECKPOINT_BUCKET}/notification/",
    # ),
]
