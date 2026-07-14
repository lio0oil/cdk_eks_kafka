import aws_cdk as core
import pytest
from aws_cdk import assertions

from ekscdk.config import ClusterConfig
from ekscdk.s3tables_stack import S3TablesStack


@pytest.fixture(scope="module")
def template():
    app = core.App()
    env = core.Environment(account="123456789012", region="ap-northeast-1")
    config = ClusterConfig.for_prd()
    stack = S3TablesStack(app, "S3TablesStack", config=config, env=env)
    return assertions.Template.from_stack(stack)


def test_table_bucket_uses_sse_kms_encryption(template):
    # consumer (EMR) が書き込む table_bucket / checkpoint_bucket は同じ書き込み元
    # (EMR 実行ロール) を信頼境界とするため、KMS キーを共有する設計。
    template.has_resource_properties(
        "AWS::S3Tables::TableBucket",
        {
            "EncryptionConfiguration": {
                "SSEAlgorithm": "aws:kms",
                "KMSKeyArn": assertions.Match.any_value(),
            }
        },
    )


def test_checkpoint_bucket_uses_sse_kms_encryption(template):
    template.has_resource_properties(
        "AWS::S3::Bucket",
        {
            "BucketEncryption": {
                "ServerSideEncryptionConfiguration": assertions.Match.array_with(
                    [
                        assertions.Match.object_like(
                            {
                                "ServerSideEncryptionByDefault": {
                                    "SSEAlgorithm": "aws:kms",
                                    "KMSMasterKeyID": assertions.Match.any_value(),
                                }
                            }
                        )
                    ]
                )
            }
        },
    )


def test_table_bucket_and_checkpoint_bucket_share_one_kms_key(template):
    # 両方とも EMR consumer ロールが書き込む同一信頼境界のため、鍵を分けずコストと運用を
    # シンプルにする（VPC Flow Log 等の別信頼境界のバケットとは共有しない）。
    template.resource_count_is("AWS::KMS::Key", 1)


def test_kms_key_has_alias(template):
    # コンソール/CLI での識別用に alias を付ける（KeyId だけでは判別しづらいため）。
    template.has_resource_properties(
        "AWS::KMS::Alias",
        {"AliasName": "alias/kafka-consumer-s3tables"},
    )


def test_dlq_table_has_reason_column(template):
    """DLQ には failed_at / rawdata に加えて reason 列がある。

    consumer が「なぜ DLQ に入れたか」を記録するため。原因 (missing_version /
    unsupported_version / deserialize_error) を後から集計・アラート設定するのに使う。
    """
    template.has_resource_properties(
        "AWS::S3Tables::Table",
        {
            "TableName": "sample_events_dlq",
            "IcebergMetadata": {
                "IcebergSchema": {
                    "SchemaFieldList": assertions.Match.array_with(
                        [assertions.Match.object_like({"Name": "reason", "Type": "string", "Required": True})]
                    )
                }
            },
        },
    )


def test_event_table_has_extra_type_column(template):
    """sample_events_event に extra_type 列があり、Envelope の oneof case 名を保持する。

    Envelope 構造を採用したため schema_name (常に "Envelope") では行を区別できず、
    代わりに oneof case (order_event / user_event / ...) を extra_type に書く。
    """
    template.has_resource_properties(
        "AWS::S3Tables::Table",
        {
            "TableName": "sample_events_event",
            "IcebergMetadata": {
                "IcebergSchema": {
                    "SchemaFieldList": assertions.Match.array_with(
                        [assertions.Match.object_like({"Name": "extra_type", "Type": "string", "Required": True})]
                    )
                }
            },
        },
    )


def test_event_table_columns_are_envelope_aware(template):
    """sample_events_event の列は Envelope 構造に対応する 4 列のみ。

    proto 側 Event.name や schema_name はテーブルには持たない。rawdata に Envelope の
    bytes をそのまま保存するため、後段で再 deserialize して全フィールドを取り出せる。
    """
    event_tables = template.find_resources(
        "AWS::S3Tables::Table",
        {"Properties": {"TableName": "sample_events_event"}},
    )
    assert len(event_tables) == 1
    [resource] = event_tables.values()
    fields = resource["Properties"]["IcebergMetadata"]["IcebergSchema"]["SchemaFieldList"]
    names = [f["Name"] for f in fields]
    assert names == ["event_id", "event_datetime", "extra_type", "rawdata"], names


def test_event_table_partition_spec_is_extra_type_then_day(template):
    """event テーブルの partition は identity(extra_type) → day(event_datetime) の順。

    Envelope 採用後は schema_name が常に "Envelope" で prune の役に立たないため、
    oneof case (extra_type) で等値 prune できるようにする。後段の day(event_datetime) は
    時系列スキャン用。
    """
    event_tables = template.find_resources(
        "AWS::S3Tables::Table",
        {"Properties": {"TableName": "sample_events_event"}},
    )
    [resource] = event_tables.values()
    fields = resource["Properties"]["IcebergMetadata"]["IcebergPartitionSpec"]["Fields"]
    assert [(f["Name"], f["Transform"]) for f in fields] == [
        ("extra_type", "identity"),
        ("event_datetime_day", "day"),
    ]


def test_dlq_table_has_schema_name_column(template):
    """DLQ にも schema_name 列を追加し、Kafka header の proto-schema をそのまま記録する。

    header に proto-schema が無い missing_schema 行は NULL になるため required=False。
    NULL の意味は reason 列の 'missing_schema' で識別できる。
    """
    template.has_resource_properties(
        "AWS::S3Tables::Table",
        {
            "TableName": "sample_events_dlq",
            "IcebergMetadata": {
                "IcebergSchema": {
                    "SchemaFieldList": assertions.Match.array_with(
                        [assertions.Match.object_like({"Name": "schema_name", "Type": "string", "Required": False})]
                    )
                }
            },
        },
    )


def test_dlq_table_partition_spec_is_schema_name_then_day(template):
    """DLQ も identity(schema_name) → day(failed_at) の順。

    schema 別の失敗集計を等値フィルタで完全 prune できるようにする。後段の day(failed_at) は
    時系列スキャン用 (アラート / lifecycle)。
    """
    dlq_tables = template.find_resources(
        "AWS::S3Tables::Table",
        {"Properties": {"TableName": "sample_events_dlq"}},
    )
    [resource] = dlq_tables.values()
    fields = resource["Properties"]["IcebergMetadata"]["IcebergPartitionSpec"]["Fields"]
    assert [(f["Name"], f["Transform"]) for f in fields] == [
        ("schema_name", "identity"),
        ("failed_at_day", "day"),
    ]
