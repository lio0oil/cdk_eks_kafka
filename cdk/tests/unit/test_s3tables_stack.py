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


def test_event_table_has_schema_name_column(template):
    """sample_events_event に schema_name 列があり、どの ProtoBuf 型由来かを保持する。

    分析時に「どの consumer query から書かれた行か」を 1 テーブル内で識別できるようにする。
    consumer は Kafka header の proto-schema 列をそのまま流し込む。
    """
    template.has_resource_properties(
        "AWS::S3Tables::Table",
        {
            "TableName": "sample_events_event",
            "IcebergMetadata": {
                "IcebergSchema": {
                    "SchemaFieldList": assertions.Match.array_with(
                        [assertions.Match.object_like({"Name": "schema_name", "Type": "string", "Required": True})]
                    )
                }
            },
        },
    )


def test_event_table_does_not_have_name_column(template):
    """sample_events_event から name 列を削除したことを invariant 化する。

    proto 側 Event.name は残るが、Iceberg テーブルでは持たない。consumer の select で
    drop しているかは別途 ロジックテスト範囲。
    """
    event_tables = template.find_resources(
        "AWS::S3Tables::Table",
        {"Properties": {"TableName": "sample_events_event"}},
    )
    assert len(event_tables) == 1
    [resource] = event_tables.values()
    fields = resource["Properties"]["IcebergMetadata"]["IcebergSchema"]["SchemaFieldList"]
    names = [f["Name"] for f in fields]
    assert "name" not in names, f"name column must be removed, got: {names}"


def test_event_table_partition_spec_is_schema_name_then_day(template):
    """event テーブルの partition は identity(schema_name) → day(datetime) の順。

    順序の意図: Iceberg では先頭 partition のカーディナリティが低いほどファイル爆発を抑え、
    等値フィルタ (schema_name = 'X') を完全 prune できる。範囲フィルタ (datetime BETWEEN ...)
    は後ろでも prune される。
    """
    event_tables = template.find_resources(
        "AWS::S3Tables::Table",
        {"Properties": {"TableName": "sample_events_event"}},
    )
    [resource] = event_tables.values()
    fields = resource["Properties"]["IcebergMetadata"]["IcebergPartitionSpec"]["Fields"]
    assert [(f["Name"], f["Transform"]) for f in fields] == [
        ("schema_name", "identity"),
        ("datetime_day", "day"),
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
