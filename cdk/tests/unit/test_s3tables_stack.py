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
