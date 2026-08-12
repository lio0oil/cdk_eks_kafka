import dataclasses

import aws_cdk as core
import pytest
from aws_cdk import assertions
from aws_cdk import aws_iam as iam

from ekscdk.config import ClusterConfig
from ekscdk.constructs._manifest import manifest_dir, parse_kafka_external_listener
from ekscdk.constructs.network import KAFKA_PRIVATE_DNS_NAME
from ekscdk.ekscdk_stack import EksCdkStack
from ekscdk.iam_stack import IamStack
from ekscdk.kafka_consumer_app_stack import KafkaConsumerAppStack

_CODE_OBJECT_VERSION = "dummy-version"


def _synth(config: ClusterConfig) -> assertions.Template:
    app = core.App()
    env = core.Environment(account="123456789012", region="ap-northeast-1")
    iam_stack = IamStack(
        app,
        "IamStack",
        admin_principal=iam.AccountRootPrincipal(),
        role_name=config.admin_role_name,
        env=env,
    )
    infra_stack = EksCdkStack(
        app,
        "EksCdkStack",
        admin_role=iam_stack.eks_admin_role,
        config=config,
        env=env,
    )
    _, bootstrap_port = parse_kafka_external_listener(manifest_dir("kafka"))
    app_stack = KafkaConsumerAppStack(
        app,
        "KafkaConsumerAppStack",
        vpc=infra_stack.vpc,
        bootstrap_dns_name=KAFKA_PRIVATE_DNS_NAME,
        bootstrap_port=bootstrap_port,
        esm_sg=infra_stack.esm_sg,
        data_bucket=infra_stack.data_bucket,
        artifact_bucket=infra_stack.artifact_bucket,
        code_object_version=config.kafka_consumer_code_object_version,
        config=config,
        env=env,
    )
    return assertions.Template.from_stack(app_stack)


@pytest.fixture(scope="module")
def template():
    config = dataclasses.replace(ClusterConfig.for_prd(), kafka_consumer_code_object_version=_CODE_OBJECT_VERSION)
    return _synth(config)


@pytest.fixture(scope="module")
def dev_template():
    config = dataclasses.replace(ClusterConfig.for_dev(), kafka_consumer_code_object_version=_CODE_OBJECT_VERSION)
    return _synth(config)


def test_kafka_consumer_function_exists(template):
    # Code は artifact_bucket の特定 VersionId を参照する（S3Key だけでは CloudFormation
    # がコード変更を検知できないため、ClusterConfig.kafka_consumer_code_object_version を
    # 明示指定する設計。scripts/upload-kafka-consumer-lambda.sh がアップロード後に
    # 出力する VersionId をここに反映する運用）。
    template.has_resource_properties(
        "AWS::Lambda::Function",
        {
            "Runtime": "python3.13",
            "Handler": "index.handler",
            "Timeout": 30,
            "Code": assertions.Match.object_like(
                {
                    "S3Bucket": assertions.Match.any_value(),
                    "S3Key": "function.zip",
                    "S3ObjectVersion": _CODE_OBJECT_VERSION,
                }
            ),
        },
    )


def test_kafka_consumer_event_source_mapping_targets_bootstrap_and_topic(template):
    # bootstrap servers は NLB の Private Hosted Zone 名 (kafka.local) + external listener
    # port（KafkaConstruct が Kafka CR に注入する advertisedHost と同じ経路）。
    # Topic は topics/test-topic.yaml の metadata.name が単一の真実の源。
    template.has_resource_properties(
        "AWS::Lambda::EventSourceMapping",
        {
            "SelfManagedEventSource": {"Endpoints": {"KafkaBootstrapServers": ["kafka.local:9094"]}},
            "Topics": ["test-topic"],
            "StartingPosition": "LATEST",
        },
    )


def test_kafka_consumer_uses_vpc_source_access_configuration_only(template):
    # external listener は plaintext・無認証（NLB 経由の VPC 内アクセス限定）のため、
    # SourceAccessConfigurations は VPC_SUBNET / VPC_SECURITY_GROUP のみで
    # SASL/TLS 系（Secrets Manager 参照）は含まない。
    mappings = template.find_resources("AWS::Lambda::EventSourceMapping")
    assert len(mappings) == 1
    configs = next(iter(mappings.values()))["Properties"]["SourceAccessConfigurations"]
    types = {c["Type"] for c in configs}
    assert types == {"VPC_SUBNET", "VPC_SECURITY_GROUP"}
    assert sum(1 for c in configs if c["Type"] == "VPC_SECURITY_GROUP") == 1


def test_kafka_consumer_execution_role_has_eni_permissions(template):
    # SelfManagedKafkaEventSource L2 は IAM ポリシーを自動付与しないため、ENI 管理権限
    # （self-managed Kafka VPC アクセスの必須権限、出典: AWS Lambda Developer Guide
    # with-kafka-permissions.html）を実行ロールへ個別に付与している必要がある。
    required_actions = {
        "ec2:CreateNetworkInterface",
        "ec2:DescribeNetworkInterfaces",
        "ec2:DescribeVpcs",
        "ec2:DeleteNetworkInterface",
        "ec2:DescribeSubnets",
        "ec2:DescribeSecurityGroups",
    }
    policies = template.find_resources("AWS::IAM::Policy")
    matching: list[dict] = []
    for p in policies.values():
        for stmt in p["Properties"]["PolicyDocument"]["Statement"]:
            action = stmt.get("Action")
            actions = set(action if isinstance(action, list) else [action])
            if required_actions <= actions:
                matching.append(stmt)
    assert len(matching) == 1, "ENI 管理権限を持つ Statement がちょうど 1 個ではない"


def _arn_resource_contains(resource, needle: str) -> bool:
    # format_arn は partition が Fn::Join のトークンになるため、account/region が既知でも
    # 合成後は文字列ではなく {"Fn::Join": ["", [...]]} になる。中身を平坦化して判定する。
    if isinstance(resource, str):
        return needle in resource
    if isinstance(resource, dict) and "Fn::Join" in resource:
        _, parts = resource["Fn::Join"]
        return any(isinstance(p, str) and needle in p for p in parts)
    return False


def test_kafka_consumer_execution_role_has_s3tables_write_permissions(template):
    # S3 Tables は CDK 管理外（S3TablesStack は EMR consumer 用の別ライフサイクル）のため、
    # config.s3_table_bucket_name から ARN を組み立てて付与する（S3TablesStack への
    # cross-stack 参照は張らない）。PutTableData だけでは Iceberg クライアントの commit
    # （メタデータポインタ更新）が完了できないため、GetTable / GetTableMetadataLocation /
    # UpdateTableMetadataLocation も含める。Resource は table bucket 全体
    # （bucket/<name> と bucket/<name>/table/*）に絞り、"*" にはしない。
    required_actions = {
        "s3tables:PutTableData",
        "s3tables:GetTable",
        "s3tables:GetTableMetadataLocation",
        "s3tables:UpdateTableMetadataLocation",
    }
    policies = template.find_resources("AWS::IAM::Policy")
    matching: list[dict] = []
    for p in policies.values():
        for stmt in p["Properties"]["PolicyDocument"]["Statement"]:
            action = stmt.get("Action")
            actions = set(action if isinstance(action, list) else [action])
            if required_actions <= actions:
                matching.append(stmt)
    assert len(matching) == 1, "S3 Tables 書き込み権限を持つ Statement がちょうど 1 個ではない"

    resource = matching[0]["Resource"]
    resources = resource if isinstance(resource, list) else [resource]
    assert len(resources) == 2
    assert all(r != "*" for r in resources)
    assert any(_arn_resource_contains(r, "bucket/kafka-events") for r in resources)
    assert any(_arn_resource_contains(r, "bucket/kafka-events/table/*") for r in resources)


def test_kafka_consumer_execution_role_has_fixed_name(template):
    # role_name を明示しないと CloudFormation の物理 ID からロール名が自動生成され、
    # コンソール上で他の実行ロールと見分けづらい。config.cluster_name を含めることで
    # 環境（dev/stg/prd）ごとに一意になる（eks-cluster-admin-{cluster_name} 等と同じ規約）。
    template.has_resource_properties(
        "AWS::IAM::Role",
        {"RoleName": "kafka-lambda-consumer-eks-cluster"},
    )


def test_kafka_consumer_execution_role_can_read_data_bucket(template):
    # 実行ロールに付与する S3 権限は GetObject + ListBucket（読み取り + 一覧）のみで、
    # Resource はバケット ARN 限定（"*" ではない）であること。data_bucket は
    # EksCdkStack（インフラ側）が管理するため、これはクロススタック参照の権限付与。
    policies = template.find_resources("AWS::IAM::Policy")
    matching: list[dict] = []
    for p in policies.values():
        for stmt in p["Properties"]["PolicyDocument"]["Statement"]:
            action = stmt.get("Action")
            actions = set(action if isinstance(action, list) else [action])
            if any(a.startswith("s3:GetObject") for a in actions):
                matching.append(stmt)
    assert len(matching) == 1, "S3 読み取り権限を持つ Statement がちょうど 1 個ではない"
    stmt = matching[0]
    resource = stmt["Resource"]
    resources = resource if isinstance(resource, list) else [resource]
    assert resources != ["*"]
    assert all(r != "*" for r in resources)


def test_dev_kafka_consumer_subnets_pinned_to_single_az(dev_template):
    # dev は kafka_single_az=True のため、NLB / broker nodegroup と同じ 1AZ に
    # ESM の ENI も固定する（cross-AZ データ転送料を避ける）。
    mappings = dev_template.find_resources("AWS::Lambda::EventSourceMapping")
    assert len(mappings) == 1
    configs = next(iter(mappings.values()))["Properties"]["SourceAccessConfigurations"]
    subnet_configs = [c for c in configs if c["Type"] == "VPC_SUBNET"]
    assert len(subnet_configs) == 1


def test_prd_kafka_consumer_subnets_span_multiple_az(template):
    # stg/prd は kafka_single_az=False のため multi-AZ を維持する。
    mappings = template.find_resources("AWS::Lambda::EventSourceMapping")
    assert len(mappings) == 1
    configs = next(iter(mappings.values()))["Properties"]["SourceAccessConfigurations"]
    subnet_configs = [c for c in configs if c["Type"] == "VPC_SUBNET"]
    assert len(subnet_configs) == 3
