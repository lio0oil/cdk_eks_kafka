from typing import cast

from aws_cdk import CfnOutput, Duration, Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_lambda_event_sources as lambda_event_sources
from aws_cdk import aws_logs as logs
from aws_cdk import aws_s3 as s3
from constructs import Construct

from ekscdk.config import ClusterConfig
from ekscdk.constructs._manifest import load_manifest, manifest_dir

_DIR = manifest_dir("kafka")

# artifact_bucket 内での Lambda デプロイパッケージの固定キー。バージョンは
# ClusterConfig.kafka_consumer_code_object_version（S3 VersionId）で識別する。
ARTIFACT_CODE_KEY = "function.zip"


class KafkaConsumerAppStack(Stack):
    """test-topic を Self-managed Kafka の Event Source Mapping (ESM) で購読する Lambda（アプリ層）。

    KafkaConsumerInfraConstruct（EksCdkStack）が用意した VPC / SG / S3 バケットを参照する
    だけで、これ自身は新規の AWS ネットワークリソースを作らない。Lambda コードは
    cdk deploy の外（scripts/ のアップロードスクリプト）で artifact_bucket にアップロードし、
    その S3 VersionId を ClusterConfig.kafka_consumer_code_object_version に反映してから
    このスタックをデプロイする運用（README 参照）。

    external listener（NLB 経由、plaintext・無認証）を bootstrap servers として使うため、
    SourceAccessConfigurations は VPC_SUBNET / VPC_SECURITY_GROUP のみで SASL/TLS 認証は不要。

    Lambda 関数本体は VPC にアタッチしない。self-managed Kafka の ESM では VPC 接続
    （ENI 作成）は SourceAccessConfigurations 経由で AWS 側のポーラーが行うため、
    関数コード自体が VPC 内リソースにアクセスしない限り VPC 常駐は不要
    （ハンドラは cdk/lambda/kafka_consumer/、現状は受信レコードをログ出力するだけのスタブ）。
    ただし ENI 管理権限は実行ロールへの自動付与対象外（SelfManagedKafkaEventSource L2 は
    IAM ポリシーを追加しない）のため、AWS Lambda Developer Guide の self-managed Kafka VPC
    アクセス必須権限（with-kafka-permissions.html）を明示的に付与する。
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: ec2.IVpc,
        bootstrap_dns_name: str,
        bootstrap_port: int,
        esm_sg: ec2.ISecurityGroup,
        data_bucket: s3.IBucket,
        artifact_bucket: s3.IBucket,
        code_object_version: str,
        config: ClusterConfig,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        topic_name = load_manifest(_DIR, "topics/test-topic.yaml")["metadata"]["name"]

        # kafka_single_az=True の場合、NLB / broker nodegroup と同じ 1AZ に ESM の ENI も
        # 固定する（network.py の kafka_nlb_subnets と同じ理由: AZ 跨ぎのデータ転送料を避ける）。
        vpc_subnets = ec2.SubnetSelection(
            subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
            availability_zones=[vpc.availability_zones[0]] if config.kafka_single_az else None,
        )

        log_group = logs.LogGroup(
            self,
            "KafkaLambdaConsumerLogs",
            retention=config.log_retention,
            removal_policy=config.log_removal_policy,
        )

        consumer = lambda_.Function(
            self,
            "KafkaLambdaConsumer",
            runtime=lambda_.Runtime.PYTHON_3_13,
            handler="index.handler",
            code=lambda_.Code.from_bucket(artifact_bucket, ARTIFACT_CODE_KEY, object_version=code_object_version),
            timeout=Duration.seconds(30),
            log_group=log_group,
        )
        # Function（L2）は内部生成する実行ロールの role_name を公開していないため、
        # aws-lbc-pod-identity と同じ流儀（addons.py）で CfnRole 経由で上書きする。
        execution_role = cast(iam.IRole, consumer.role)
        cast(iam.CfnRole, execution_role.node.default_child).role_name = f"kafka-lambda-consumer-{config.cluster_name}"

        data_bucket.grant_read(consumer)

        consumer.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "ec2:CreateNetworkInterface",
                    "ec2:DescribeNetworkInterfaces",
                    "ec2:DescribeVpcs",
                    "ec2:DeleteNetworkInterface",
                    "ec2:DescribeSubnets",
                    "ec2:DescribeSecurityGroups",
                ],
                resources=["*"],
            )
        )

        # S3 Tables は CDK 管理外（S3TablesStack は EMR consumer 用の別ライフサイクル）のため、
        # config.s3_table_bucket_name から ARN を組み立てる（S3TablesStack への cross-stack
        # 参照は張らない）。PutTableData だけでは Iceberg クライアントの commit（メタデータ
        # ポインタ更新）が完了できないため、GetTable / GetTableMetadataLocation /
        # UpdateTableMetadataLocation も付与する。
        table_bucket_arn = self.format_arn(
            service="s3tables", resource="bucket", resource_name=config.s3_table_bucket_name
        )
        consumer.add_to_role_policy(
            iam.PolicyStatement(
                actions=[
                    "s3tables:PutTableData",
                    "s3tables:GetTable",
                    "s3tables:GetTableMetadataLocation",
                    "s3tables:UpdateTableMetadataLocation",
                ],
                resources=[
                    table_bucket_arn,
                    f"{table_bucket_arn}/table/*",
                ],
            )
        )

        consumer.add_event_source(
            lambda_event_sources.SelfManagedKafkaEventSource(
                bootstrap_servers=[f"{bootstrap_dns_name}:{bootstrap_port}"],
                topic=topic_name,
                consumer_group_id=f"{config.cluster_name}-lambda-esm",
                starting_position=lambda_.StartingPosition.LATEST,
                vpc=vpc,
                vpc_subnets=vpc_subnets,
                security_group=esm_sg,
            )
        )

        CfnOutput(self, "KafkaLambdaConsumerFunctionName", value=consumer.function_name)
