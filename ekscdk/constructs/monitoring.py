from typing import cast

from aws_cdk import Stack
from aws_cdk import aws_aps as aps
from aws_cdk import aws_eks_v2 as eks
from aws_cdk import aws_grafana as grafana
from aws_cdk import aws_iam as iam
from aws_cdk import aws_logs as logs
from constructs import Construct

from ekscdk.constructs._manifest import load, load_all, load_with_subs, manifest_dir

_DIR = manifest_dir("monitoring")


class MonitoringConstruct(Construct):
    """監視環境 Construct

    AWS リソース:
      - AMP（Prometheus メトリクス）
      - AMG（Grafana ダッシュボード / AWS SSO 認証）
      - CloudWatch Log Group（コンテナログ）

    Kubernetes リソース（CDK 管理 Helm）:
      - ADOT DaemonSet: メトリクス → AMP + CloudWatch、トレース → X-Ray
      - Fluent Bit DaemonSet: ログ → CloudWatch Logs
      - Kafka Exporter（Strimzi 組み込み）: Consumer Lag メトリクス → ADOT 経由で AMP へ
      - JMX Prometheus Exporter（Strimzi 組み込み）: ブローカー内部メトリクス → ADOT kubernetes_sd 経由で AMP へ
    """

    def __init__(self, scope: Construct, construct_id: str, cluster: eks.ICluster, cluster_name: str = "eks-cluster") -> None:
        super().__init__(scope, construct_id)

        region = Stack.of(self).region

        # ── AMP ──────────────────────────────────────────────────────────────
        amp_workspace = aps.CfnWorkspace(self, "AmpWorkspace", alias=cluster_name)
        amp_remote_write_url = f"{amp_workspace.attr_prometheus_endpoint}api/v1/remote_write"

        # ── CloudWatch Log Group ──────────────────────────────────────────────
        log_group = logs.LogGroup(
            self,
            "ApplicationLogGroup",
            log_group_name=f"/aws/eks/{cluster_name}/application",
            retention=logs.RetentionDays.ONE_MONTH,
        )

        # ── monitoring Namespace ──────────────────────────────────────────────
        namespace = cluster.add_manifest(
            "MonitoringNamespace", load(_DIR, "namespace.yaml")
        )

        # ── ADOT RBAC（Pod ディスカバリ用）────────────────────────────────────
        adot_rbac = cluster.add_manifest(
            "AdotRbac", *load_all(_DIR, "adot-rbac.yaml")
        )

        # ── ADOT Pod Identity ─────────────────────────────────────────────────
        adot_sa = cluster.add_service_account(
            "AdotSa",
            name="adot-collector",
            namespace="monitoring",
            identity_type=eks.IdentityType.POD_IDENTITY,
        )
        adot_sa.node.add_dependency(namespace)
        for stmt in [
            iam.PolicyStatement(
                actions=["aps:RemoteWrite", "aps:GetSeries", "aps:GetLabels", "aps:GetMetricMetadata"],
                resources=[amp_workspace.attr_arn],
            ),
            # X-Ray / EC2 describe はサービス仕様上リソース指定不可のため * が必須
            iam.PolicyStatement(
                actions=[
                    "xray:PutTraceSegments",
                    "xray:PutTelemetryRecords",
                    "xray:GetSamplingRules",
                    "xray:GetSamplingTargets",
                    "ec2:DescribeVolumes",
                    "ec2:DescribeTags",
                ],
                resources=["*"],
            ),
            # CloudWatch: Container Insights が動的にロググループを作成するため CreateLogGroup は *
            iam.PolicyStatement(
                actions=["cloudwatch:PutMetricData", "logs:CreateLogGroup"],
                resources=["*"],
            ),
            iam.PolicyStatement(
                actions=["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogGroups", "logs:DescribeLogStreams"],
                resources=[log_group.log_group_arn, log_group.log_group_arn + ":*"],
            ),
        ]:
            cast(iam.Role, adot_sa.role).add_to_policy(stmt)

        # ── Fluent Bit Pod Identity ───────────────────────────────────────────
        fluent_bit_sa = cluster.add_service_account(
            "FluentBitSa",
            name="fluent-bit",
            namespace="monitoring",
            identity_type=eks.IdentityType.POD_IDENTITY,
        )
        fluent_bit_sa.node.add_dependency(namespace)
        cast(iam.Role, fluent_bit_sa.role).add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "logs:DescribeLogGroups",
                    "logs:DescribeLogStreams",
                ],
                resources=[log_group.log_group_arn, log_group.log_group_arn + ":*"],
            )
        )

        # ── AMG ──────────────────────────────────────────────────────────────
        # デフォルトは AWS_SSO。SSO 未導入の場合は -c amg-auth-provider=SAML を指定する。
        amg_auth_provider: str = self.node.try_get_context("amg-auth-provider") or "AWS_SSO"

        amg_role = iam.Role(
            self,
            "AmgRole",
            assumed_by=iam.ServicePrincipal("grafana.amazonaws.com"),  # type: ignore[arg-type]
        )
        amg_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonPrometheusQueryAccess")
        )
        # ワークスペース名を変更すると CloudFormation がリソースを再作成するため、
        # 手動インポート済みのダッシュボードが失われる点に注意。
        grafana.CfnWorkspace(
            self,
            "AmgWorkspace",
            name=f"{cluster_name}-grafana",
            account_access_type="CURRENT_ACCOUNT",
            authentication_providers=[amg_auth_provider],
            permission_type="SERVICE_MANAGED",
            role_arn=amg_role.role_arn,
            data_sources=["PROMETHEUS"],
            grafana_version="12.0",
        )

        # ── ADOT DaemonSet（Helm）────────────────────────────────────────────
        adot = cluster.add_helm_chart(
            "AdotCollector",
            chart="opentelemetry-collector",
            repository="https://open-telemetry.github.io/opentelemetry-helm-charts",
            namespace="monitoring",
            version="0.153.0",
            values=load_with_subs(
                _DIR, "adot-values.yaml",
                REGION=region,
                AMP_REMOTE_WRITE_URL=amp_remote_write_url,
            ),
        )
        adot.node.add_dependency(adot_sa)
        adot.node.add_dependency(adot_rbac)

        # ── Fluent Bit DaemonSet（Helm）───────────────────────────────────────
        fluent_bit = cluster.add_helm_chart(
            "FluentBit",
            chart="fluent-bit",
            repository="https://fluent.github.io/helm-charts",
            namespace="monitoring",
            version="0.57.3",
            values=load_with_subs(
                _DIR, "fluent-bit-values.yaml",
                REGION=region,
                LOG_GROUP_NAME=log_group.log_group_name,
            ),
        )
        fluent_bit.node.add_dependency(fluent_bit_sa)
