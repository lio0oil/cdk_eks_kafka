import hashlib
import json
from typing import cast

from aws_cdk import Stack
from aws_cdk import aws_aps as aps
from aws_cdk import aws_eks_v2 as eks
from aws_cdk import aws_grafana as grafana
from aws_cdk import aws_iam as iam
from aws_cdk import aws_logs as logs
from constructs import Construct

from ekscdk.config import ClusterConfig
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

    def __init__(self, scope: Construct, construct_id: str, cluster: eks.ICluster, config: ClusterConfig) -> None:
        super().__init__(scope, construct_id)

        region = Stack.of(self).region

        # ── AMP ──────────────────────────────────────────────────────────────
        amp_workspace = aps.CfnWorkspace(self, "AmpWorkspace", alias=config.cluster_name)
        amp_remote_write_url = f"{amp_workspace.attr_prometheus_endpoint}api/v1/remote_write"

        # ── CloudWatch Log Group ──────────────────────────────────────────────
        log_group = logs.LogGroup(
            self,
            "ApplicationLogGroup",
            log_group_name=f"/aws/eks/{config.cluster_name}/application",
            retention=config.log_retention,
            removal_policy=config.log_removal_policy,
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
            # cloudwatch:PutMetricData はリソース指定不可のため * が必須
            iam.PolicyStatement(
                actions=["cloudwatch:PutMetricData"],
                resources=["*"],
            ),
            # Container Insights が動的に作成するロググループを /aws/containerinsights/* に限定
            iam.PolicyStatement(
                actions=["logs:CreateLogGroup"],
                resources=["arn:*:logs:*:*:log-group:/aws/containerinsights/*"],
            ),
            iam.PolicyStatement(
                actions=["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogGroups", "logs:DescribeLogStreams"],
                resources=[log_group.log_group_arn, log_group.log_group_arn + ":*"],
            ),
            # awsemf exporter が /aws/containerinsights/<cluster>/performance に書き込むための権限
            iam.PolicyStatement(
                actions=["logs:CreateLogStream", "logs:PutLogEvents", "logs:DescribeLogGroups", "logs:DescribeLogStreams"],
                resources=[
                    "arn:*:logs:*:*:log-group:/aws/containerinsights/*",
                    "arn:*:logs:*:*:log-group:/aws/containerinsights/*:*",
                ],
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
            name=f"{config.cluster_name}-grafana",
            account_access_type="CURRENT_ACCOUNT",
            authentication_providers=[amg_auth_provider],
            permission_type="SERVICE_MANAGED",
            role_arn=amg_role.role_arn,
            data_sources=["PROMETHEUS"],
            grafana_version=config.grafana_version,
        )

        # ── ADOT ConfigMap（Helm chart のデフォルト debug エクスポーターを回避するため直接管理）──
        adot_configmap_manifest = load_with_subs(
            _DIR, "adot-configmap.yaml",
            REGION=region,
            AMP_REMOTE_WRITE_URL=amp_remote_write_url,
            CLUSTER_NAME=config.cluster_name,
        )
        adot_configmap = cluster.add_manifest("AdotConfigMap", adot_configmap_manifest)
        adot_configmap.node.add_dependency(namespace)

        # ── ADOT DaemonSet（Helm）────────────────────────────────────────────
        # ConfigMap の内容ハッシュを podAnnotations に仕込み、内容変更時に Pod を自動ロールアウトする
        adot_config_hash = hashlib.md5(
            json.dumps(adot_configmap_manifest["data"], sort_keys=True).encode()
        ).hexdigest()
        adot_values = load(_DIR, "adot-values.yaml")
        adot_values.setdefault("podAnnotations", {})["checksum/config"] = adot_config_hash

        adot = cluster.add_helm_chart(
            "AdotCollector",
            chart="opentelemetry-collector",
            repository=config.adot_chart_repo,
            namespace="monitoring",
            version=config.adot_chart_version,
            values=adot_values,
        )
        adot.node.add_dependency(adot_configmap)
        adot.node.add_dependency(adot_sa)
        adot.node.add_dependency(adot_rbac)

        # ── Fluent Bit DaemonSet（Helm）───────────────────────────────────────
        fluent_bit = cluster.add_helm_chart(
            "FluentBit",
            chart="fluent-bit",
            repository=config.fluent_bit_chart_repo,
            namespace="monitoring",
            version=config.fluent_bit_chart_version,
            values=load_with_subs(
                _DIR, "fluent-bit-values.yaml",
                REGION=region,
                LOG_GROUP_NAME=log_group.log_group_name,
            ),
        )
        fluent_bit.node.add_dependency(fluent_bit_sa)
