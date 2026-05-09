from typing import cast

from aws_cdk import Duration, Stack
from aws_cdk import aws_aps as aps
from aws_cdk import aws_eks_v2 as eks
from aws_cdk import aws_grafana as grafana
from aws_cdk import aws_iam as iam
from aws_cdk import aws_logs as logs
from constructs import Construct

from ekscdk.config import ClusterConfig
from ekscdk.constructs._manifest import load, load_with_subs, manifest_dir

_DIR = manifest_dir("monitoring")
_KAFKA_DIR = manifest_dir("kafka")


class MonitoringConstruct(Construct):
    """監視環境 Construct

    AWS リソース:
      - AMP（Prometheus メトリクス）
      - AMG（Grafana ダッシュボード / AWS SSO 認証）
      - CloudWatch Log Group（コンテナログ）

    Kubernetes リソース（CDK 管理 Helm）:
      - kube-prometheus-stack: メトリクス収集 → AMP remote write
          - Prometheus（SigV4 認証付き remote write）
          - node-exporter（ノードメトリクス）
          - kube-state-metrics（Pod/Node/PV メトリクス）
          - ServiceMonitor / PodMonitor（Kafka metrics）
      - Fluent Bit DaemonSet: ログ → CloudWatch Logs
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

        # ── Prometheus Pod Identity ────────────────────────────────────────────
        # kube-prometheus-stack の serviceAccount.create=false で使用する SA を事前作成
        prometheus_sa = cluster.add_service_account(
            "PrometheusSa",
            name="prometheus",
            namespace="monitoring",
            identity_type=eks.IdentityType.POD_IDENTITY,
        )
        prometheus_sa.node.add_dependency(namespace)
        cast(iam.Role, prometheus_sa.role).add_to_policy(
            iam.PolicyStatement(
                actions=["aps:RemoteWrite", "aps:GetSeries", "aps:GetLabels", "aps:GetMetricMetadata"],
                resources=[amp_workspace.attr_arn],
            )
        )

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

        # ── kube-prometheus-stack（Helm）──────────────────────────────────────
        kps_values = load_with_subs(
            _DIR, "kube-prometheus-stack-values.yaml",
            REGION=region,
            AMP_REMOTE_WRITE_URL=amp_remote_write_url,
        )
        # timeout を延長して admission webhook の cert 生成 Job が完了するまで待機する
        kps = cluster.add_helm_chart(
            "KubePrometheusStack",
            chart="kube-prometheus-stack",
            repository=config.kube_prometheus_stack_chart_repo,
            namespace="monitoring",
            version=config.kube_prometheus_stack_chart_version,
            values=kps_values,
            timeout=Duration.minutes(15),
        )
        kps.node.add_dependency(namespace)
        kps.node.add_dependency(prometheus_sa)

        # ── Kafka ServiceMonitor / PodMonitor ─────────────────────────────────
        # kube-prometheus-stack の CRD がインストールされた後に適用する
        kafka_sm = cluster.add_manifest(
            "KafkaServiceMonitor", load(_KAFKA_DIR, "kafka-service-monitor.yaml")
        )
        kafka_sm.node.add_dependency(kps)

        kafka_pm = cluster.add_manifest(
            "KafkaJmxPodMonitor", load(_KAFKA_DIR, "kafka-pod-monitor.yaml")
        )
        kafka_pm.node.add_dependency(kps)

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
