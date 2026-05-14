from typing import cast

from aws_cdk import Duration, Stack
from aws_cdk import aws_eks_v2 as eks
from aws_cdk import aws_iam as iam
from aws_cdk import aws_logs as logs
from constructs import Construct

from ekscdk.config import ClusterConfig
from ekscdk.constructs._manifest import load, load_with_subs, manifest_dir
from ekscdk.constructs.addons import AddonsConstruct

_DIR = manifest_dir("monitoring")
_DIR_KAFKA = manifest_dir("kafka")


class MonitoringConstruct(Construct):
    """監視環境 Construct

    AWS リソース:
      - CloudWatch Log Group（コンテナログ）

    Kubernetes リソース（CDK 管理 Helm / manifest）:
      - kube-prometheus-stack: Prometheus（in-cluster、1 replica、retention=15d、
        gp3 PVC 20Gi）/ Prometheus Operator / Grafana / kube-state-metrics /
        node-exporter（Alertmanager のみ無効化）
      - Strimzi 系 PodMonitor 3 件（kafka-resources / cluster-operator / entity-operator）
      - Fluent Bit DaemonSet: ログ → CloudWatch Logs

    Grafana は chart デフォルトの in-cluster Prometheus datasource をそのまま使う。
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        cluster: eks.ICluster,
        config: ClusterConfig,
        addons: AddonsConstruct,
        kafka_namespace: eks.KubernetesManifest,
    ) -> None:
        super().__init__(scope, construct_id)

        region = Stack.of(self).region

        # ── CloudWatch Log Group ──────────────────────────────────────────────
        log_group = logs.LogGroup(
            self,
            "ApplicationLogGroup",
            log_group_name=f"/aws/eks/{config.cluster_name}/application",
            retention=config.log_retention,
            removal_policy=config.log_removal_policy,
        )

        # ── monitoring Namespace ──────────────────────────────────────────────
        namespace = cluster.add_manifest("MonitoringNamespace", load(_DIR, "namespace.yaml"))

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

        # ── Grafana Pod Identity ───────────────────────────────────────────────
        # in-cluster Prometheus を datasource として直接 query するため IAM は不要。
        # SA だけ namespace 整列のために CDK で先に作っておく（chart 側は create: false）。
        grafana_sa = cluster.add_service_account(
            "GrafanaSa",
            name="grafana",
            namespace="monitoring",
            identity_type=eks.IdentityType.POD_IDENTITY,
        )
        grafana_sa.node.add_dependency(namespace)

        # ── kube-prometheus-stack（Helm）──────────────────────────────────────
        # in-cluster Prometheus + Operator + Grafana を chart 同梱で deploy。
        # Alertmanager は無効化（values 参照）。Grafana datasource は chart デフォルトの
        # in-cluster Prometheus（name: Prometheus, isDefault: true）をそのまま使う。
        kps_values = load_with_subs(
            _DIR,
            "kube-prometheus-stack-values.yaml",
            REGION=region,
        )
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
        kps.node.add_dependency(grafana_sa)

        # ── Kafka / Strimzi PodMonitor ─────────────────────────────────────────
        # broker / controller / cruise-control / kafka-exporter を 1 つの PodMonitor で
        # 収集する。Prometheus Operator (kps) が PodMonitor CRD を提供するため kps Ready
        # 後に apply、対象 NS の存在も依存に張る。
        kafka_pm = cluster.add_manifest(
            "KafkaResourcesPodMonitor",
            load(_DIR_KAFKA, "kafka-pod-monitor.yaml"),
        )
        kafka_pm.node.add_dependency(kps)
        kafka_pm.node.add_dependency(kafka_namespace)

        cluster_op_pm = cluster.add_manifest(
            "StrimziClusterOperatorPodMonitor",
            load(_DIR_KAFKA, "cluster-operator-pod-monitor.yaml"),
        )
        cluster_op_pm.node.add_dependency(kps)
        # strimzi-system NS は Strimzi chart が create_namespace=True で作るため依存する。
        cluster_op_pm.node.add_dependency(addons.strimzi_chart)

        entity_op_pm = cluster.add_manifest(
            "StrimziEntityOperatorPodMonitor",
            load(_DIR_KAFKA, "entity-operator-pod-monitor.yaml"),
        )
        entity_op_pm.node.add_dependency(kps)
        entity_op_pm.node.add_dependency(kafka_namespace)

        # ── Grafana Dashboard ConfigMap ───────────────────────────────────────
        # kube-prometheus-stack の sidecar が monitoring namespace の
        # ConfigMap でラベル grafana_dashboard=1 を持つものを自動取り込みする。
        for fname in (
            "grafana-strimzi-kafka-dashboard.yaml",
            "grafana-strimzi-exporter-dashboard.yaml",
            "grafana-strimzi-operators-dashboard.yaml",
        ):
            cm_id = "Dash" + fname.removeprefix("grafana-strimzi-").removesuffix("-dashboard.yaml").title().replace(
                "-", ""
            )
            cm = cluster.add_manifest(cm_id, load(_DIR, f"dashboards/{fname}"))
            cm.node.add_dependency(kps)

        # ── Fluent Bit DaemonSet（Helm）───────────────────────────────────────
        fluent_bit = cluster.add_helm_chart(
            "FluentBit",
            chart="fluent-bit",
            repository=config.fluent_bit_chart_repo,
            namespace="monitoring",
            version=config.fluent_bit_chart_version,
            values=load_with_subs(
                _DIR,
                "fluent-bit-values.yaml",
                REGION=region,
                LOG_GROUP_NAME=log_group.log_group_name,
            ),
        )
        fluent_bit.node.add_dependency(fluent_bit_sa)
