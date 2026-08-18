from typing import cast

from aws_cdk import Duration, Stack
from aws_cdk import aws_eks_v2 as eks
from aws_cdk import aws_iam as iam
from aws_cdk import aws_lambda as lambda_
from aws_cdk import aws_logs as logs
from aws_cdk import aws_sns as sns
from aws_cdk import aws_sns_subscriptions as sns_subs
from constructs import Construct

from ekscdk.config import ClusterConfig
from ekscdk.constructs._manifest import load_manifest, load_manifest_with_subs, load_manifests, manifest_dir

_DIR = manifest_dir("monitoring")


class MonitoringConstruct(Construct):
    """監視環境 Construct

    AWS リソース:
      - CloudWatch Log Group（コンテナログ）
      - SNS Topic（Alertmanager 通知配送先。subscriber は手動 / 別 PR で追加）

    Kubernetes リソース（CDK 管理 Helm / manifest）:
      - kube-prometheus-stack: Prometheus（in-cluster、2 replica、retention=15d、
        gp3 PVC 20Gi）/ Prometheus Operator / Grafana / Alertmanager（3 replica HA、
        gp3 PVC 1Gi、SNS receiver）/ kube-state-metrics / node-exporter
      - Strimzi 系 PodMonitor 3 件（kafka-resources / cluster-operator / entity-operator）
      - Grafana Dashboard ConfigMap 5 件（kafka / exporter / operators / cruise-control / kraft）
      - PrometheusRule 5 件: Strimzi 公式 prometheus-rules をそのまま適用（稼働コンポーネント分のみ）
      - Strimzi kube-state-metrics: Kafka/KafkaTopic/KafkaUser 等 CR ステータス監視の専用インスタンス
        + PrometheusRule（examples/metrics/kube-state-metrics/ 起点）
      - Fluent Bit DaemonSet: ログ → CloudWatch Logs

    Grafana は chart デフォルトの in-cluster Prometheus datasource をそのまま使う。
    Alertmanager は Pod Identity 経由で sns:Publish（Topic ARN 限定）を実行する。
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        cluster: eks.ICluster,
        config: ClusterConfig,
        coredns_addon: eks.IAddon,
        gp3_storage_class: eks.KubernetesManifest,
        strimzi_chart: eks.HelmChart,
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
        namespace = cluster.add_manifest("MonitoringNamespace", load_manifest(_DIR, "namespace.yaml"))

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

        # ── Alertmanager 通知配送先 (SNS Topic) ────────────────────────────────
        # webhook URL / Secrets Manager / ESO を介さず、Pod Identity の sns:Publish
        # で SNS に直接 publish する設計。subscriber（Email / AWS Chatbot 等）は通知先
        # 運用が確定してから手動 or 別 PR で追加する（CDK 管理対象外）。
        alertmanager_topic = sns.Topic(
            self,
            "AlertmanagerNotificationTopic",
            topic_name=f"{config.cluster_name}-alertmanager",
        )
        alertmanager_topic.apply_removal_policy(config.log_removal_policy)

        # ── SNS → Lambda → CloudWatch Logs（テスト環境用の通知本文確認経路）─────
        # dev では Email/Teams subscriber を用意せず、publish された Alertmanager 通知の
        # 本文を Lambda が stdout に書き出し、Lambda の Log Group（/aws/lambda/<name>）で
        # 確認する。stg/prd は実通知先に配送するため作らない（config フラグで分岐）。
        if config.enable_alertmanager_sns_log_forwarder:
            forwarder_logs = logs.LogGroup(
                self,
                "AlertmanagerSnsLogForwarderLogs",
                retention=config.log_retention,
                removal_policy=config.log_removal_policy,
            )
            log_forwarder = lambda_.Function(
                self,
                "AlertmanagerSnsLogForwarder",
                runtime=lambda_.Runtime.PYTHON_3_13,
                handler="index.handler",
                code=lambda_.Code.from_inline(
                    "import json\n"
                    "def handler(event, context):\n"
                    "    for record in event['Records']:\n"
                    "        print(json.dumps(record['Sns'], ensure_ascii=False))\n"
                ),
                timeout=Duration.seconds(30),
                log_group=forwarder_logs,
            )
            # jsii バインディングでは具象 Function を IFunction パラメータへ渡すと pyright が
            # 構造的非互換と誤検知するため cast で明示する（fluent_bit_sa.role と同じ流儀）。
            alertmanager_topic.add_subscription(sns_subs.LambdaSubscription(cast(lambda_.IFunction, log_forwarder)))

        # ── Alertmanager Pod Identity ─────────────────────────────────────────
        # sns:Publish を Topic ARN 限定で付与（webhook URL 平文管理を回避する根拠）。
        alertmanager_sa = cluster.add_service_account(
            "AlertmanagerSa",
            name="alertmanager",
            namespace="monitoring",
            identity_type=eks.IdentityType.POD_IDENTITY,
        )
        alertmanager_sa.node.add_dependency(namespace)
        cast(iam.Role, alertmanager_sa.role).add_to_policy(
            iam.PolicyStatement(
                actions=["sns:Publish"],
                resources=[alertmanager_topic.topic_arn],
            )
        )

        # ── kube-prometheus-stack（Helm）──────────────────────────────────────
        # in-cluster Prometheus + Operator + Grafana + Alertmanager を chart 同梱で
        # deploy。Alertmanager は 3 replica HA、receiver は SNS（sigv4）。Grafana
        # datasource は chart デフォルトの in-cluster Prometheus をそのまま使う。
        kps_values = load_manifest_with_subs(
            _DIR,
            "kube-prometheus-stack-values.yaml",
            REGION=region,
            SNS_TOPIC_ARN=alertmanager_topic.topic_arn,
            PROMETHEUS_RETENTION=config.prometheus_resources.retention,
            PROMETHEUS_STORAGE_SIZE=config.prometheus_resources.storage_size,
            PROMETHEUS_SCRAPE_INTERVAL=config.prometheus_resources.scrape_interval,
            PROMETHEUS_MEMORY_REQUEST=config.prometheus_resources.memory_request,
            PROMETHEUS_MEMORY_LIMIT=config.prometheus_resources.memory_limit,
            PROMETHEUS_CPU_REQUEST=config.prometheus_resources.cpu_request,
            PROMETHEUS_CPU_LIMIT=config.prometheus_resources.cpu_limit,
            ALERTMANAGER_MEMORY_REQUEST=config.alertmanager_resources.memory_request,
            ALERTMANAGER_MEMORY_LIMIT=config.alertmanager_resources.memory_limit,
            ALERTMANAGER_CPU_REQUEST=config.alertmanager_resources.cpu_request,
            ALERTMANAGER_CPU_LIMIT=config.alertmanager_resources.cpu_limit,
            PROMETHEUS_REPLICAS=str(config.prometheus_replicas),
            ALERTMANAGER_REPLICAS=str(config.alertmanager_replicas),
            ALERTMANAGER_STORAGE_SIZE=config.alertmanager_resources.storage_size,
        )
        kps = cluster.add_helm_chart(
            "KubePrometheusStack",
            chart="kube-prometheus-stack",
            repository=config.kube_prometheus_stack_chart_repo,
            namespace="monitoring",
            version=config.kube_prometheus_stack_chart_version,
            values=kps_values,
            # wait=True で Pod が Ready になるまで待つ。chart は Prometheus Operator の
            # admission webhook（PrometheusRule を検証、patch.enabled=true のため
            # failurePolicy=Fail）を導入するため、待たないと直後に apply する PrometheusRule が
            # webhook 呼び出しに失敗する（AWS LBC の TargetGroupBinding と同じ失敗パターン）。
            wait=True,
            timeout=Duration.minutes(15),
        )
        # add_service_account(POD_IDENTITY) が自動生成する eks-pod-identity-agent Addon
        # （各ノードで動く DaemonSet）への DependsOn は付与されないため明示する
        # （addons.py の _add_aws_lbc と同じ理由）。coredns も Managed Addon 化により
        # cluster CREATE 時点では存在しないため、名前解決を要する Pod 側で待つ。
        # gp3 StorageClass は Prometheus / Alertmanager の PVC が要求するため、wait=True と
        # 併せると未作成のままでは PVC が Pending で install がタイムアウトする。
        kps.node.add_dependency(
            namespace,
            grafana_sa,
            alertmanager_sa,
            cast(eks.IAddon, cluster.eks_pod_identity_agent),
            coredns_addon,
            gp3_storage_class,
        )

        # ── Kafka / Strimzi PodMonitor ─────────────────────────────────────────
        # broker / controller / cruise-control / kafka-exporter を 1 つの PodMonitor で
        # 収集する。Prometheus Operator (kps) が PodMonitor CRD を提供するため kps Ready
        # 後に apply、対象 NS の存在も依存に張る。
        kafka_pm = cluster.add_manifest(
            "KafkaResourcesPodMonitor",
            load_manifest(_DIR, "prometheus-install/pod-monitors/kafka-pod-monitor.yaml"),
        )
        kafka_pm.node.add_dependency(kps)
        kafka_pm.node.add_dependency(kafka_namespace)

        cluster_op_pm = cluster.add_manifest(
            "StrimziClusterOperatorPodMonitor",
            load_manifest(_DIR, "prometheus-install/pod-monitors/cluster-operator-pod-monitor.yaml"),
        )
        cluster_op_pm.node.add_dependency(kps)
        # strimzi-system NS は Strimzi chart が create_namespace=True で作るため依存する。
        cluster_op_pm.node.add_dependency(strimzi_chart)

        entity_op_pm = cluster.add_manifest(
            "StrimziEntityOperatorPodMonitor",
            load_manifest(_DIR, "prometheus-install/pod-monitors/entity-operator-pod-monitor.yaml"),
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
            "grafana-strimzi-cruise-control-dashboard.yaml",
            "grafana-strimzi-kraft-dashboard.yaml",
        ):
            cm_id = "Dash" + fname.removeprefix("grafana-strimzi-").removesuffix("-dashboard.yaml").title().replace(
                "-", ""
            )
            cm = cluster.add_manifest(cm_id, load_manifest(_DIR, f"grafana-dashboards/{fname}"))
            cm.node.add_dependency(kps)

        # ── PrometheusRule（Strimzi 公式 prometheus-rules 起点、稼働コンポーネント分のみ）──
        # Prometheus Operator が CRD（PrometheusRule）を提供するため kps 依存。
        for fname in (
            "prometheus-rules-kafka.yaml",
            "prometheus-rules-kafka-exporter-topic.yaml",
            "prometheus-rules-cluster-operator.yaml",
            "prometheus-rules-entity-operator.yaml",
            "prometheus-rules-certificate.yaml",
        ):
            rule_id = "Rule" + fname.removeprefix("prometheus-rules-").removesuffix(".yaml").title().replace("-", "")
            rule = cluster.add_manifest(rule_id, load_manifest(_DIR, f"prometheus-install/prometheus-rules/{fname}"))
            rule.node.add_dependency(kps)

        # ── Strimzi kube-state-metrics（CR ステータス監視）──────────────────────
        # kube-prometheus-stack 同梱の kube-state-metrics は Strimzi CRD を知らないため、
        # Strimzi 公式の専用インスタンス（examples/metrics/kube-state-metrics/）を別途 apply する。
        # Kafka / KafkaTopic / KafkaUser 等 CRD の .status.conditions を Ready/Warning/
        # Deprecated としてメトリクス化し、対応する PrometheusRule でアラートする。
        ksm_cm = cluster.add_manifest("StrimziKsmConfigMap", load_manifest(_DIR, "kube-state-metrics/configmap.yaml"))
        ksm_cm.node.add_dependency(namespace)

        ksm_docs = load_manifests(_DIR, "kube-state-metrics/ksm.yaml")
        ksm = cluster.add_manifest("StrimziKubeStateMetrics", *ksm_docs)
        ksm.node.add_dependency(ksm_cm)
        # ClusterRole が kafka.strimzi.io / core.strimzi.io / access.strimzi.io の CRD を
        # 参照するため、Strimzi chart（CRD 登録元）に依存する。
        ksm.node.add_dependency(strimzi_chart)
        ksm.node.add_dependency(kps)

        ksm_rules = cluster.add_manifest(
            "StrimziKsmPrometheusRule", load_manifest(_DIR, "kube-state-metrics/prometheus-rules.yaml")
        )
        ksm_rules.node.add_dependency(kps)

        # ── Fluent Bit DaemonSet（Helm）───────────────────────────────────────
        fluent_bit = cluster.add_helm_chart(
            "FluentBit",
            chart="fluent-bit",
            repository=config.fluent_bit_chart_repo,
            namespace="monitoring",
            version=config.fluent_bit_chart_version,
            values=load_manifest_with_subs(
                _DIR,
                "fluent-bit-values.yaml",
                REGION=region,
                LOG_GROUP_NAME=log_group.log_group_name,
                FLUENT_BIT_MEMORY_REQUEST=config.fluent_bit_resources.memory_request,
                FLUENT_BIT_MEMORY_LIMIT=config.fluent_bit_resources.memory_limit,
                FLUENT_BIT_CPU_REQUEST=config.fluent_bit_resources.cpu_request,
                FLUENT_BIT_CPU_LIMIT=config.fluent_bit_resources.cpu_limit,
                FLUENT_BIT_MEM_BUF_LIMIT=config.fluent_bit_resources.mem_buf_limit,
            ),
        )
        # Fluent Bit は logs.<region>.amazonaws.com の名前解決に CoreDNS を要する。
        fluent_bit.node.add_dependency(fluent_bit_sa, cast(eks.IAddon, cluster.eks_pod_identity_agent), coredns_addon)
