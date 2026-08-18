import json
import os
from typing import cast

from aws_cdk import Duration, Stack
from aws_cdk import aws_eks_v2 as eks
from aws_cdk import aws_iam as iam
from constructs import Construct

from ekscdk.config import ClusterConfig
from ekscdk.constructs._manifest import load_manifest, manifest_dir

_DIR = manifest_dir("addons")
_DIR_KAFKA = manifest_dir("kafka")


class AddonsConstruct(Construct):
    """クラスター共通の Kubernetes リソース（StorageClass / Namespace / Helm chart）を導入する。

    EKS Managed Addon は NodeGroup との作成順序に制約があり、順序をリソース単位で
    明示する必要があるため EksClusterConstruct 側に集約している（本 Construct は扱わない）。
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        cluster: eks.ICluster,
        coredns_addon: eks.IAddon,
        config: ClusterConfig,
    ) -> None:
        super().__init__(scope, construct_id)

        self._cluster: eks.ICluster = cluster
        self._coredns_addon = coredns_addon
        self._config = config

        self._gp3_storage_class = self._add_gp3_storage_class()
        self._kafka_namespace = self._add_kafka_namespace()
        self._strimzi_chart = self._add_strimzi(self._kafka_namespace)
        self._aws_lbc_chart = self._add_aws_lbc()

    def _add_kafka_namespace(self) -> eks.KubernetesManifest:
        """kafka Namespace。

        Strimzi chart（`_add_strimzi`）に `watchNamespaces: ["kafka"]` を渡しており、
        対象 NS が事前に存在しないと RoleBinding 作成時に Helm install が失敗する
        （`namespaces "kafka" not found`）。KafkaConstruct より前に Addons 側で
        用意することで、KafkaConstruct を導入する前の段階的デプロイでも Addons 単体で
        synth / deploy できるようにする。KafkaConstruct はこの Namespace を受け取って
        NodePool 等の実体を apply する。
        """
        return self._cluster.add_manifest("KafkaNamespace", load_manifest(_DIR_KAFKA, "namespace.yaml"))

    def _add_gp3_storage_class(self) -> eks.KubernetesManifest:
        # Kafka broker/controller と Prometheus/Alertmanager が共有するデフォルト StorageClass。
        # Kafka 専用の StorageClass（gp3-kafka）は単一消費者のため KafkaConstruct 側で管理する。
        return self._cluster.add_manifest("Gp3StorageClass", load_manifest(_DIR, "gp3-storageclass.yaml"))

    def _add_strimzi(self, kafka_namespace: eks.KubernetesManifest) -> eks.HelmChart:
        chart = self._cluster.add_helm_chart(
            "StrimziOperator",
            chart="strimzi-kafka-operator",
            repository=self._config.strimzi_chart_repo,
            namespace="strimzi-system",
            create_namespace=True,
            version=self._config.strimzi_version,
            values={
                "watchNamespaces": ["kafka"],
                "replicas": 2,
                "logLevel": "WARN",
                # toleration 不要：system-nodegroup には taint が無く、
                # kafka nodegroup の DedicatedKafka taint を tolerate しないため
                # 自然に system に schedule される
                # ScheduleAnyway は best-effort（AZ 分散優先だが、満たせなくても schedule）。
                # DoNotSchedule にすると node 障害で Pod が Pending に陥るリスクを取る。
                "topologySpreadConstraints": [
                    {
                        "maxSkew": 1,
                        "topologyKey": "topology.kubernetes.io/zone",
                        "whenUnsatisfiable": "ScheduleAnyway",
                        "labelSelector": {"matchLabels": {"name": "strimzi-cluster-operator"}},
                    }
                ],
                # chart デフォルトは enabled: false。replicas: 2 の leader-election 構成だが
                # PDB 無効のままノードドレインが走ると 2 Pod が同時に evict されうるため明示的に有効化する。
                # 値は chart デフォルトの minAvailable: 1 にそのまま乗る（templates は
                # minAvailable / maxUnavailable それぞれ truthy なら spec に書く実装、chart デフォルトの
                # maxUnavailable は空値で falsy 評価される）。1 Pod 残れば leader election で
                # そちらが leader を引き継ぐため reconcile 速度は落ちるが機能停止には至らない。
                "podDisruptionBudget": {"enabled": True},
            },
        )
        # watchNamespaces=["kafka"] が RoleBinding を作る対象 NS の存在を前提にするため、
        # kafka Namespace の作成完了を待ってから導入する。
        # Operator Pod は Kafka broker の DNS 名解決に CoreDNS を要するため Addon を待つ。
        chart.node.add_dependency(kafka_namespace, self._coredns_addon)
        return chart

    @property
    def kafka_namespace(self) -> eks.KubernetesManifest:
        """kafka Namespace リソース。

        KafkaConstruct が NodePool / Kafka CR 等の実体を apply する際の依存先、
        および MonitoringConstruct が PodMonitor を kafka NS に紐付ける際の依存先として使う。
        """
        return self._kafka_namespace

    @property
    def aws_lbc_chart(self) -> eks.HelmChart:
        """AWS Load Balancer Controller の Helm chart リソース。

        TargetGroupBinding 等、AWS LBC が提供する CRD を使う manifest からは
        この chart リソースに add_dependency() して CRD インストール後に
        kubectl apply されるよう順序を担保する。
        """
        return self._aws_lbc_chart

    @property
    def gp3_storage_class(self) -> eks.KubernetesManifest:
        """デフォルト StorageClass（gp3）。

        この StorageClass の PVC を要求するワークロード（Prometheus / Alertmanager）は、
        作成完了を待たないと PVC が Pending のままになるため依存を張る。
        """
        return self._gp3_storage_class

    @property
    def strimzi_chart(self) -> eks.HelmChart:
        """Strimzi Operator Helm chart リソース。

        strimzi-system Namespace を作るのも Strimzi chart のため、その NS に
        置く PodMonitor は chart Ready を待つ必要がある（cluster-operator-pod-monitor.yaml）。
        """
        return self._strimzi_chart

    def _add_aws_lbc(self) -> eks.HelmChart:
        """AWS Load Balancer Controller を導入する。

        Strimzi の per-broker NodePort Service を NLB の TargetGroup に
        TargetGroupBinding 経由で動的バインドするために必要。
        ASG ベースの static attachment と異なり、Pod のローリング更新時にも
        Service Endpoints と TargetGroup の同期が追従する。
        """
        sa = self._cluster.add_service_account(
            "AwsLbcSa",
            name="aws-load-balancer-controller",
            namespace="kube-system",
            identity_type=eks.IdentityType.POD_IDENTITY,
        )
        # add_service_account（L2）は内部生成する IAM Role の role_name を公開していないため、
        # L1 escape hatch で明示的に名前を付ける。
        cast(iam.CfnRole, sa.role.node.default_child).role_name = f"aws-lbc-pod-identity-{self._config.cluster_name}"
        # AWS LBC 公式 IAM ポリシー
        with open(os.path.join(_DIR, "aws-lbc-iam-policy.json")) as f:
            policy_doc = json.load(f)
        for stmt in policy_doc["Statement"]:
            cast(iam.Role, sa.role).add_to_policy(iam.PolicyStatement.from_json(stmt))

        # wait=True で Pod が Ready になるまで待つ。
        # TargetGroupBinding の apply 時に AWS LBC の MutatingWebhook が呼ばれるため、
        # webhook service の endpoint が立ち上がっていないと apply が失敗する。
        chart = self._cluster.add_helm_chart(
            "AwsLbc",
            chart="aws-load-balancer-controller",
            repository=self._config.aws_lbc_chart_repo,
            namespace="kube-system",
            version=self._config.aws_lbc_chart_version,
            values={
                "clusterName": self._config.cluster_name,
                "region": Stack.of(self).region,
                "vpcId": self._cluster.vpc.vpc_id,
                "serviceAccount": {
                    "create": False,
                    "name": "aws-load-balancer-controller",
                },
                # toleration 不要：DedicatedKafka taint で kafka nodegroup から弾かれ、
                # system-nodegroup（taint 無し）に自然に乗る
                "replicaCount": 2,
                # chart デフォルトの configureDefaultAffinity=true で「同 node に co-locate しない」
                # podAntiAffinity が入る。それに加えて AZ 分散を topologySpreadConstraints で上乗せ。
                "topologySpreadConstraints": [
                    {
                        "maxSkew": 1,
                        "topologyKey": "topology.kubernetes.io/zone",
                        "whenUnsatisfiable": "ScheduleAnyway",
                        "labelSelector": {"matchLabels": {"app.kubernetes.io/name": "aws-load-balancer-controller"}},
                    }
                ],
                # chart デフォルトは空 dict {} で、templates/pdb.yaml の `if .Values.podDisruptionBudget`
                # が falsy 評価されて PDB が生成されない。Strimzi / Prometheus chart と違い
                # minAvailable のデフォルト値も持たないため、値を明示しないと spec が空になる。
                # MutatingWebhook が止まると TargetGroupBinding 等の apply が詰まるため、
                # replicaCount を将来増やしても同時喪失を 1 に固定できる maxUnavailable で表現する
                # （minAvailable: 1 だと replicaCount 増えるほど許容喪失が増えてしまう）。
                "podDisruptionBudget": {"maxUnavailable": 1},
            },
            wait=True,
            timeout=Duration.minutes(10),
        )
        chart.node.add_dependency(sa)
        # add_service_account(POD_IDENTITY) は内部で eks-pod-identity-agent Addon を
        # 自動生成するが、生成される CfnPodIdentityAssociation にはこの Addon への
        # DependsOn が付かない。Addon（各ノードで動く DaemonSet）が Ready になる前に
        # AWS LBC Pod がスケジュールされるとクレデンシャル取得に失敗しうるため明示する。
        chart.node.add_dependency(cast(eks.IAddon, self._cluster.eks_pod_identity_agent))
        # AWS LBC は TargetGroupBinding の mutating webhook で DescribeTargetGroups を呼び、
        # その名前解決に CoreDNS を要する。CoreDNS 未 Ready のまま webhook が呼ばれると
        # 解決がハングして webhook の 10s deadline を超過する。
        chart.node.add_dependency(self._coredns_addon)
        return chart
