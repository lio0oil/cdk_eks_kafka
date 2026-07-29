import json
import os
from typing import cast

from aws_cdk import Duration, Stack
from aws_cdk import aws_eks as eks_l1
from aws_cdk import aws_eks_v2 as eks
from aws_cdk import aws_iam as iam
from constructs import Construct

from ekscdk.config import ClusterConfig
from ekscdk.constructs._manifest import load, manifest_dir

_DIR = manifest_dir("addons")


class AddonsConstruct(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        cluster: eks.ICluster,
        config: ClusterConfig,
    ) -> None:
        super().__init__(scope, construct_id)

        self._cluster: eks.ICluster = cluster
        self._config = config

        self._add_eks_addons()
        self._add_gp3_storage_class()
        self._strimzi_chart = self._add_strimzi()
        self._aws_lbc_chart = self._add_aws_lbc()

    def _add_eks_addons(self) -> None:
        # aws-ebs-csi-driver addon は ebs-csi-controller-sa という ServiceAccount を
        # 自身で作成するため、CDK 側では add_service_account で SA を作らない
        # （事前に同名 SA を作ると addon 作成時に衝突するため）。
        # ただし PodIdentityAssociations の RoleArn は EKS/addon 側が自動生成できない
        # （どのポリシーを付けるかはワークロード固有の権限設計でユーザー側が決める事項のため）。
        # そのため IAM Role の作成だけは CDK 側に残す必要がある。
        ebs_csi_role = iam.Role(
            self,
            "EbsCsiPodIdentityRole",
            assumed_by=iam.ServicePrincipal("pods.eks.amazonaws.com").with_session_tags(),  # type: ignore[arg-type]
        )
        ebs_csi_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonEBSCSIDriverPolicy")
        )

        # vpc-cni/coredns/kube-proxy は bootstrap_self_managed_addons のデフォルト（True）
        # による self-managed 版をそのまま使うため、ここでは明示管理しない。

        # aws_eks_v2.Addon（L2）は PodIdentityAssociations を公開していないため、
        # aws-ebs-csi-driver だけは L1 の CfnAddon を直接使う。
        # SA を自身で作らない他の addon と違い競合する事前作成 SA も無いため、
        # ResolveConflicts のエスケープハッチは不要。
        eks_l1.CfnAddon(
            self,
            "EbsCsiDriver",
            addon_name="aws-ebs-csi-driver",
            cluster_name=self._cluster.cluster_name,
            addon_version=self._config.addon_versions["aws-ebs-csi-driver"],
            pod_identity_associations=[
                eks_l1.CfnAddon.PodIdentityAssociationProperty(
                    role_arn=ebs_csi_role.role_arn,
                    service_account="ebs-csi-controller-sa",
                )
            ],
        )

        # metrics-server / eks-node-monitoring-agent は SA を事前作成しないため衝突要因が無く、
        # ResolveConflicts のエスケープハッチも不要。L2 の Addon で十分。
        eks.Addon(
            self,
            "MetricsServer",
            cluster=self._cluster,
            addon_name="metrics-server",
            addon_version=self._config.addon_versions["metrics-server"],
        )

        eks.Addon(
            self,
            "NodeMonitoringAgent",
            cluster=self._cluster,
            addon_name="eks-node-monitoring-agent",
            addon_version=self._config.addon_versions["eks-node-monitoring-agent"],
            # NMA は --verbosity フラグを zap level に -1 倍して渡すため、
            # WARN 以上 (zapcore.WarnLevel = 1) にするには --verbosity=-1。
            # additionalArgs は完全置換なので chart デフォルトの --metrics-address も含める。
            configuration_values={
                "nodeAgent": {
                    "additionalArgs": [
                        "--metrics-address=:8003",
                        "--verbosity=-1",
                    ],
                },
            },
        )

        # snapshot-controller は aws-ebs-csi-driver とは別の EKS Managed Addon。
        # VolumeSnapshot 系 CRD + controller 本体をまとめて管理してくれるため、
        # CRD だけを self-managed で apply する運用は行わない。
        # SA を事前作成しないため衝突要因が無く、ResolveConflicts のエスケープハッチも不要。
        eks.Addon(
            self,
            "SnapshotController",
            cluster=self._cluster,
            addon_name="snapshot-controller",
            addon_version=self._config.addon_versions["snapshot-controller"],
        )

    def _add_gp3_storage_class(self) -> None:
        # Kafka broker/controller と Prometheus/Alertmanager が共有するデフォルト StorageClass。
        # Kafka 専用の StorageClass（gp3-kafka）は単一消費者のため KafkaConstruct 側で管理する。
        self._cluster.add_manifest("Gp3StorageClass", load(_DIR, "gp3-storageclass.yaml"))

    def _add_strimzi(self) -> eks.HelmChart:
        return self._cluster.add_helm_chart(
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

    @property
    def aws_lbc_chart(self) -> eks.HelmChart:
        """AWS Load Balancer Controller の Helm chart リソース。

        TargetGroupBinding 等、AWS LBC が提供する CRD を使う manifest からは
        この chart リソースに add_dependency() して CRD インストール後に
        kubectl apply されるよう順序を担保する。
        """
        return self._aws_lbc_chart

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
        return chart
