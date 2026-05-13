import json
import os
from typing import cast

from aws_cdk import Duration, Stack
from aws_cdk import aws_eks as eks_l1
from aws_cdk import aws_eks_v2 as eks
from aws_cdk import aws_iam as iam
from constructs import Construct

from ekscdk.config import ClusterConfig
from ekscdk.constructs._manifest import load, load_all, manifest_dir

_DIR = manifest_dir("addons")
_DIR_SNAPSHOTTER = manifest_dir("snapshotter")


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
        self._add_external_snapshotter()
        self._strimzi_chart = self._add_strimzi()
        self._aws_lbc_chart = self._add_aws_lbc()

    def _add_eks_addons(self) -> None:
        # EBS CSI Driver 用 Pod Identity
        ebs_csi_sa = self._cluster.add_service_account(
            "EbsCsiSa",
            name="ebs-csi-controller-sa",
            namespace="kube-system",
            identity_type=eks.IdentityType.POD_IDENTITY,
        )
        ebs_csi_sa.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonEBSCSIDriverPolicy")
        )

        for addon_name, construct_id in {
            "vpc-cni": "VpcCni",
            "coredns": "CoreDns",
            "kube-proxy": "KubeProxy",
            "aws-ebs-csi-driver": "EbsCsiDriver",
            "metrics-server": "MetricsServer",
            "eks-node-monitoring-agent": "NodeMonitoringAgent",
        }.items():
            addon = eks.Addon(
                self,
                construct_id,
                cluster=self._cluster,
                addon_name=addon_name,
                addon_version=self._config.addon_versions[addon_name],
            )
            # aws_eks_v2.Addon は ResolveConflicts を公開していないためエスケープハッチで設定する。
            # OVERWRITE にしないと既存 SA のラベルと衝突してデプロイが失敗する。
            cfn_addon = cast(eks_l1.CfnAddon, addon.node.default_child)
            cfn_addon.add_override("Properties.ResolveConflicts", "OVERWRITE")
            if addon_name == "eks-node-monitoring-agent":
                # NMA は --verbosity フラグを zap level に -1 倍して渡すため、
                # WARN 以上 (zapcore.WarnLevel = 1) にするには --verbosity=-1。
                # additionalArgs は完全置換なので chart デフォルトの --metrics-address も含める。
                cfn_addon.add_property_override(
                    "ConfigurationValues",
                    json.dumps(
                        {
                            "nodeAgent": {
                                "additionalArgs": [
                                    "--metrics-address=:8003",
                                    "--verbosity=-1",
                                ],
                            },
                        }
                    ),
                )

        # aws_eks_v2.Cluster が自動追加する eks-pod-identity-agent Addon は
        # CDK API から AddonVersion を渡せないため CFN プロパティ override で pin する。
        pod_identity_addon = self._cluster.node.try_find_child("EksPodIdentityAgentAddon")
        if pod_identity_addon is not None:
            cfn_pod_identity = cast(eks_l1.CfnAddon, pod_identity_addon.node.default_child)
            cfn_pod_identity.add_property_override(
                "AddonVersion", self._config.addon_versions["eks-pod-identity-agent"]
            )

    def _add_external_snapshotter(self) -> None:
        """external-snapshotter の CRD だけを apply する。

        EBS CSI Driver の csi-snapshotter サイドカーは起動時に VolumeSnapshotClass CRD を
        watch するため、CRD が無いと "the server could not find the requested resource"
        エラーが永続的にログに出続ける。CRD さえあれば watch は成立してエラーは消える。

        VolumeSnapshot ワークフローを動かす snapshot-controller と、参照される
        VolumeSnapshotClass は実需が無いので導入しない。実バックアップ運用は
        AWS Backup の Backup Plan で broker / controller の EBS を並列スナップショット
        する想定で、Kubernetes 側で VolumeSnapshot リソースを作る予定が無いため。
        必要になった時点で snapshot-controller を足す。
        """
        self._cluster.add_manifest(
            "ExtSnapshotterCrds",
            *load_all(_DIR_SNAPSHOTTER, "crds.yaml"),
        )

    @property
    def strimzi_chart(self) -> eks.HelmChart:
        """Strimzi Kafka Operator の Helm chart リソース。

        strimzi-system namespace に配置する PodMonitor 等は、namespace 作成
        （chart 内の create_namespace=True）を待つ必要があるためこの chart に
        依存を張る。
        """
        return self._strimzi_chart

    def _add_strimzi(self) -> eks.HelmChart:
        self._cluster.add_manifest("Gp3StorageClass", load(_DIR, "gp3-storageclass.yaml"))

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
            },
            wait=True,
            timeout=Duration.minutes(10),
        )
        chart.node.add_dependency(sa)
        return chart
