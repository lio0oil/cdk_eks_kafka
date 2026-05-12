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
        """external-snapshotter (CRD + snapshot-controller + default VolumeSnapshotClass) を導入する。

        EBS CSI Driver の csi-snapshotter サイドカーは起動時に VolumeSnapshotClass CRD を
        watch するため、CRD が無いと "the server could not find the requested resource"
        エラーが永続的にログに出続ける。また Kubernetes 流の VolumeSnapshot ワークフロー
        (PVC.dataSource からの restore など) を使うには snapshot-controller も必要。

        Kafka クラスタ全体のアトミックバックアップは VolumeGroupSnapshot が EBS CSI Driver
        未対応の現状では実現できないため、実バックアップ運用は AWS Backup の Backup Plan で
        broker / controller の EBS を並列スナップショットする想定。ここで導入するのは
        Kubernetes 側の「スナップショット機能の有効化」であり、取得運用は別途設計する。
        """
        crds = self._cluster.add_manifest(
            "ExtSnapshotterCrds",
            *load_all(_DIR_SNAPSHOTTER, "crds.yaml"),
        )
        controller = self._cluster.add_manifest(
            "ExtSnapshotterController",
            *load_all(_DIR_SNAPSHOTTER, "controller.yaml"),
        )
        controller.node.add_dependency(crds)

        # deletionPolicy=Retain: VolumeSnapshot を誤削除しても EBS Snapshot は残す本番安全側
        # is-default-class アノテーション無し: VolumeSnapshot 作成時の明示指定を強制する
        vsc = self._cluster.add_manifest(
            "EbsVolumeSnapshotClass",
            load(_DIR_SNAPSHOTTER, "volumesnapshotclass.yaml"),
        )
        vsc.node.add_dependency(crds)

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
            },
            wait=True,
            timeout=Duration.minutes(10),
        )
        chart.node.add_dependency(sa)
        return chart
