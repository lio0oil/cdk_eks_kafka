import json
import os
from typing import cast

from aws_cdk import Duration, Stack
from aws_cdk import aws_eks_v2 as eks
from aws_cdk import aws_iam as iam
from constructs import Construct

from ekscdk.config import ClusterConfig
from ekscdk.constructs._manifest import load, manifest_dir

_DIR = manifest_dir("addons")


class AddonsConstruct(Construct):
    def __init__(
        self, scope: Construct, construct_id: str, cluster: eks.ICluster, config: ClusterConfig
    ) -> None:
        super().__init__(scope, construct_id)

        self._cluster: eks.ICluster = cluster
        self._config = config

        self._add_eks_addons()
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
            iam.ManagedPolicy.from_aws_managed_policy_name(
                "service-role/AmazonEBSCSIDriverPolicy"
            )
        )

        for addon_name, construct_id in {
            "vpc-cni":            "VpcCni",
            "coredns":            "CoreDns",
            "kube-proxy":         "KubeProxy",
            "aws-ebs-csi-driver": "EbsCsiDriver",
        }.items():
            addon = eks.Addon(self, construct_id, cluster=self._cluster,
                              addon_name=addon_name, addon_version=self._config.addon_versions[addon_name])
            # aws_eks_v2.Addon は ResolveConflicts を公開していないためエスケープハッチで設定する。
            # OVERWRITE にしないと既存 SA のラベルと衝突してデプロイが失敗する。
            addon.node.default_child.add_override("Properties.ResolveConflicts", "OVERWRITE")

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
                "tolerations": [
                    {"key": "CriticalAddonsOnly", "operator": "Equal", "value": "true", "effect": "NoSchedule"}
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
                # システムノードのみで稼働させる
                "tolerations": [
                    {"key": "CriticalAddonsOnly", "operator": "Equal", "value": "true", "effect": "NoSchedule"}
                ],
                "replicaCount": 2,
            },
            wait=True,
            timeout=Duration.minutes(10),
        )
        chart.node.add_dependency(sa)
        return chart
