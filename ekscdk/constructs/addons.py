from aws_cdk import aws_eks_v2 as eks
from aws_cdk import aws_iam as iam
from constructs import Construct

from ekscdk.constructs._manifest import load, manifest_dir

_DIR = manifest_dir("addons")


class AddonsConstruct(Construct):
    def __init__(
        self, scope: Construct, construct_id: str, cluster: eks.ICluster
    ) -> None:
        super().__init__(scope, construct_id)

        self._cluster: eks.ICluster = cluster

        self._add_eks_addons()
        self._add_strimzi()

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

        # Kubernetes 1.35 向け最新バージョン（2026-05 時点）
        # 更新時は全アドオンを一括確認:
        #   for addon in vpc-cni coredns kube-proxy eks-pod-identity-agent aws-ebs-csi-driver; do
        #     echo -n "$addon: "
        #     aws eks describe-addon-versions --addon-name "$addon" \
        #       --kubernetes-version 1.35 \
        #       --query 'addons[0].addonVersions[0].addonVersion' --output text
        #   done
        eks.Addon(self, "VpcCni", cluster=self._cluster,
                  addon_name="vpc-cni", addon_version="v1.21.1-eksbuild.8")
        eks.Addon(self, "CoreDns", cluster=self._cluster,
                  addon_name="coredns", addon_version="v1.14.2-eksbuild.4")
        eks.Addon(self, "KubeProxy", cluster=self._cluster,
                  addon_name="kube-proxy", addon_version="v1.35.3-eksbuild.5")
        eks.Addon(self, "PodIdentityAgent", cluster=self._cluster,
                  addon_name="eks-pod-identity-agent", addon_version="v1.3.10-eksbuild.3")
        eks.Addon(self, "EbsCsiDriver", cluster=self._cluster,
                  addon_name="aws-ebs-csi-driver", addon_version="v1.59.0-eksbuild.1")

    def _add_strimzi(self) -> None:
        self._cluster.add_manifest("Gp3StorageClass", load(_DIR, "gp3-storageclass.yaml"))

        self._cluster.add_helm_chart(
            "StrimziOperator",
            chart="strimzi-kafka-operator",
            repository="https://strimzi.io/charts/",
            namespace="strimzi-system",
            create_namespace=True,
            version="1.0.0",
            values={
                "watchNamespaces": ["kafka"],
                "replicas": 2,
            },
        )
