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

        for addon_name, construct_id in {
            "vpc-cni":                "VpcCni",
            "coredns":                "CoreDns",
            "kube-proxy":             "KubeProxy",
            "eks-pod-identity-agent": "PodIdentityAgent",
            "aws-ebs-csi-driver":     "EbsCsiDriver",
        }.items():
            eks.Addon(self, construct_id, cluster=self._cluster,
                      addon_name=addon_name, addon_version=self._config.addon_versions[addon_name])

    def _add_strimzi(self) -> None:
        self._cluster.add_manifest("Gp3StorageClass", load(_DIR, "gp3-storageclass.yaml"))

        self._cluster.add_helm_chart(
            "StrimziOperator",
            chart="strimzi-kafka-operator",
            repository="https://strimzi.io/charts/",
            namespace="strimzi-system",
            create_namespace=True,
            version=self._config.strimzi_version,
            values={
                "watchNamespaces": ["kafka"],
                "replicas": 2,
            },
        )
