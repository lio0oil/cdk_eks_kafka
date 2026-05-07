from aws_cdk import aws_eks_v2 as eks
from aws_cdk import aws_iam as iam
from constructs import Construct


class AddonsConstruct(Construct):
    def __init__(self, scope: Construct, construct_id: str, cluster: eks.ICluster) -> None:
        super().__init__(scope, construct_id)

        self._cluster: eks.ICluster = cluster

        self._add_eks_addons()
        self._add_argocd()

    def _add_eks_addons(self) -> None:
        # EBS CSI Driver用 IRSA
        ebs_csi_sa = self._cluster.add_service_account(
            "EbsCsiSa",
            name="ebs-csi-controller-sa",
            namespace="kube-system",
        )
        ebs_csi_sa.role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonEBSCSIDriverPolicy")
        )

        # L2 Addon コンストラクトを使用
        eks.Addon(
            self,
            "VpcCni",
            cluster=self._cluster,
            addon_name="vpc-cni",
        )

        eks.Addon(
            self,
            "CoreDns",
            cluster=self._cluster,
            addon_name="coredns",
        )

        eks.Addon(
            self,
            "KubeProxy",
            cluster=self._cluster,
            addon_name="kube-proxy",
        )

        eks.Addon(
            self,
            "PodIdentityAgent",
            cluster=self._cluster,
            addon_name="eks-pod-identity-agent",
        )

        eks.Addon(
            self,
            "EbsCsiDriver",
            cluster=self._cluster,
            addon_name="aws-ebs-csi-driver",
            configuration_values={"serviceAccount": {"annotations": {"eks.amazonaws.com/role-arn": ebs_csi_sa.role.role_arn}}},
        )

    def _add_argocd(self) -> None:
        # gp3 をデフォルトStorageClassとして設定（EBS CSI Driver依存）
        self._cluster.add_manifest(
            "Gp3StorageClass",
            {
                "apiVersion": "storage.k8s.io/v1",
                "kind": "StorageClass",
                "metadata": {
                    "name": "gp3",
                    "annotations": {"storageclass.kubernetes.io/is-default-class": "true"},
                },
                "provisioner": "ebs.csi.aws.com",
                "volumeBindingMode": "WaitForFirstConsumer",
                "reclaimPolicy": "Retain",
                "parameters": {
                    "type": "gp3",
                    "encrypted": "true",
                },
            },
        )

        self._cluster.add_helm_chart(
            "ArgoCD",
            chart="argo-cd",
            repository="https://argoproj.github.io/argo-helm",
            namespace="argocd",
            create_namespace=True,
            version="7.8.23",
            values={
                "server": {
                    "replicas": 2,
                    "autoscaling": {"enabled": True, "minReplicas": 2},
                    "service": {"type": "ClusterIP"},
                    "tolerations": [{"key": "CriticalAddonsOnly", "operator": "Exists"}],
                    "nodeSelector": {"role": "system"},
                },
                "applicationSet": {"replicaCount": 2},
                "controller": {
                    "tolerations": [{"key": "CriticalAddonsOnly", "operator": "Exists"}],
                    "nodeSelector": {"role": "system"},
                },
                "redis": {
                    "tolerations": [{"key": "CriticalAddonsOnly", "operator": "Exists"}],
                    "nodeSelector": {"role": "system"},
                },
                "repoServer": {
                    "tolerations": [{"key": "CriticalAddonsOnly", "operator": "Exists"}],
                    "nodeSelector": {"role": "system"},
                },
            },
        )
