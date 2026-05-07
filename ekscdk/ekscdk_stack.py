from typing import cast

from aws_cdk import Stack
from aws_cdk import aws_eks_v2 as eks
from aws_cdk import aws_iam as iam
from constructs import Construct

from ekscdk.constructs.network import NetworkConstruct
from ekscdk.constructs.eks_cluster import EksClusterConstruct
from ekscdk.constructs.addons import AddonsConstruct


class EksCdkStack(Stack):
    """インフラ + ArgoCD Stack（VPC / EKS / アドオン / ArgoCD + Bootstrap Application）

    必須コンテキスト:
      repo-url: GitリポジトリのURL (例: https://github.com/org/ekscdk)
    """

    def __init__(self, scope: Construct, construct_id: str, admin_role: iam.IRole, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        network = NetworkConstruct(self, "Network")
        eks_construct = EksClusterConstruct(self, "EksCluster", vpc=network.vpc, admin_role=admin_role)
        addons = AddonsConstruct(self, "Addons", cluster=cast(eks.ICluster, eks_construct.cluster))
        addons.node.add_dependency(eks_construct)
