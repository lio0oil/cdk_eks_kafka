from aws_cdk import Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_eks_v2 as eks
from constructs import Construct

from ekscdk.constructs.network import NetworkConstruct
from ekscdk.constructs.eks_cluster import EksClusterConstruct
from ekscdk.constructs.addons import AddonsConstruct


class EksCdkStack(Stack):
    """インフラ + ArgoCD Stack（VPC / EKS / アドオン / ArgoCD）"""

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        network = NetworkConstruct(self, "Network")
        eks_construct = EksClusterConstruct(self, "EksCluster", vpc=network.vpc)
        addons = AddonsConstruct(self, "Addons", cluster=eks_construct.cluster)
        addons.node.add_dependency(eks_construct)

        self.cluster: eks.Cluster = eks_construct.cluster
        self.vpc: ec2.Vpc = network.vpc
