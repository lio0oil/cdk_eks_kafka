from aws_cdk import CfnOutput, Stack
from aws_cdk import aws_iam as iam
from constructs import Construct

from ekscdk.constructs.addons import AddonsConstruct
from ekscdk.constructs.eks_cluster import EksClusterConstruct
from ekscdk.constructs.monitoring import MonitoringConstruct
from ekscdk.constructs.network import NetworkConstruct


class EksCdkStack(Stack):
    """インフラ + ArgoCD Stack（VPC / EKS / アドオン / ArgoCD + Bootstrap Application）

    必須コンテキスト:
      repo-url: GitリポジトリのURL (例: https://github.com/org/ekscdk)
    """

    def __init__(
        self, scope: Construct, construct_id: str, admin_role: iam.IRole, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        network = NetworkConstruct(self, "Network")
        eks_construct = EksClusterConstruct(
            self, "EksCluster", vpc=network.vpc, admin_role=admin_role
        )
        addons = AddonsConstruct(self, "Addons", cluster=eks_construct.cluster)
        addons.node.add_dependency(eks_construct)
        MonitoringConstruct(self, "Monitoring", cluster=eks_construct.cluster)

        # ── Outputs ──────────────────────────────────────────────────────────
        # manifests/kafka/privatelink.yaml の書き換えに使用する値を出力
        CfnOutput(
            self,
            "VpcId",
            value=network.vpc.vpc_id,
            description="VPC ID for privatelink.yaml",
        )
        CfnOutput(
            self,
            "KafkaSharedNlbArn",
            value=network.kafka_nlb.load_balancer_arn,
            description="Shared NLB ARN for privatelink.yaml",
        )
