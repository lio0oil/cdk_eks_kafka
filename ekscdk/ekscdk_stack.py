from aws_cdk import Stack
from aws_cdk import aws_iam as iam
from constructs import Construct

from ekscdk.constructs.addons import AddonsConstruct
from ekscdk.constructs.eks_cluster import EksClusterConstruct
from ekscdk.constructs.kafka import KafkaConstruct
from ekscdk.constructs.monitoring import MonitoringConstruct
from ekscdk.constructs.network import NetworkConstruct


class EksCdkStack(Stack):
    """インフラスタック（VPC / EKS / アドオン / Kafka）"""

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
        kafka = KafkaConstruct(
            self,
            "Kafka",
            cluster=eks_construct.cluster,
            vpc=network.vpc,
            nlb=network.kafka_nlb,
        )
        kafka.node.add_dependency(addons)
