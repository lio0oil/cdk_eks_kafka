from aws_cdk import CfnOutput, Stack
from aws_cdk import aws_iam as iam
from constructs import Construct

from ekscdk.constructs._manifest import manifest_dir, parse_kafka_nlb_ports
from ekscdk.constructs.addons import AddonsConstruct
from ekscdk.constructs.eks_cluster import EksClusterConstruct
from ekscdk.constructs.kafka import KafkaConstruct
from ekscdk.constructs.monitoring import MonitoringConstruct
from ekscdk.constructs.network import NetworkConstruct

_NLB_PORTS = parse_kafka_nlb_ports(manifest_dir("kafka"))
_BROKER_COUNT = len(_NLB_PORTS) - 1


class EksCdkStack(Stack):
    """インフラスタック（VPC / EKS / アドオン / Kafka）"""

    def __init__(
        self, scope: Construct, construct_id: str, admin_role: iam.IRole, cluster_name: str = "eks-cluster", **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        network = NetworkConstruct(self, "Network", nlb_ports=_NLB_PORTS)
        eks_construct = EksClusterConstruct(
            self, "EksCluster", vpc=network.vpc, admin_role=admin_role, broker_count=_BROKER_COUNT, cluster_name=cluster_name
        )
        addons = AddonsConstruct(self, "Addons", cluster=eks_construct.cluster)
        addons.node.add_dependency(eks_construct)
        MonitoringConstruct(self, "Monitoring", cluster=eks_construct.cluster, cluster_name=cluster_name)
        kafka = KafkaConstruct(
            self,
            "Kafka",
            cluster=eks_construct.cluster,
            broker_count=_BROKER_COUNT,
            nlb_dns_name=network.kafka_nlb.load_balancer_dns_name,
        )
        kafka.node.add_dependency(addons)

        CfnOutput(self, "KafkaNlbDnsName", value=network.kafka_nlb.load_balancer_dns_name)
