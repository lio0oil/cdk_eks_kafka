from aws_cdk import Stack
from constructs import Construct

from ekscdk.constructs.network import NetworkConstruct
from ekscdk.constructs.eks_cluster import EksClusterConstruct
from ekscdk.constructs.addons import AddonsConstruct
from ekscdk.constructs.kafka import KafkaConstruct


class EksCdkStack(Stack):
    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        network = NetworkConstruct(self, "Network")
        eks = EksClusterConstruct(self, "EksCluster", vpc=network.vpc)
        addons = AddonsConstruct(self, "Addons", cluster=eks.cluster)
        KafkaConstruct(self, "Kafka", cluster=eks.cluster)

        # KafkaのデプロイはArgoCDインストール後に行う
        addons.node.add_dependency(eks)
