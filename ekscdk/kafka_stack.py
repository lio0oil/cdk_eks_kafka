from aws_cdk import Stack
from aws_cdk import aws_eks_v2 as eks
from constructs import Construct

from ekscdk.constructs.kafka import KafkaConstruct


class KafkaStack(Stack):
    """Strimzi + Kafka CR Stack（ArgoCD経由でNLBも自動作成）"""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        cluster: eks.Cluster,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        KafkaConstruct(self, "Kafka", cluster=cluster)
