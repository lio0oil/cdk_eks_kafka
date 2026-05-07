from aws_cdk import Stack
from constructs import Construct

from ekscdk.constructs.kafka_privatelink import KafkaPrivateLinkConstruct


class PrivateLinkStack(Stack):
    """PrivateLink Stack（KafkaのNLBプロビジョニング後にデプロイ）

    前提: KafkaStack デプロイ後に ArgoCD が Strimzi bootstrap NLB を作成済みであること。
    NLB ARN を CDK コンテキストで渡す:
      cdk deploy PrivateLinkStack -c kafka-bootstrap-nlb-arn=<ARN>
    """

    def __init__(self, scope: Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        nlb_arn: str | None = self.node.try_get_context("kafka-bootstrap-nlb-arn")
        if nlb_arn:
            KafkaPrivateLinkConstruct(self, "KafkaPrivateLink", nlb_arn=nlb_arn)
