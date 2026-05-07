from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_elasticloadbalancingv2 as elbv2
from constructs import Construct


class KafkaPrivateLinkConstruct(Construct):
    """
    Kafka bootstrap NLBにPrivateLinkを設定する。
    Strimziがbootstrap Serviceを作成してNLBをプロビジョニングした後に
    NLBのARNをCDKコンテキストで渡してデプロイする。

    デプロイ手順:
      1. KafkaStack をデプロイ（Strimziが NLB を作成）
      2. NLB ARN を取得:
         kubectl get svc kafka-cluster-kafka-external-bootstrap -n kafka \\
           -o jsonpath='{.metadata.annotations.service\\.beta\\.kubernetes\\.io/aws-load-balancer-arn}'
      3. PrivateLinkStack をデプロイ:
         cdk deploy PrivateLinkStack -c kafka-bootstrap-nlb-arn=<ARN>
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        nlb_arn: str,
    ) -> None:
        super().__init__(scope, construct_id)

        nlb = elbv2.NetworkLoadBalancer.from_network_load_balancer_attributes(
            self,
            "BootstrapNlb",
            load_balancer_arn=nlb_arn,
        )

        self.endpoint_service = ec2.VpcEndpointService(
            self,
            "EndpointService",
            vpc_endpoint_service_load_balancers=[nlb],
            acceptance_required=False,
        )
