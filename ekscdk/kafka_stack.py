from aws_cdk import Stack
from aws_cdk import aws_eks_v2 as eks
from constructs import Construct

from ekscdk.constructs.kafka import KafkaConstruct


class KafkaStack(Stack):
    """Strimzi + Kafka ArgoCD Application Stack

    ArgoCD が manifests/kafka/ を Git から同期するため、
    git push だけで Kafka 設定変更がクラスターに反映される。

    必須コンテキスト:
      repo-url: GitリポジトリのURL (例: https://github.com/org/ekscdk)
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        cluster: eks.ICluster,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        repo_url: str = self.node.get_context("repo-url")
        KafkaConstruct(self, "Kafka", cluster=cluster, repo_url=repo_url)
