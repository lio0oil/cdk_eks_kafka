import copy
from pathlib import Path

import yaml
from aws_cdk import aws_eks_v2 as eks
from constructs import Construct

_MANIFESTS = Path(__file__).parent.parent.parent / "manifests"


def _load(rel_path: str) -> dict:
    return yaml.safe_load((_MANIFESTS / rel_path).read_text())


class KafkaConstruct(Construct):
    """
    CDK は ArgoCD Application を 2 つ apply する。
      1. strimzi-operator: Helm チャートから Strimzi をインストール
      2. kafka-cluster:    manifests/kafka/ を Git から同期（GitOps）
    Kafka CR 本体は manifests/kafka/kafka-cluster.yaml で管理し、
    git push によって ArgoCD 経由でクラスターに自動反映される。
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        cluster: eks.ICluster,
        repo_url: str,
    ) -> None:
        super().__init__(scope, construct_id)

        self._cluster: eks.ICluster = cluster
        self._repo_url = repo_url

        strimzi_app = self._add_strimzi_operator()
        self._add_kafka_cluster_app(strimzi_app)

    def _add_strimzi_operator(self) -> eks.KubernetesManifest:
        manifest = _load("argocd/strimzi-operator.yaml")
        return self._cluster.add_manifest("StrimziOperatorApp", manifest)

    def _add_kafka_cluster_app(self, strimzi_app: eks.KubernetesManifest) -> None:
        manifest = copy.deepcopy(_load("argocd/kafka-cluster-app.yaml"))
        manifest["spec"]["source"]["repoURL"] = self._repo_url

        kafka_app = self._cluster.add_manifest("KafkaClusterApp", manifest)
        kafka_app.node.add_dependency(strimzi_app)
