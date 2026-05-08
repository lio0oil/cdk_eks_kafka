import os

import yaml
from aws_cdk import aws_eks_v2 as eks
from constructs import Construct

_MANIFESTS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "manifests", "kafka")


def _load(filename: str) -> dict:
    with open(os.path.join(_MANIFESTS_DIR, filename)) as f:
        return yaml.safe_load(f)


def _load_all_with_subs(filename: str, **subs: str) -> list[dict]:
    with open(os.path.join(_MANIFESTS_DIR, filename)) as f:
        text = f.read()
    for placeholder, value in subs.items():
        text = text.replace(f"<REPLACE_WITH_CDK_OUTPUT_{placeholder}>", value)
    return [doc for doc in yaml.safe_load_all(text) if doc is not None]


def _manifest_id(name: str) -> str:
    """'kafka-bootstrap-tg' → 'KafkaBootstrapTg'"""
    return "".join(part.capitalize() for part in name.replace("-", " ").split())


class KafkaConstruct(Construct):
    """Kafka 基盤 Construct

    Kubernetes リソース（CDK 管理）:
      - kafka Namespace
      - JMX メトリクス ConfigMap
      - KafkaNodePool（controller x3 / broker x3）
      - Kafka CR（KRaft モード / 外部リスナー NodePort）
      - ACK TargetGroup + Listener（Shared NLB のリスナー設定）

    VPC Endpoint Service 本体は NetworkConstruct が管理する。
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        cluster: eks.ICluster,
        vpc_id: str,
        nlb_arn: str,
    ) -> None:
        super().__init__(scope, construct_id)

        # ── Namespace ─────────────────────────────────────────────────────────
        namespace = cluster.add_manifest(
            "KafkaNamespace",
            {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "kafka"}},
        )

        # ── JMX メトリクス ConfigMap ──────────────────────────────────────────
        cm = cluster.add_manifest("KafkaMetricsCm", _load("cm.yaml"))
        cm.node.add_dependency(namespace)

        # ── KafkaNodePool: controller ─────────────────────────────────────────
        controller_pool = cluster.add_manifest("KafkaControllerPool", _load("node-pool-controller.yaml"))
        controller_pool.node.add_dependency(namespace)

        # ── KafkaNodePool: broker ─────────────────────────────────────────────
        broker_pool = cluster.add_manifest("KafkaBrokerPool", _load("node-pool-broker.yaml"))
        broker_pool.node.add_dependency(namespace)

        # ── Kafka CR ──────────────────────────────────────────────────────────
        kafka_cr = cluster.add_manifest("KafkaCluster", _load("kafka-cluster.yaml"))
        kafka_cr.node.add_dependency(cm)
        kafka_cr.node.add_dependency(controller_pool)
        kafka_cr.node.add_dependency(broker_pool)

        # ── ACK TargetGroup / Listener（NLB リスナー設定）────────────────────
        # VPCEndpointService は NetworkConstruct で CDK 管理済みのため除外する
        pl_docs = _load_all_with_subs(
            "privatelink.yaml",
            VpcId=vpc_id,
            KafkaSharedNlbArn=nlb_arn,
        )
        tg_by_name = {}
        listener_queue = []
        for doc in pl_docs:
            if doc["kind"] == "VPCEndpointService":
                continue
            cdk_id = _manifest_id(doc["metadata"]["name"])
            if doc["kind"] == "TargetGroup":
                tg = cluster.add_manifest(cdk_id, doc)
                tg.node.add_dependency(namespace)
                tg_by_name[doc["metadata"]["name"]] = tg
            elif doc["kind"] == "Listener":
                listener_queue.append((cdk_id, doc))

        for cdk_id, doc in listener_queue:
            listener = cluster.add_manifest(cdk_id, doc)
            tg_ref = doc["spec"]["defaultActions"][0]["targetGroupRef"]["from"]["name"]
            if tg_ref in tg_by_name:
                listener.node.add_dependency(tg_by_name[tg_ref])
