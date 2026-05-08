import os

import yaml
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_elasticloadbalancingv2 as elbv2
from aws_cdk import aws_eks_v2 as eks
from constructs import Construct

_MANIFESTS_DIR = os.path.join(os.path.dirname(__file__), "..", "..", "manifests", "kafka")

_BROKER_PORTS = [
    ("Bootstrap", 9094, 30094),
    ("Broker0",   9095, 30095),
    ("Broker1",   9096, 30096),
    ("Broker2",   9097, 30097),
]


def _load(filename: str) -> dict:
    with open(os.path.join(_MANIFESTS_DIR, filename)) as f:
        return yaml.safe_load(f)


class KafkaConstruct(Construct):
    """Kafka 基盤 Construct

    Kubernetes リソース（CDK 管理）:
      - kafka Namespace
      - JMX メトリクス ConfigMap
      - KafkaNodePool（controller x3 / broker x3）
      - Kafka CR（KRaft モード / 外部リスナー NodePort）

    AWS リソース（CDK 管理）:
      - NLB TargetGroup + Listener（bootstrap / broker 0〜2）

    VPC Endpoint Service 本体は NetworkConstruct が管理する。
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        cluster: eks.ICluster,
        vpc: ec2.IVpc,
        nlb: elbv2.INetworkLoadBalancer,
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

        # ── NLB TargetGroup + Listener ────────────────────────────────────────
        for name, listener_port, node_port in _BROKER_PORTS:
            tg = elbv2.NetworkTargetGroup(
                self,
                f"Kafka{name}Tg",
                vpc=vpc,
                port=node_port,
                protocol=elbv2.Protocol.TCP,
                target_type=elbv2.TargetType.INSTANCE,
            )
            elbv2.NetworkListener(
                self,
                f"Kafka{name}Listener",
                load_balancer=nlb,
                port=listener_port,
                protocol=elbv2.Protocol.TCP,
                default_target_groups=[tg],
            )
