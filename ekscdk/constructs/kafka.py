from aws_cdk import aws_eks_v2 as eks
from constructs import Construct

from ekscdk.constructs._manifest import load, load_with_subs, manifest_dir

_DIR = manifest_dir("kafka")


class KafkaConstruct(Construct):
    """Kafka 基盤 Construct（Kubernetes リソースのみ管理）

    - kafka Namespace
    - JMX メトリクス ConfigMap
    - KafkaNodePool（controller x3 / broker x3）
    - Kafka CR（KRaft モード / 外部リスナー NodePort）

    NLB / SG / Listener / TargetGroup は NetworkConstruct が管理する。
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        cluster: eks.ICluster,
        broker_count: int,
        nlb_dns_name: str,
    ) -> None:
        super().__init__(scope, construct_id)

        # ── Namespace ─────────────────────────────────────────────────────────
        namespace = cluster.add_manifest(
            "KafkaNamespace", load(_DIR, "namespace.yaml")
        )

        # ── JMX メトリクス ConfigMap ──────────────────────────────────────────
        cm = cluster.add_manifest("KafkaMetricsCm", load(_DIR, "cm.yaml"))
        cm.node.add_dependency(namespace)

        # ── KafkaNodePool: controller ─────────────────────────────────────────
        controller_pool = cluster.add_manifest("KafkaControllerPool", load(_DIR, "node-pool-controller.yaml"))
        controller_pool.node.add_dependency(namespace)

        # ── KafkaNodePool: broker ─────────────────────────────────────────────
        broker_pool = cluster.add_manifest(
            "KafkaBrokerPool",
            load_with_subs(_DIR, "node-pool-broker.yaml", BROKER_REPLICAS=str(broker_count)),
        )
        broker_pool.node.add_dependency(namespace)

        # ── Kafka CR ──────────────────────────────────────────────────────────
        kafka_cr = cluster.add_manifest(
            "KafkaCluster",
            load_with_subs(_DIR, "kafka-cluster.yaml", KAFKA_ADVERTISED_HOST=nlb_dns_name),
        )
        kafka_cr.node.add_dependency(cm)
        kafka_cr.node.add_dependency(controller_pool)
        kafka_cr.node.add_dependency(broker_pool)
