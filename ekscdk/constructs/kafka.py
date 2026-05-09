from aws_cdk import aws_eks_v2 as eks
from aws_cdk import aws_elasticloadbalancingv2 as elbv2
from constructs import Construct

from ekscdk.constructs._manifest import load, load_with_subs, manifest_dir

_DIR = manifest_dir("kafka")


class KafkaConstruct(Construct):
    """Kafka 基盤 Construct（Kubernetes リソースのみ管理）

    - kafka Namespace
    - JMX メトリクス ConfigMap
    - KafkaNodePool（controller x3 / broker x3）
    - Kafka CR（KRaft モード / 外部リスナー NodePort）
    - TargetGroupBinding（NLB TargetGroup と Strimzi NodePort Service の動的バインド）

    NLB / SG / Listener / TargetGroup は NetworkConstruct が管理する。
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        cluster: eks.ICluster,
        broker_count: int,
        nlb_dns_name: str,
        kafka_target_groups: dict[str, elbv2.NetworkTargetGroup],
        external_listener_port: int,
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

        # ── Kafka Exporter Service ─────────────────────────────────────────────
        # Strimzi 1.0.0 は kafka-exporter の Service を自動作成しないため CDK で管理する
        exporter_svc = cluster.add_manifest(
            "KafkaExporterService", load(_DIR, "kafka-exporter-service.yaml")
        )
        exporter_svc.node.add_dependency(kafka_cr)

        # ── TargetGroupBinding ─────────────────────────────────────────────────
        # AWS LBC が Service Endpoints と TargetGroup を同期する。
        # Bootstrap: kafka-cluster-kafka-external-bootstrap (全 broker pod を選択)
        # Broker N : kafka-cluster-kafka-N (broker ID N の pod を選択)
        # ローリング更新時の pod 移動にも追従するため、TargetType=instance でも
        # Endpoints があるノードのみが NLB ターゲットになる。
        for tg_key, tg in kafka_target_groups.items():
            if tg_key == "Bootstrap":
                service_name = "kafka-cluster-kafka-external-bootstrap"
                binding_name = "kafka-external-bootstrap"
            else:
                # "Broker0" -> 0
                broker_id = tg_key.removeprefix("Broker")
                service_name = f"kafka-cluster-kafka-{broker_id}"
                binding_name = f"kafka-broker-{broker_id}"

            binding = cluster.add_manifest(
                f"TargetGroupBinding{tg_key}",
                load_with_subs(
                    _DIR, "target-group-binding.yaml",
                    BINDING_NAME=binding_name,
                    SERVICE_NAME=service_name,
                    SERVICE_PORT=str(external_listener_port),
                    TARGET_GROUP_ARN=tg.target_group_arn,
                ),
            )
            binding.node.add_dependency(kafka_cr)
