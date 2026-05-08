from aws_cdk import Duration
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_elasticloadbalancingv2 as elbv2
from aws_cdk import aws_eks_v2 as eks
from constructs import Construct

from ekscdk.constructs._manifest import load, load_with_subs, manifest_dir

_DIR = manifest_dir("kafka")


def _parse_nlb_ports() -> list[tuple[str, int, int]]:
    """kafka-cluster.yaml の external listener 設定から (name, listener_port, node_port) を返す。

    host プレースホルダーが残っていても yaml.safe_load は文字列として読めるため問題ない。
    """
    manifest = load(_DIR, "kafka-cluster.yaml")
    external = next(
        l for l in manifest["spec"]["kafka"]["listeners"]
        if l["name"] == "external"
    )
    cfg = external["configuration"]
    return [("Bootstrap", external["port"], cfg["bootstrap"]["nodePort"])] + [
        (f"Broker{b['broker']}", b["advertisedPort"], b["nodePort"])
        for b in cfg["brokers"]
    ]


_NLB_PORTS = _parse_nlb_ports()
BROKER_COUNT: int = len(_NLB_PORTS) - 1  # Bootstrap を除いたブローカー数（kafka-cluster.yaml 由来）


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
            "KafkaNamespace", load(_DIR,"namespace.yaml")
        )

        # ── JMX メトリクス ConfigMap ──────────────────────────────────────────
        cm = cluster.add_manifest("KafkaMetricsCm", load(_DIR,"cm.yaml"))
        cm.node.add_dependency(namespace)

        # ── KafkaNodePool: controller ─────────────────────────────────────────
        controller_pool = cluster.add_manifest("KafkaControllerPool", load(_DIR,"node-pool-controller.yaml"))
        controller_pool.node.add_dependency(namespace)

        # ── KafkaNodePool: broker ─────────────────────────────────────────────
        broker_pool = cluster.add_manifest(
            "KafkaBrokerPool",
            load_with_subs(_DIR, "node-pool-broker.yaml", BROKER_REPLICAS=str(BROKER_COUNT)),
        )
        broker_pool.node.add_dependency(namespace)

        # ── Kafka CR ──────────────────────────────────────────────────────────
        kafka_cr = cluster.add_manifest(
            "KafkaCluster",
            load_with_subs(_DIR, "kafka-cluster.yaml", KAFKA_ADVERTISED_HOST=nlb.load_balancer_dns_name),
        )
        kafka_cr.node.add_dependency(cm)
        kafka_cr.node.add_dependency(controller_pool)
        kafka_cr.node.add_dependency(broker_pool)

        # ── NLB TargetGroup + Listener ────────────────────────────────────────
        for name, listener_port, node_port in _NLB_PORTS:
            tg = elbv2.NetworkTargetGroup(
                self,
                f"Kafka{name}Tg",
                vpc=vpc,
                port=node_port,
                protocol=elbv2.Protocol.TCP,
                target_type=elbv2.TargetType.INSTANCE,
                health_check=elbv2.HealthCheck(
                    port=str(node_port),
                    protocol=elbv2.Protocol.TCP,
                    healthy_threshold_count=2,
                    unhealthy_threshold_count=2,
                    interval=Duration.seconds(10),
                ),
            )
            elbv2.NetworkListener(
                self,
                f"Kafka{name}Listener",
                load_balancer=nlb,
                port=listener_port,
                protocol=elbv2.Protocol.TCP,
                default_target_groups=[tg],
            )
