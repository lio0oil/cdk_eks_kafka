from aws_cdk import aws_eks_v2 as eks
from aws_cdk import aws_elasticloadbalancingv2 as elbv2
from constructs import Construct

from ekscdk.constructs._manifest import (
    build_kafka_broker_configs,
    load,
    load_with_subs,
    manifest_dir,
)

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
        nlb_ports: list[tuple[str, int, int]],
        nlb_sg_id: str,
        external_listener_name: str,
        aws_lbc_chart: eks.HelmChart,
        delete_claim: bool,
        controller_count: int,
    ) -> None:
        super().__init__(scope, construct_id)

        # ── Namespace ─────────────────────────────────────────────────────────
        namespace = cluster.add_manifest("KafkaNamespace", load(_DIR, "namespace.yaml"))
        # PodMonitor が kafka NS に lookup できるよう、MonitoringConstruct から
        # 依存を張れる参照として外に公開する。
        self.kafka_namespace = namespace

        # ── JMX メトリクス ConfigMap ──────────────────────────────────────────
        cm = cluster.add_manifest("KafkaMetricsCm", load(_DIR, "cm.yaml"))
        cm.node.add_dependency(namespace)

        # delete_claim を YAML の boolean リテラル文字列に変換（True → "true"）
        delete_claim_str = "true" if delete_claim else "false"

        # ── KafkaNodePool: controller ─────────────────────────────────────────
        controller_pool = cluster.add_manifest(
            "KafkaControllerPool",
            load_with_subs(
                _DIR,
                "node-pool-controller.yaml",
                DELETE_CLAIM=delete_claim_str,
                CONTROLLER_REPLICAS=str(controller_count),
            ),
        )
        controller_pool.node.add_dependency(namespace)

        # ── KafkaNodePool: broker ─────────────────────────────────────────────
        broker_pool = cluster.add_manifest(
            "KafkaBrokerPool",
            load_with_subs(
                _DIR,
                "node-pool-broker.yaml",
                BROKER_REPLICAS=str(broker_count),
                DELETE_CLAIM=delete_claim_str,
            ),
        )
        broker_pool.node.add_dependency(namespace)

        # ── Kafka CR ──────────────────────────────────────────────────────────
        # kafka-cluster.yaml の external listener には brokers[] を含めず、
        # ここで broker_count から生成して inject する（broker_count を単一の真実の源にする）。
        kafka_cr_manifest = load(_DIR, "kafka-cluster.yaml")
        external_listener = next(
            listener
            for listener in kafka_cr_manifest["spec"]["kafka"]["listeners"]
            if listener["name"] == external_listener_name
        )
        external_listener["configuration"]["brokers"] = build_kafka_broker_configs(
            broker_count=broker_count,
            advertised_host=nlb_dns_name,
        )
        kafka_cr = cluster.add_manifest("KafkaCluster", kafka_cr_manifest)
        kafka_cr.node.add_dependency(cm)
        kafka_cr.node.add_dependency(controller_pool)
        kafka_cr.node.add_dependency(broker_pool)

        # ── TargetGroupBinding ─────────────────────────────────────────────────
        # AWS LBC が Service Endpoints と TargetGroup を同期する。
        # Bootstrap: kafka-cluster-kafka-<listener>-bootstrap (全 broker pod を選択)
        # Broker N : kafka-cluster-kafka-N             (broker ID N の pod を選択)
        # ローリング更新時の pod 移動にも追従するため、TargetType=instance でも
        # Endpoints があるノードのみが NLB ターゲットになる。
        # Service の port は number ではなく name `tcp-<listener>` で参照する
        # （Kubernetes Service の慣習：port は name 参照が推奨）。
        port_name = f"tcp-{external_listener_name}"
        node_ports_by_name = {name: node_port for name, _, node_port in nlb_ports}
        for tg_key, tg in kafka_target_groups.items():
            if tg_key == "Bootstrap":
                service_name = f"kafka-cluster-kafka-{external_listener_name}-bootstrap"
                binding_name = f"kafka-{external_listener_name}-bootstrap"
            else:
                # "Broker0" -> 0
                broker_id = tg_key.removeprefix("Broker")
                service_name = f"kafka-cluster-kafka-{broker_id}"
                binding_name = f"kafka-broker-{broker_id}"

            binding = cluster.add_manifest(
                f"TargetGroupBinding{tg_key}",
                load_with_subs(
                    _DIR,
                    "target-group-binding.yaml",
                    BINDING_NAME=binding_name,
                    SERVICE_NAME=service_name,
                    SERVICE_PORT=port_name,
                    TARGET_GROUP_ARN=tg.target_group_arn,
                    NLB_SG_ID=nlb_sg_id,
                    NODE_PORT=str(node_ports_by_name[tg_key]),
                ),
            )
            binding.node.add_dependency(kafka_cr)
            # AWS LBC が提供する CRD (TargetGroupBinding) のインストール完了を待つ。
            # construct レベルの add_dependency だけでは個別 manifest の DependsOn が
            # 確実に伝搬しないため、リソース単位で明示する。
            binding.node.add_dependency(aws_lbc_chart)

        # ── KafkaTopic ────────────────────────────────────────────────────────
        # Strimzi Topic Operator (entityOperator) が KafkaTopic CR を監視し
        # 実 Kafka に Topic を作成する。Operator は Kafka CR より後に起動するため
        # kafka_cr への依存だけで apply 順序は十分。
        test_topic = cluster.add_manifest("KafkaTopicTestTopic", load(_DIR, "topics/test-topic.yaml"))
        test_topic.node.add_dependency(kafka_cr)
