from aws_cdk import aws_eks_v2 as eks
from aws_cdk import aws_elasticloadbalancingv2 as elbv2
from constructs import Construct

from ekscdk.config import ClusterConfig
from ekscdk.constructs._manifest import (
    build_kafka_broker_configs,
    load_manifest,
    load_manifest_with_subs,
    manifest_dir,
)

_DIR = manifest_dir("kafka")
_DIR_MONITORING = manifest_dir("monitoring")


class KafkaConstruct(Construct):
    """Kafka 基盤 Construct（Kubernetes リソースのみ管理）

    - JMX メトリクス ConfigMap（Kafka broker 用 / Cruise Control 用の 2 件）
    - KafkaNodePool（controller x3 / broker x3）
    - Kafka CR（KRaft モード / 外部リスナー NodePort）
    - TargetGroupBinding（NLB TargetGroup と Strimzi NodePort Service の動的バインド）

    kafka Namespace は Strimzi chart（AddonsConstruct）の watchNamespaces が事前存在を
    要求するため AddonsConstruct 側で作成し、ここでは受け取って利用するだけ。
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
        kafka_target_port: int,
        nlb_sg_id: str,
        external_listener_name: str,
        aws_lbc_chart: eks.HelmChart,
        strimzi_chart: eks.HelmChart,
        kafka_namespace: eks.KubernetesManifest,
        config: ClusterConfig,
    ) -> None:
        super().__init__(scope, construct_id)

        # ── JMX メトリクス ConfigMap ──────────────────────────────────────────
        # Kafka broker 用と Cruise Control 用は Strimzi 公式サンプルでも別 ConfigMap
        # （名前・key が異なる）のため、1 つに統合せずそれぞれ apply する。ファイル自体は
        # 公式 examples/metrics/ の配置に合わせ manifests/monitoring/ 側に置くが、
        # Kafka CR がこの ConfigMap に依存するため apply の所有権は KafkaConstruct のまま。
        cm = cluster.add_manifest("KafkaMetricsCm", load_manifest(_DIR_MONITORING, "kafka-metrics.yaml"))
        cm.node.add_dependency(kafka_namespace)

        cruise_control_cm = cluster.add_manifest(
            "CruiseControlMetricsCm", load_manifest(_DIR_MONITORING, "kafka-cruise-control-metrics.yaml")
        )
        cruise_control_cm.node.add_dependency(kafka_namespace)

        # kafka_delete_claim を YAML の boolean リテラル文字列に変換（True → "true"）
        delete_claim_str = "true" if config.kafka_delete_claim else "false"

        # ── gp3-kafka StorageClass ───────────────────────────────────────────
        # Kafka broker/controller の I/O 特性は Prometheus/Alertmanager と異なるため、
        # AddonsConstruct が管理する default StorageClass（gp3）を共有せず、
        # broker/controller 専用の StorageClass を用意する（IOPS/throughput を
        # 監視系と切り離して個別チューニングできるようにするため）。
        cluster.add_manifest("Gp3KafkaStorageClass", load_manifest(_DIR, "gp3-kafka-storageclass.yaml"))

        # ── KafkaNodePool: controller ─────────────────────────────────────────
        # KafkaNodePool CRD は Strimzi Operator chart が導入するため、chart 未導入の
        # クラスタに apply すると CRD 未登録で失敗する。
        controller_pool = cluster.add_manifest(
            "KafkaControllerPool",
            load_manifest_with_subs(
                _DIR,
                "node-pool-controller.yaml",
                DELETE_CLAIM=delete_claim_str,
                CONTROLLER_REPLICAS=str(config.kafka_controller_count),
                CONTROLLER_MEMORY_REQUEST=config.kafka_controller_resources.memory_request,
                CONTROLLER_MEMORY_LIMIT=config.kafka_controller_resources.memory_limit,
                CONTROLLER_CPU_REQUEST=config.kafka_controller_resources.cpu_request,
                CONTROLLER_CPU_LIMIT=config.kafka_controller_resources.cpu_limit,
                CONTROLLER_STORAGE_SIZE=config.kafka_controller_resources.storage_size,
                CONTROLLER_JVM_XMS=config.kafka_controller_resources.jvm_xms,
                CONTROLLER_JVM_XMX=config.kafka_controller_resources.jvm_xmx,
            ),
        )
        controller_pool.node.add_dependency(kafka_namespace)
        controller_pool.node.add_dependency(strimzi_chart)

        # ── KafkaNodePool: broker ─────────────────────────────────────────────
        broker_pool = cluster.add_manifest(
            "KafkaBrokerPool",
            load_manifest_with_subs(
                _DIR,
                "node-pool-broker.yaml",
                BROKER_REPLICAS=str(broker_count),
                DELETE_CLAIM=delete_claim_str,
                BROKER_MEMORY_REQUEST=config.kafka_broker_resources.memory_request,
                BROKER_MEMORY_LIMIT=config.kafka_broker_resources.memory_limit,
                BROKER_CPU_REQUEST=config.kafka_broker_resources.cpu_request,
                BROKER_CPU_LIMIT=config.kafka_broker_resources.cpu_limit,
                BROKER_STORAGE_SIZE=config.kafka_broker_resources.storage_size,
                BROKER_JVM_XMS=config.kafka_broker_resources.jvm_xms,
                BROKER_JVM_XMX=config.kafka_broker_resources.jvm_xmx,
            ),
        )
        broker_pool.node.add_dependency(kafka_namespace)
        broker_pool.node.add_dependency(strimzi_chart)

        # ── Kafka CR ──────────────────────────────────────────────────────────
        # kafka-cluster.yaml の external listener には brokers[] を含めず、
        # ここで broker_count から生成して inject する（broker_count を単一の真実の源にする）。
        kafka_cr_manifest = load_manifest(_DIR, "kafka-cluster.yaml")
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
        kafka_cr.node.add_dependency(cruise_control_cm)
        kafka_cr.node.add_dependency(controller_pool)
        kafka_cr.node.add_dependency(broker_pool)

        # ── TargetGroupBinding ─────────────────────────────────────────────────
        # AWS LBC が Service Endpoints と TargetGroup を同期する。
        # Bootstrap: kafka-cluster-kafka-<listener>-bootstrap (全 broker pod を選択)
        #            bootstrap Service は Strimzi が pool 名と無関係に固定文字列
        #            `kafka` で生成するため、broker pool 名（broker）ではなく
        #            常に `kafka` を使う（実クラスタで確認済み）。
        # Broker N : kafka-cluster-broker-N             (broker ID N の pod を選択)
        #            こちらは broker KafkaNodePool 名（broker）に依存する。
        # TargetType=ip（network.py）のため AWS LBC は Service の EndpointSlice から
        # Pod IP を直接 target 登録する。ローリング更新時の pod 移動にも、Pod IP の
        # 変化にそのまま追従する形で対応できる。
        # Service の port は number ではなく name `tcp-<listener>` で参照する
        # （Kubernetes Service の慣習：port は name 参照が推奨）。
        port_name = f"tcp-{external_listener_name}"
        for tg_key, tg in kafka_target_groups.items():
            if tg_key == "Bootstrap":
                service_name = f"kafka-cluster-kafka-{external_listener_name}-bootstrap"
                binding_name = f"kafka-{external_listener_name}-bootstrap"
            else:
                # "Broker0" -> 0
                broker_id = tg_key.removeprefix("Broker")
                service_name = f"kafka-cluster-broker-{broker_id}"
                binding_name = f"kafka-broker-{broker_id}"

            binding = cluster.add_manifest(
                f"TargetGroupBinding{tg_key}",
                load_manifest_with_subs(
                    _DIR,
                    "target-group-binding.yaml",
                    BINDING_NAME=binding_name,
                    SERVICE_NAME=service_name,
                    SERVICE_PORT=port_name,
                    TARGET_GROUP_ARN=tg.target_group_arn,
                    NLB_SG_ID=nlb_sg_id,
                    TARGET_PORT=str(kafka_target_port),
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
        test_topic = cluster.add_manifest("KafkaTopicTestTopic", load_manifest(_DIR, "topics/test-topic.yaml"))
        test_topic.node.add_dependency(kafka_cr)
