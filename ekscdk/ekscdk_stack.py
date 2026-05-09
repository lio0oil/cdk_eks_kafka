from aws_cdk import CfnOutput, Stack
from aws_cdk import aws_iam as iam
from constructs import Construct

from ekscdk.config import ClusterConfig
from ekscdk.constructs._manifest import (
    manifest_dir,
    parse_kafka_external_listener,
    parse_kafka_nlb_ports,
)
from ekscdk.constructs.addons import AddonsConstruct
from ekscdk.constructs.eks_cluster import EksClusterConstruct
from ekscdk.constructs.kafka import KafkaConstruct
from ekscdk.constructs.monitoring import MonitoringConstruct
from ekscdk.constructs.network import NetworkConstruct


class EksCdkStack(Stack):
    """インフラスタック（VPC / EKS / アドオン / Kafka）"""

    def __init__(
        self, scope: Construct, construct_id: str, admin_role: iam.IRole, config: ClusterConfig, **kwargs
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        kafka_dir = manifest_dir("kafka")
        nlb_ports = parse_kafka_nlb_ports(kafka_dir)
        external_listener_name, _ = parse_kafka_external_listener(kafka_dir)
        broker_count = len(nlb_ports) - 1

        network = NetworkConstruct(self, "Network", nlb_ports=nlb_ports, config=config)
        eks_construct = EksClusterConstruct(
            self, "EksCluster", vpc=network.vpc, admin_role=admin_role, broker_count=broker_count, config=config
        )
        addons = AddonsConstruct(self, "Addons", cluster=eks_construct.cluster, config=config)
        addons.node.add_dependency(eks_construct)
        kafka = KafkaConstruct(
            self,
            "Kafka",
            cluster=eks_construct.cluster,
            broker_count=broker_count,
            nlb_dns_name=network.kafka_nlb.load_balancer_dns_name,
            kafka_target_groups=network.kafka_target_groups,
            nlb_ports=nlb_ports,
            nlb_sg_id=network.kafka_nlb_sg.security_group_id,
            external_listener_name=external_listener_name,
            aws_lbc_chart=addons.aws_lbc_chart,
        )
        kafka.node.add_dependency(addons)
        # MonitoringConstruct は Strimzi PodMonitor を strimzi-system / kafka namespace に
        # 配置するため、Strimzi chart（strimzi-system namespace を作る）と
        # kafka namespace 作成 manifest 後に deploy する必要がある。
        MonitoringConstruct(
            self,
            "Monitoring",
            cluster=eks_construct.cluster,
            config=config,
            strimzi_chart=addons.strimzi_chart,
            kafka_namespace=kafka.kafka_namespace,
        )

        CfnOutput(self, "KafkaNlbDnsName", value=network.kafka_nlb.load_balancer_dns_name)
