from aws_cdk import CfnOutput, Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from constructs import Construct

from ekscdk.config import ClusterConfig
from ekscdk.constructs._manifest import (
    build_kafka_nlb_ports,
    manifest_dir,
    parse_kafka_external_listener,
)
from ekscdk.constructs.addons import AddonsConstruct
from ekscdk.constructs.eks_cluster import EksClusterConstruct
from ekscdk.constructs.kafka import KafkaConstruct
from ekscdk.constructs.monitoring import MonitoringConstruct
from ekscdk.constructs.network import NetworkConstruct


class EksCdkStack(Stack):
    """インフラスタック（VPC / EKS / アドオン / Kafka）"""

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        admin_role: iam.IRole,
        config: ClusterConfig,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        kafka_dir = manifest_dir("kafka")
        broker_count = config.broker_count
        nlb_ports = build_kafka_nlb_ports(kafka_dir, broker_count=broker_count)
        external_listener_name, _ = parse_kafka_external_listener(kafka_dir)

        network = NetworkConstruct(self, "Network", nlb_ports=nlb_ports, config=config)
        self._vpc = network.vpc
        eks_construct = EksClusterConstruct(
            self,
            "EksCluster",
            vpc=network.vpc,
            admin_role=admin_role,
            broker_count=broker_count,
            config=config,
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
            delete_claim=config.delete_claim,
            controller_count=config.kafka_controller_count,
        )
        kafka.node.add_dependency(addons)
        MonitoringConstruct(
            self,
            "Monitoring",
            cluster=eks_construct.cluster,
            config=config,
            addons=addons,
            kafka_namespace=kafka.kafka_namespace,
        )

        CfnOutput(self, "KafkaNlbDnsName", value=network.kafka_nlb.load_balancer_dns_name)

    @property
    def vpc(self) -> ec2.IVpc:
        return self._vpc
