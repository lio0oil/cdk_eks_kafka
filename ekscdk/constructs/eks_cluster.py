from typing import cast

from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_eks_v2 as eks
from aws_cdk import aws_iam as iam
from aws_cdk.aws_eks_v2 import DefaultCapacityType
from aws_cdk.lambda_layer_kubectl_v35 import KubectlV35Layer
from constructs import Construct


class EksClusterConstruct(Construct):
    def __init__(
        self, scope: Construct, construct_id: str, vpc: ec2.IVpc, admin_role: iam.IRole, broker_count: int = 3
    ) -> None:
        super().__init__(scope, construct_id)

        self._cluster = eks.Cluster(
            self,
            "Cluster",
            cluster_name="eks-cluster",
            vpc=vpc,
            vpc_subnets=[
                ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS)
            ],
            version=eks.KubernetesVersion.V1_35,
            default_capacity=0,
            default_capacity_type=DefaultCapacityType.NODEGROUP,
            endpoint_access=eks.EndpointAccess.PUBLIC_AND_PRIVATE,
            masters_role=admin_role,  # type: ignore[arg-type]
            kubectl_provider_options=eks.KubectlProviderOptions(
                kubectl_layer=KubectlV35Layer(self, "KubectlLayer"),
            ),
        )

        # システムノードグループ: CoreDNS等のクリティカルアドオン専用
        self._cluster.add_nodegroup_capacity(
            "SystemNodeGroup",
            nodegroup_name="system-nodegroup",
            instance_types=[ec2.InstanceType("m8g.large")],
            min_size=3,
            max_size=6,
            desired_size=3,
            capacity_type=eks.CapacityType.ON_DEMAND,
            subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            labels={"role": "system"},
            taints=[
                eks.TaintSpec(
                    key="CriticalAddonsOnly",
                    value="true",
                    effect=eks.TaintEffect.NO_SCHEDULE,
                )
            ],
            enable_node_auto_repair=True,
        )

        # KafkaノードグループはStrimziが管理するKafkaブローカー専用
        self._cluster.add_nodegroup_capacity(
            "KafkaNodeGroup",
            nodegroup_name="kafka-nodegroup",
            instance_types=[ec2.InstanceType("r8g.large")],
            min_size=broker_count,
            max_size=broker_count,
            desired_size=broker_count,
            capacity_type=eks.CapacityType.ON_DEMAND,
            subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            labels={"role": "kafka"},
            taints=[
                eks.TaintSpec(
                    key="DedicatedKafka",
                    value="true",
                    effect=eks.TaintEffect.NO_SCHEDULE,
                )
            ],
            enable_node_auto_repair=True,
        )

    @property
    def cluster(self) -> eks.ICluster:
        return cast(eks.ICluster, self._cluster)
