from typing import cast

from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_eks_v2 as eks
from aws_cdk import aws_iam as iam
from aws_cdk.aws_eks_v2 import DefaultCapacityType
from aws_cdk.lambda_layer_kubectl_v35 import KubectlV35Layer
from constructs import Construct

from ekscdk.config import ClusterConfig


class EksClusterConstruct(Construct):
    def __init__(
        self, scope: Construct, construct_id: str, vpc: ec2.IVpc, admin_role: iam.IRole, broker_count: int, config: ClusterConfig
    ) -> None:
        super().__init__(scope, construct_id)

        self._cluster = eks.Cluster(
            self,
            "Cluster",
            cluster_name=config.cluster_name,
            vpc=vpc,
            vpc_subnets=[
                ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS)
            ],
            version=eks.KubernetesVersion.V1_35,
            default_capacity=0,
            default_capacity_type=DefaultCapacityType.NODEGROUP,
            endpoint_access=eks.EndpointAccess.PUBLIC_AND_PRIVATE,
            bootstrap_cluster_creator_admin_permissions=True,
            kubectl_provider_options=eks.KubectlProviderOptions(
                kubectl_layer=KubectlV35Layer(self, "KubectlLayer"),
            ),
        )

        _cluster_admin_policy = [
            eks.AccessPolicy.from_access_policy_name(
                "AmazonEKSClusterAdminPolicy",
                access_scope_type=eks.AccessScopeType.CLUSTER,
            )
        ]

        eks.AccessEntry(
            self,
            "AdminAccessEntry",
            cluster=self._cluster,  # type: ignore[arg-type]
            principal=admin_role.role_arn,
            access_policies=_cluster_admin_policy,
        )

        # -c console-role-arns=arn1,arn2 で追加の管理者ロール（SSO等）を登録する
        console_role_arns: str = self.node.try_get_context("console-role-arns") or ""
        for i, arn in enumerate(filter(None, console_role_arns.split(","))):
            eks.AccessEntry(
                self,
                f"ConsoleAccessEntry{i}",
                cluster=self._cluster,  # type: ignore[arg-type]
                principal=arn.strip(),
                access_policies=_cluster_admin_policy,
            )

        # システムノードグループ: CoreDNS等のクリティカルアドオン専用
        self._cluster.add_nodegroup_capacity(
            "SystemNodeGroup",
            nodegroup_name="system-nodegroup",
            instance_types=[ec2.InstanceType(config.system_instance_type)],
            ami_type=config.nodegroup_ami_type,
            min_size=config.system_min_size,
            max_size=config.system_max_size,
            desired_size=config.system_desired_size,
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
            instance_types=[ec2.InstanceType(config.kafka_instance_type)],
            ami_type=config.nodegroup_ami_type,
            min_size=broker_count,
            max_size=broker_count + 1,  # ローリングアップデート時に新ノードを起動できる余裕を確保
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
