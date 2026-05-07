from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_eks_v2 as eks
from aws_cdk import aws_iam as iam
from aws_cdk.aws_eks_v2 import DefaultCapacityType
from aws_cdk.lambda_layer_kubectl_v32 import KubectlV32Layer
from constructs import Construct


class EksClusterConstruct(Construct):
    def __init__(self, scope: Construct, construct_id: str, vpc: ec2.Vpc) -> None:
        super().__init__(scope, construct_id)

        cluster_admin_role = iam.Role(
            self,
            "ClusterAdminRole",
            assumed_by=iam.AccountRootPrincipal(),  # type: ignore[arg-type]
        )

        self.cluster = eks.Cluster(
            self,
            "Cluster",
            cluster_name="eks-cluster",
            vpc=vpc,
            vpc_subnets=[ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS)],
            version=eks.KubernetesVersion.V1_32,
            default_capacity=0,
            default_capacity_type=DefaultCapacityType.NODEGROUP,
            endpoint_access=eks.EndpointAccess.PUBLIC_AND_PRIVATE,
            masters_role=cluster_admin_role,  # type: ignore[arg-type]
            alb_controller=eks.AlbControllerOptions(
                version=eks.AlbControllerVersion.V2_8_2,
            ),
            kubectl_provider_options=eks.KubectlProviderOptions(
                kubectl_layer=KubectlV32Layer(self, "KubectlLayer"),
            ),
        )

        # システムノードグループ: CoreDNS等のクリティカルアドオン専用
        self.cluster.add_nodegroup_capacity(
            "SystemNodeGroup",
            nodegroup_name="system-nodegroup",
            instance_types=[ec2.InstanceType("m5.xlarge")],
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

        # アプリケーションノードグループ: 一般ワークロード用
        self.cluster.add_nodegroup_capacity(
            "AppNodeGroup",
            nodegroup_name="app-nodegroup",
            instance_types=[ec2.InstanceType("m5.xlarge")],
            min_size=3,
            max_size=9,
            desired_size=3,
            capacity_type=eks.CapacityType.ON_DEMAND,
            subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            labels={"role": "app"},
            enable_node_auto_repair=True,
        )

        # KafkaノードグループはStrimziが管理するKafkaブローカー専用
        self.cluster.add_nodegroup_capacity(
            "KafkaNodeGroup",
            nodegroup_name="kafka-nodegroup",
            instance_types=[ec2.InstanceType("r5.xlarge")],
            min_size=3,
            max_size=9,
            desired_size=3,
            capacity_type=eks.CapacityType.ON_DEMAND,
            subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            labels={"role": "kafka"},
            enable_node_auto_repair=True,
        )
