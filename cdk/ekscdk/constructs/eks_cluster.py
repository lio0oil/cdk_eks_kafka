from typing import cast

from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_eks as eks_l1
from aws_cdk import aws_eks_v2 as eks
from aws_cdk import aws_iam as iam
from aws_cdk.aws_eks_v2 import DefaultCapacityType
from aws_cdk.lambda_layer_kubectl_v35 import KubectlV35Layer
from constructs import Construct

from ekscdk.config import ClusterConfig


class EksClusterConstruct(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: ec2.IVpc,
        admin_role: iam.IRole,
        broker_count: int,
        config: ClusterConfig,
    ) -> None:
        super().__init__(scope, construct_id)

        self._cluster = eks.Cluster(
            self,
            "Cluster",
            cluster_name=config.cluster_name,
            vpc=vpc,
            vpc_subnets=[ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS)],
            version=eks.KubernetesVersion.V1_35,
            default_capacity=0,
            default_capacity_type=DefaultCapacityType.NODEGROUP,
            endpoint_access=eks.EndpointAccess.PUBLIC_AND_PRIVATE,
            bootstrap_cluster_creator_admin_permissions=True,
            kubectl_provider_options=eks.KubectlProviderOptions(
                kubectl_layer=KubectlV35Layer(self, "KubectlLayer"),
            ),
        )

        # aws_eks_v2.Cluster は UpgradePolicy / DeletionProtection を直接プロパティ化
        # していないため、CfnCluster にエスケープハッチで設定する。
        # - UpgradePolicy.SupportType = STANDARD: K8s バージョンサポートを Extended（追加課金）
        #   ではなく Standard（無償・約 14 ヶ月）に固定する
        # - DeletionProtection = config.deletion_protection: 誤削除防止
        #   （dev=False, stg/prd=True）
        # aws_eks_v2.Cluster の node.default_child は L1 の aws_eks.CfnCluster。
        # CDK v2 では L2 の高レベル Construct が aws_eks_v2 / aws_eks 両方にあるが、
        # CFN リソース直接の Cfn* 系は aws_eks（L1）側にしか存在しないため別名 import する。
        cfn_cluster = cast(eks_l1.CfnCluster, self._cluster.node.default_child)
        cfn_cluster.add_property_override("UpgradePolicy.SupportType", "STANDARD")
        cfn_cluster.add_property_override("DeletionProtection", config.deletion_protection)
        # Control Plane Logs を CloudWatch Logs に送る。
        # 有効化する場合は terraform-aws-modules/eks のデフォルト
        # ["audit", "api", "authenticator"] と揃える。controllerManager / scheduler は
        # 採用しない（リファレンス側も未有効化、コスト対監査価値が低い）。
        # 3 種類の粒度を分ける運用価値が薄いため config.enable_control_plane_logs で
        # まとめて on/off する（dev=False、stg/prd=True）。
        enabled_log_types: list[dict[str, str]] = (
            [{"Type": "audit"}, {"Type": "api"}, {"Type": "authenticator"}] if config.enable_control_plane_logs else []
        )
        cfn_cluster.add_property_override(
            "Logging.ClusterLogging.EnabledTypes",
            enabled_log_types,
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

        # kafka_single_az=True の場合は VPC の 1 AZ 目だけに固定する（dev のコスト最適化。
        # AZ 跨ぎのデータ転送料と broker 間レプリケーションの AZ 間トラフィックを避ける）。
        # system-nodegroup（Prometheus 等の監視 scrape 対象含む）も同じ 1 AZ に揃える:
        # AZ 障害時の自動フェイルオーバーを用意していないため、監視だけ multi-AZ に残しても
        # 得られるのは「通知が届くタイミングが早まる」程度で対応不能な点は変わらず、
        # AZ 間 scrape トラフィックのコストに見合わない。
        # EKS クラスター自体は control plane ENI 用に multi-AZ subnet が必須なため
        # NetworkConstruct の VPC は変えず、各 nodegroup の配置先だけ絞る。
        nodegroup_subnets = ec2.SubnetSelection(
            subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
            availability_zones=[vpc.availability_zones[0]] if config.kafka_single_az else None,
        )

        # システムノードグループ: 監視 / Operator / アドオン用
        # taint は打たない（kafka 用ノードを DedicatedKafka taint で隔離する設計のため、
        # system 側を taint で守る必要がない。toleration 未指定の Pod は自然に system に
        # schedule される）
        self._cluster.add_nodegroup_capacity(
            "SystemNodeGroup",
            nodegroup_name="system-nodegroup",
            instance_types=[ec2.InstanceType(config.system_instance_type)],
            ami_type=config.nodegroup_ami_type,
            min_size=config.system_min_size,
            max_size=config.system_max_size,
            desired_size=config.system_desired_size,
            capacity_type=eks.CapacityType.ON_DEMAND,
            subnets=nodegroup_subnets,
            labels={"role": "system"},
            enable_node_auto_repair=True,
        )

        # Kafka 用ノードグループは broker と controller で分離する。
        # 役割ごとに最適なインスタンスサイズが異なる（broker は memory-optimized、
        # controller はメタデータ管理のみで軽量）ため別 nodegroup にして、
        # node-pool-*.yaml の nodeAffinity (role=kafka-broker / kafka-controller)
        # で物理的に配置を分離する。これにより 1 ノード障害で broker と controller を
        # 同時に失うリスクも回避できる。
        # 各 nodegroup は max=desired+1 でローリング時の新ノード起動余裕を確保する。
        self._cluster.add_nodegroup_capacity(
            "KafkaBrokerNodeGroup",
            nodegroup_name="kafka-broker-nodegroup",
            instance_types=[ec2.InstanceType(config.kafka_broker_instance_type)],
            ami_type=config.nodegroup_ami_type,
            min_size=broker_count,
            max_size=broker_count + 1,
            desired_size=broker_count,
            capacity_type=eks.CapacityType.ON_DEMAND,
            subnets=nodegroup_subnets,
            labels={"role": "kafka-broker"},
            taints=[
                eks.TaintSpec(
                    key="DedicatedKafka",
                    value="true",
                    effect=eks.TaintEffect.NO_SCHEDULE,
                )
            ],
            enable_node_auto_repair=True,
        )

        controller_count = config.kafka_controller_count
        self._cluster.add_nodegroup_capacity(
            "KafkaControllerNodeGroup",
            nodegroup_name="kafka-controller-nodegroup",
            instance_types=[ec2.InstanceType(config.kafka_controller_instance_type)],
            ami_type=config.nodegroup_ami_type,
            min_size=controller_count,
            max_size=controller_count + 1,
            desired_size=controller_count,
            capacity_type=eks.CapacityType.ON_DEMAND,
            subnets=nodegroup_subnets,
            labels={"role": "kafka-controller"},
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
