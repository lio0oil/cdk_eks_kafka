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
            # vpc-cni/coredns/kube-proxy を self-managed 版の自動 bootstrap に任せず
            # Managed Addon として明示管理する（バージョン更新をアドオン単位で制御するため）。
            # 自動 bootstrap と違い作成順序を自前で担保する必要があり、その順序は NodeGroup
            # との前後関係で決まるため、Managed Addon は本 Construct に集約している。
            bootstrap_self_managed_addons=False,
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

        # vpc-cni/kube-proxy は Node が kubelet 登録後に Ready になるための前提コンポーネント。
        # bootstrap_self_managed_addons=False のため self-managed 版は入らず、CNI が無いと
        # ノードが NotReady のままとなり NodeGroup 作成（CfnNodegroup の CREATE）自体が
        # タイムアウトする。そのため NodeGroup 作成より前に Managed Addon として導入し、
        # 各 add_nodegroup_capacity の戻り値に add_dependency で順序を明示する。
        # coredns は逆に Pod をスケジュールするノードが無いと Addon 作成がタイムアウトする
        # ため、NodeGroup 作成後に導入する（本コンストラクタ末尾）。
        vpc_cni_addon = eks.Addon(
            self,
            "VpcCni",
            cluster=self._cluster,  # type: ignore[arg-type]
            addon_name="vpc-cni",
            addon_version=config.addon_versions["vpc-cni"],
        )
        kube_proxy_addon = eks.Addon(
            self,
            "KubeProxy",
            cluster=self._cluster,  # type: ignore[arg-type]
            addon_name="kube-proxy",
            addon_version=config.addon_versions["kube-proxy"],
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
        system_nodegroup = self._cluster.add_nodegroup_capacity(
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
        system_nodegroup.node.add_dependency(vpc_cni_addon, kube_proxy_addon)

        # Kafka 用ノードグループは broker と controller で分離する。
        # 役割ごとに最適なインスタンスサイズが異なる（broker は memory-optimized、
        # controller はメタデータ管理のみで軽量）ため別 nodegroup にして、
        # node-pool-*.yaml の nodeAffinity (role=kafka-broker / kafka-controller)
        # で物理的に配置を分離する。これにより 1 ノード障害で broker と controller を
        # 同時に失うリスクも回避できる。
        # 各 nodegroup は max=desired+1 でローリング時の新ノード起動余裕を確保する。
        kafka_broker_nodegroup = self._cluster.add_nodegroup_capacity(
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
        kafka_broker_nodegroup.node.add_dependency(vpc_cni_addon, kube_proxy_addon)

        controller_count = config.kafka_controller_count
        kafka_controller_nodegroup = self._cluster.add_nodegroup_capacity(
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
        kafka_controller_nodegroup.node.add_dependency(vpc_cni_addon, kube_proxy_addon)

        # ここから下は Pod をスケジュールするノードを要する Addon（Deployment / DaemonSet を持つ）。
        # ノードが無い状態で作ると ACTIVE に到達できず CREATE がタイムアウトするため、
        # いずれも全 NodeGroup の作成完了を待たせる。
        nodegroups = (system_nodegroup, kafka_broker_nodegroup, kafka_controller_nodegroup)

        # eks-pod-identity-agent は add_service_account(POD_IDENTITY) が初回参照時に遅延生成する
        # Addon。DaemonSet のためノード 0 台でも ACTIVE になってしまい「Addon が存在する」以上の
        # 保証が得られないので、ここで先に参照して NodeGroup を待たせ、ACTIVE 到達が実際の
        # Pod 起動を伴うようにする（Pod Identity でクレデンシャルを得るワークロードの前提）。
        cast(eks.IAddon, self._cluster.eks_pod_identity_agent).node.add_dependency(*nodegroups)

        # coredns は DNS の提供元。Pod は外部ドメイン（AWS API エンドポイント等）の名前解決も
        # CoreDNS 経由で行うため、DNS を要するワークロード側から coredns_addon に依存させる。
        self._coredns_addon = eks.Addon(
            self,
            "CoreDns",
            cluster=self._cluster,  # type: ignore[arg-type]
            addon_name="coredns",
            addon_version=config.addon_versions["coredns"],
        )
        self._coredns_addon.node.add_dependency(*nodegroups)

        # aws-ebs-csi-driver addon は ebs-csi-controller-sa という ServiceAccount を
        # 自身で作成するため、CDK 側では add_service_account で SA を作らない
        # （事前に同名 SA を作ると addon 作成時に衝突するため）。
        # ただし PodIdentityAssociations の RoleArn は EKS/addon 側が自動生成できない
        # （どのポリシーを付けるかはワークロード固有の権限設計でユーザー側が決める事項のため）。
        # そのため IAM Role の作成だけは CDK 側に残す必要がある。
        ebs_csi_role = iam.Role(
            self,
            "EbsCsiPodIdentityRole",
            role_name=f"ebs-csi-pod-identity-{config.cluster_name}",
            assumed_by=iam.ServicePrincipal("pods.eks.amazonaws.com").with_session_tags(),  # type: ignore[arg-type]
        )
        ebs_csi_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("service-role/AmazonEBSCSIDriverPolicy")
        )

        # aws_eks_v2.Addon（L2）は PodIdentityAssociations を公開していないため、
        # aws-ebs-csi-driver だけは L1 の CfnAddon を直接使う。
        # SA を自身で作らない他の addon と違い競合する事前作成 SA も無いため、
        # ResolveConflicts のエスケープハッチは不要。
        ebs_csi_addon = eks_l1.CfnAddon(
            self,
            "EbsCsiDriver",
            addon_name="aws-ebs-csi-driver",
            cluster_name=self._cluster.cluster_name,
            addon_version=config.addon_versions["aws-ebs-csi-driver"],
            pod_identity_associations=[
                eks_l1.CfnAddon.PodIdentityAssociationProperty(
                    role_arn=ebs_csi_role.role_arn,
                    service_account="ebs-csi-controller-sa",
                )
            ],
        )
        ebs_csi_addon.node.add_dependency(*nodegroups)

        # metrics-server / eks-node-monitoring-agent は SA を事前作成しないため衝突要因が無く、
        # ResolveConflicts のエスケープハッチも不要。L2 の Addon で十分。
        metrics_server_addon = eks.Addon(
            self,
            "MetricsServer",
            cluster=self._cluster,  # type: ignore[arg-type]
            addon_name="metrics-server",
            addon_version=config.addon_versions["metrics-server"],
        )
        metrics_server_addon.node.add_dependency(*nodegroups)

        node_monitoring_addon = eks.Addon(
            self,
            "NodeMonitoringAgent",
            cluster=self._cluster,  # type: ignore[arg-type]
            addon_name="eks-node-monitoring-agent",
            addon_version=config.addon_versions["eks-node-monitoring-agent"],
            # NMA は --verbosity フラグを zap level に -1 倍して渡すため、
            # WARN 以上 (zapcore.WarnLevel = 1) にするには --verbosity=-1。
            # additionalArgs は完全置換なので chart デフォルトの --metrics-address も含める。
            configuration_values={
                "nodeAgent": {
                    "additionalArgs": [
                        "--metrics-address=:8003",
                        "--verbosity=-1",
                    ],
                },
            },
        )
        node_monitoring_addon.node.add_dependency(*nodegroups)

        # snapshot-controller は aws-ebs-csi-driver とは別の EKS Managed Addon。
        # VolumeSnapshot 系 CRD + controller 本体をまとめて管理してくれるため、
        # CRD だけを self-managed で apply する運用は行わない。
        # SA を事前作成しないため衝突要因が無く、ResolveConflicts のエスケープハッチも不要。
        snapshot_controller_addon = eks.Addon(
            self,
            "SnapshotController",
            cluster=self._cluster,  # type: ignore[arg-type]
            addon_name="snapshot-controller",
            addon_version=config.addon_versions["snapshot-controller"],
        )
        snapshot_controller_addon.node.add_dependency(*nodegroups)

    @property
    def cluster(self) -> eks.ICluster:
        return cast(eks.ICluster, self._cluster)

    @property
    def coredns_addon(self) -> eks.IAddon:
        """coredns Managed Addon。

        Pod は外部ドメイン（AWS API エンドポイント等）の名前解決も CoreDNS 経由で行うため、
        起動時に名前解決を要するワークロードはこの Addon に add_dependency() する。
        """
        return self._coredns_addon
