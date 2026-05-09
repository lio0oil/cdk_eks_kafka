from aws_cdk import Duration, Tags
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_elasticloadbalancingv2 as elbv2
from constructs import Construct

from ekscdk.config import ClusterConfig


class NetworkConstruct(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        nlb_ports: list[tuple[str, int, int]],
        config: ClusterConfig,
    ) -> None:
        super().__init__(scope, construct_id)

        self._vpc = ec2.Vpc(
            self,
            "Vpc",
            ip_addresses=ec2.IpAddresses.cidr("10.0.0.0/16"),
            max_azs=3,
            nat_gateways=config.nat_gateways,
            subnet_configuration=[
                ec2.SubnetConfiguration(
                    name="Public",
                    subnet_type=ec2.SubnetType.PUBLIC,
                    cidr_mask=24,
                ),
                ec2.SubnetConfiguration(
                    name="Private",
                    subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                    cidr_mask=24,
                ),
            ],
            enable_dns_hostnames=True,
            enable_dns_support=True,
        )

        # ── VPC エンドポイント ─────────────────────────────────────────────────
        # S3: Gateway 型（無料）。ECR イメージレイヤーはS3経由のため必須。
        self._vpc.add_gateway_endpoint(
            "S3Endpoint",
            service=ec2.GatewayVpcEndpointAwsService.S3,
        )
        # Interface 型 VPC Endpoint（時間課金 + データ処理課金）。
        # NAT Gateway のデータ処理コストを削減し、通信を AWS 網内に閉じる。
        # dev は固定費が転送量コストを上回るため無効化する。
        # - ECR: コンテナイメージ pull
        # - CloudWatch Logs: Fluent Bit のログ送信
        # - STS: Pod Identity の AssumeRole
        # - aps-workspaces: Prometheus -> AMP の remote_write（メトリクス送信）
        if config.enable_interface_endpoints:
            private_subnets = ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS)
            for endpoint_id, service in [
                ("EcrApiEndpoint",         ec2.InterfaceVpcEndpointAwsService.ECR),
                ("EcrDkrEndpoint",         ec2.InterfaceVpcEndpointAwsService.ECR_DOCKER),
                ("CloudWatchLogsEndpoint", ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS),
                ("StsEndpoint",            ec2.InterfaceVpcEndpointAwsService.STS),
                ("ApsWorkspacesEndpoint",  ec2.InterfaceVpcEndpointAwsService("aps-workspaces")),
            ]:
                self._vpc.add_interface_endpoint(endpoint_id, service=service, subnets=private_subnets)

        # EKS Load Balancer Controller用サブネットタグ
        for subnet in self._vpc.public_subnets:
            Tags.of(subnet).add("kubernetes.io/role/elb", "1")

        for subnet in self._vpc.private_subnets:
            Tags.of(subnet).add("kubernetes.io/role/internal-elb", "1")

        # ── Kafka 共有 NLB ─────────────────────────────────────────────────────
        self._kafka_nlb_sg = ec2.SecurityGroup(self, "KafkaNlbSg", vpc=self._vpc)
        for _, listener_port, _ in nlb_ports:
            self._kafka_nlb_sg.add_ingress_rule(
                ec2.Peer.ipv4(self._vpc.vpc_cidr_block),
                ec2.Port.tcp(listener_port),
            )

        self._kafka_nlb = elbv2.NetworkLoadBalancer(
            self,
            "KafkaSharedNlb",
            vpc=self._vpc,
            internet_facing=False,
            cross_zone_enabled=True,
            load_balancer_name="kafka-shared-nlb",
            security_groups=[self._kafka_nlb_sg],
        )

        # ── NLB TargetGroup + Listener ────────────────────────────────────────
        # リスナーとターゲットグループは nlb_ports（kafka-cluster.yaml 由来）で決定する。
        # TargetType.INSTANCE で作成し、ターゲット登録は AWS Load Balancer Controller の
        # TargetGroupBinding が Service Endpoints と同期して行う（KafkaConstruct で設定）。
        # これによりローリング更新時の Pod 移動にも追従できる。
        self._kafka_target_groups: dict[str, elbv2.NetworkTargetGroup] = {}
        for name, listener_port, node_port in nlb_ports:
            tg = elbv2.NetworkTargetGroup(
                self,
                f"Kafka{name}Tg",
                vpc=self._vpc,
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
                load_balancer=self._kafka_nlb,
                port=listener_port,
                protocol=elbv2.Protocol.TCP,
                default_target_groups=[tg],
            )
            self._kafka_target_groups[name] = tg

        # ── Kafka PrivateLink (Endpoint Service) ─────────────────────────────
        # NLB本体はCDK管理なので、ここで作成してしまえば ARN は不変
        self.endpoint_service = ec2.VpcEndpointService(
            self,
            "KafkaEndpointService",
            vpc_endpoint_service_load_balancers=[self._kafka_nlb],
            acceptance_required=False,
        )

    @property
    def vpc(self) -> ec2.IVpc:
        return self._vpc

    @property
    def kafka_nlb(self) -> elbv2.INetworkLoadBalancer:
        return self._kafka_nlb

    @property
    def kafka_target_groups(self) -> dict[str, elbv2.NetworkTargetGroup]:
        """Kafka NLB の TargetGroup マップ（key: 'Bootstrap' / 'Broker0' 等）。"""
        return self._kafka_target_groups

    @property
    def kafka_nlb_sg(self) -> ec2.ISecurityGroup:
        """Kafka 共有 NLB のセキュリティグループ。

        TargetGroupBinding の networking.ingress.from に指定し、
        AWS LBC がノード SG に NodePort 受け入れルールを自動追加できるようにする。
        """
        return self._kafka_nlb_sg
