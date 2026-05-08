from aws_cdk import Duration, Tags
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_elasticloadbalancingv2 as elbv2
from constructs import Construct


class NetworkConstruct(Construct):
    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        nlb_ports: list[tuple[str, int, int]],
        nat_gateways: int = 3,
    ) -> None:
        super().__init__(scope, construct_id)

        self._vpc = ec2.Vpc(
            self,
            "Vpc",
            ip_addresses=ec2.IpAddresses.cidr("10.0.0.0/16"),
            max_azs=3,
            nat_gateways=nat_gateways,
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

        # EKS Load Balancer Controller用サブネットタグ
        for subnet in self._vpc.public_subnets:
            Tags.of(subnet).add("kubernetes.io/role/elb", "1")

        for subnet in self._vpc.private_subnets:
            Tags.of(subnet).add("kubernetes.io/role/internal-elb", "1")

        # ── Kafka 共有 NLB ─────────────────────────────────────────────────────
        kafka_nlb_sg = ec2.SecurityGroup(self, "KafkaNlbSg", vpc=self._vpc)
        for _, listener_port, _ in nlb_ports:
            kafka_nlb_sg.add_ingress_rule(
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
            security_groups=[kafka_nlb_sg],
        )

        # ── NLB TargetGroup + Listener ────────────────────────────────────────
        # リスナーとターゲットグループは nlb_ports（kafka-cluster.yaml 由来）で決定する。
        # TargetType.INSTANCE はターゲットを明示登録せず、EKS ノードが NodePort 経由で
        # ヘルスチェックを通過した時点で自動的にトラフィックを受け取る。
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
