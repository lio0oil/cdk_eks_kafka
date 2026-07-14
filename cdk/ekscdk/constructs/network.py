from aws_cdk import Duration, Tags
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_elasticloadbalancingv2 as elbv2
from aws_cdk import aws_route53 as route53
from aws_cdk import aws_route53_targets as route53_targets
from aws_cdk import aws_s3 as s3
from constructs import Construct

from ekscdk.config import ClusterConfig

# PrivateLink 消費側が自分の VPC に同名の Private Hosted Zone を作れば、advertisedHost の
# 再接続先も同じ文字列で解決できる（提供側 VPC 内では NLB へ、消費側 VPC 内では消費側の
# VPC Endpoint へ、と文脈依存で解決先が変わる）。ドメイン所有権検証が要る VPC Endpoint
# Service の Private DNS 名機能は使わず、検証不要な Private Hosted Zone で完結させる。
KAFKA_PRIVATE_DNS_NAME = "kafka.local"


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

        # ── VPC Flow Logs ─────────────────────────────────────────────────────
        # stg/prd で有効化（dev はコスト削減のため無効）。送り先は S3。
        # CloudWatch Logs に比べて保存コストが約 20 倍安く、Athena でクエリ可能。
        # 監査・コンプライアンス・インシデント調査のいずれにも対応する。
        if config.enable_vpc_flow_logs:
            flow_log_bucket = s3.Bucket(
                self,
                "VpcFlowLogBucket",
                encryption=s3.BucketEncryption.S3_MANAGED,
                block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
                enforce_ssl=True,
                removal_policy=config.log_removal_policy,
            )
            self._vpc.add_flow_log(
                "FlowLog",
                destination=ec2.FlowLogDestination.to_s3(flow_log_bucket),
                traffic_type=ec2.FlowLogTrafficType.ALL,
            )

        # ── VPC エンドポイント ─────────────────────────────────────────────────
        # S3: Gateway 型（無料）。ECR イメージレイヤーはS3経由のため必須。
        self._vpc.add_gateway_endpoint(
            "S3Endpoint",
            service=ec2.GatewayVpcEndpointAwsService.S3,
        )
        # Interface 型 VPC Endpoint（PrivateLink）。機微データ / クレデンシャルを運ぶ通信を
        # VPC 外（NAT -> IGW -> インターネット）に出さないための private 経路。NAT は残すが、
        # 機微データを運ぶ通信だけ PrivateLink に固定する（defense in depth）。制御メタデータしか
        # 流れない API（elb / ec2 / sns 等）は機微データを運ばないため NAT 経由のままにし、
        # 固定費を払ってまで private 化しない。dev は監査要件が薄く固定費が見合わないため無効化する。
        # 対象（いずれも機微データ or クレデンシャルを運ぶ）:
        # - ECR (api/dkr): イメージ内容と pull 認証トークン
        #   （レイヤー実体は S3 Gateway Endpoint 経由なので既に private）
        # - CloudWatch Logs: Fluent Bit が送るコンテナログ本文（アプリログは機微が混入しうる）
        # - eks-auth: EKS Pod Identity Agent が受け取る一時 AWS クレデンシャル。Pod Identity は
        #   IRSA と異なり STS ではなく EKS Auth API で credential を発行する。NAT 経由でも到達は
        #   できるが、資格情報をインターネット経路に出さないため private に固定する。
        if config.enable_interface_endpoints:
            private_subnets = ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS)
            for endpoint_id, service in [
                ("EcrApiEndpoint", ec2.InterfaceVpcEndpointAwsService.ECR),
                ("EcrDkrEndpoint", ec2.InterfaceVpcEndpointAwsService.ECR_DOCKER),
                (
                    "CloudWatchLogsEndpoint",
                    ec2.InterfaceVpcEndpointAwsService.CLOUDWATCH_LOGS,
                ),
                ("EksAuthEndpoint", ec2.InterfaceVpcEndpointAwsService("eks-auth")),
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

        # kafka_single_az=True の場合、NLB を Kafka nodegroup と同じ 1 AZ 目に固定する
        # （EksClusterConstruct の kafka_subnets と同じ vpc.availability_zones[0] を使う）。
        # NLB が multi-AZ のまま targets だけ 1AZ に寄ると、他 AZ の NLB ノードから
        # ターゲットへ到達するのに cross-zone load balancing が必須になってしまうため、
        # NLB 自体も 1AZ に揃えることで cross-zone を無効化でき、AZ 跨ぎのデータ転送料を避けられる。
        kafka_nlb_subnets = (
            ec2.SubnetSelection(
                subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS,
                availability_zones=[self._vpc.availability_zones[0]],
            )
            if config.kafka_single_az
            else None
        )
        self._kafka_nlb = elbv2.NetworkLoadBalancer(
            self,
            "KafkaSharedNlb",
            vpc=self._vpc,
            internet_facing=False,
            cross_zone_enabled=not config.kafka_single_az,
            load_balancer_name="kafka-shared-nlb",
            security_groups=[self._kafka_nlb_sg],
            vpc_subnets=kafka_nlb_subnets,
        )

        # ── Kafka Private Hosted Zone（PrivateLink 消費側との advertisedHost 共有用）─────
        self._kafka_hosted_zone = route53.PrivateHostedZone(
            self,
            "KafkaPrivateHostedZone",
            vpc=self._vpc,
            zone_name=KAFKA_PRIVATE_DNS_NAME,
        )
        route53.ARecord(
            self,
            "KafkaNlbAliasRecord",
            zone=self._kafka_hosted_zone,
            target=route53.RecordTarget.from_alias(
                route53_targets.LoadBalancerTarget(self._kafka_nlb)  # type: ignore[arg-type]
            ),
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
        # VpcEndpointService は CFN に名前プロパティを持たないため、コンソール識別用に
        # Name タグを付ける（ServiceName 自体は自動生成のまま変わらない）。
        Tags.of(self.endpoint_service).add("Name", "kafka-endpoint-service")

    @property
    def vpc(self) -> ec2.IVpc:
        return self._vpc

    @property
    def kafka_nlb(self) -> elbv2.INetworkLoadBalancer:
        return self._kafka_nlb

    @property
    def kafka_private_dns_name(self) -> str:
        """Kafka advertisedHost に使うドメイン名。

        PrivateLink 消費側が自分の VPC に同名の Private Hosted Zone を作れば、
        NLB の生 DNS 名を使うより広い経路（提供側 VPC 内 / PrivateLink 消費側）で解決できる。
        """
        return KAFKA_PRIVATE_DNS_NAME

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
