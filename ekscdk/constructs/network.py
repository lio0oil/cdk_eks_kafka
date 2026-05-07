from aws_cdk import Tags
from aws_cdk import aws_ec2 as ec2
from constructs import Construct


class NetworkConstruct(Construct):
    def __init__(self, scope: Construct, construct_id: str) -> None:
        super().__init__(scope, construct_id)

        self.vpc = ec2.Vpc(
            self,
            "Vpc",
            ip_addresses=ec2.IpAddresses.cidr("10.0.0.0/16"),
            max_azs=3,
            nat_gateways=3,
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
        for subnet in self.vpc.public_subnets:
            Tags.of(subnet).add("kubernetes.io/role/elb", "1")

        for subnet in self.vpc.private_subnets:
            Tags.of(subnet).add("kubernetes.io/role/internal-elb", "1")
