from typing import cast

from aws_cdk import CfnOutput, Duration, RemovalPolicy, Stack
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_iam as iam
from aws_cdk import aws_s3 as s3
from constructs import Construct


class TestEc2Stack(Stack):
    """Session Manager 経由で EKS / Kafka を検証するためのテスト用 EC2 スタック。

    SSH 鍵運用は廃止し AmazonSSMManagedInstanceCore のみで接続する。
    インバウンドは一切開けず、Session Manager の outbound 経路に依存する。
    """

    # pytest が "Test*" クラスを test class として収集しようとして
    # PytestCollectionWarning を出すのを抑止する（CDK Stack は __init__ を持つため）
    __test__ = False

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: ec2.IVpc,
        **kwargs,
    ) -> None:
        super().__init__(scope, construct_id, **kwargs)

        role = iam.Role(
            self,
            "TestEc2Role",
            assumed_by=cast(iam.IPrincipal, iam.ServicePrincipal("ec2.amazonaws.com")),
            managed_policies=[
                iam.ManagedPolicy.from_aws_managed_policy_name("AmazonSSMManagedInstanceCore"),
            ],
        )

        # ── ローカル <-> EC2 ファイル転送用 S3 バケット ──────────────────────────
        # SSH 不可方針のため scp/rsync は使わず、S3 を中継して双方向転送する。
        # dev 用途に限定するため 14 日で expire、スタック削除時はオブジェクトごと消す。
        transfer_bucket = s3.Bucket(
            self,
            "TransferBucket",
            encryption=s3.BucketEncryption.S3_MANAGED,
            block_public_access=s3.BlockPublicAccess.BLOCK_ALL,
            enforce_ssl=True,
            removal_policy=RemovalPolicy.DESTROY,
            auto_delete_objects=True,
            lifecycle_rules=[s3.LifecycleRule(expiration=Duration.days(14))],
        )
        transfer_bucket.grant_read_write(role)

        security_group = ec2.SecurityGroup(
            self,
            "TestEc2Sg",
            vpc=vpc,
            allow_all_outbound=True,
            description="Test EC2 (Session Manager only, no inbound).",
        )

        user_data = ec2.UserData.for_linux()
        user_data.add_commands(
            "set -eux",
            "dnf install -y jq tar gzip git unzip bash-completion",
            'ARCH="$(uname -m)"',
            'case "$ARCH" in aarch64) K8S_ARCH=arm64 ;; x86_64) K8S_ARCH=amd64 ;; esac',
            # kubectl は EKS と同じ 1.35 系を入れる（minor 1 差までは互換）
            'curl -fsSL -o /usr/local/bin/kubectl "https://dl.k8s.io/release/v1.35.0/bin/linux/${K8S_ARCH}/kubectl"',
            "chmod +x /usr/local/bin/kubectl",
            "curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash",
        )

        instance = ec2.Instance(
            self,
            "TestEc2",
            instance_type=ec2.InstanceType("t4g.small"),
            machine_image=ec2.MachineImage.latest_amazon_linux2023(
                cpu_type=ec2.AmazonLinuxCpuType.ARM_64,
            ),
            vpc=vpc,
            vpc_subnets=ec2.SubnetSelection(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS),
            role=cast(iam.IRole, role),
            security_group=security_group,
            user_data=user_data,
        )

        CfnOutput(self, "TestEc2InstanceId", value=instance.instance_id)
        CfnOutput(
            self,
            "StartSessionCommand",
            value=f"aws ssm start-session --target {instance.instance_id}",
        )
        CfnOutput(self, "TransferBucketName", value=transfer_bucket.bucket_name)
