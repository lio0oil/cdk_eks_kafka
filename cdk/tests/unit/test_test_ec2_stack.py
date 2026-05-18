import aws_cdk as core
import pytest
from aws_cdk import assertions
from aws_cdk import aws_iam as iam

from ekscdk.config import ClusterConfig
from ekscdk.ekscdk_stack import EksCdkStack
from ekscdk.iam_stack import IamStack
from ekscdk.test_ec2_stack import TestEc2Stack


@pytest.fixture(scope="module")
def template():
    app = core.App()
    env = core.Environment(account="123456789012", region="ap-northeast-1")
    config = ClusterConfig.for_dev()
    iam_stack = IamStack(
        app,
        "IamStack",
        admin_principal=iam.AccountRootPrincipal(),
        role_name=config.admin_role_name,
        env=env,
    )
    infra_stack = EksCdkStack(
        app,
        "EksCdkStack",
        admin_role=iam_stack.eks_admin_role,
        config=config,
        env=env,
    )
    test_ec2_stack = TestEc2Stack(
        app,
        "TestEc2Stack",
        vpc=infra_stack.vpc,
        env=env,
    )
    return assertions.Template.from_stack(test_ec2_stack)


def test_stack_synthesizes(template):
    assert template is not None


def test_single_ec2_instance(template):
    template.resource_count_is("AWS::EC2::Instance", 1)


def test_instance_uses_t4g_small_arm64(template):
    # Graviton (arm64) で運用コスト最小化。EKS ノードも arm64 なので
    # クライアントライブラリ・コンテナイメージのアーキ整合性が取れる。
    template.has_resource_properties(
        "AWS::EC2::Instance",
        {"InstanceType": "t4g.small"},
    )


def test_instance_role_has_ssm_managed_policy(template):
    # Session Manager の前提は AmazonSSMManagedInstanceCore のみ。
    # 追加で S3 / KMS を付ける場合はここで合わせて assert する。
    template.has_resource_properties(
        "AWS::IAM::Role",
        {
            "AssumeRolePolicyDocument": assertions.Match.object_like(
                {
                    "Statement": assertions.Match.array_with(
                        [
                            assertions.Match.object_like(
                                {
                                    "Effect": "Allow",
                                    "Action": "sts:AssumeRole",
                                    "Principal": {"Service": "ec2.amazonaws.com"},
                                }
                            )
                        ]
                    )
                }
            ),
            "ManagedPolicyArns": assertions.Match.array_with(
                [
                    assertions.Match.object_like(
                        {
                            "Fn::Join": [
                                "",
                                assertions.Match.array_with([":iam::aws:policy/AmazonSSMManagedInstanceCore"]),
                            ]
                        }
                    )
                ]
            ),
        },
    )


def test_security_group_has_no_inbound_rules(template):
    # Session Manager は EC2 -> AWS の outbound のみで成立するため、
    # インバウンド許可は一切不要。SSH 22 等を誤って開けないこと。
    sgs = template.find_resources("AWS::EC2::SecurityGroup")
    assert len(sgs) == 1
    sg = next(iter(sgs.values()))
    assert sg["Properties"].get("SecurityGroupIngress", []) == []


def test_no_key_pair_attached(template):
    # SSH 鍵運用は廃止し Session Manager に一本化。KeyName を付けない。
    instances = template.find_resources("AWS::EC2::Instance")
    assert len(instances) == 1
    instance = next(iter(instances.values()))
    assert "KeyName" not in instance["Properties"]


def test_instance_in_private_subnet(template):
    # プライベートサブネット (PRIVATE_WITH_EGRESS) に配置し、外部から直接到達不可にする。
    # クロススタック参照のため SubnetId は Fn::ImportValue になる。
    instances = template.find_resources("AWS::EC2::Instance")
    instance = next(iter(instances.values()))
    subnet_id = instance["Properties"]["SubnetId"]
    assert isinstance(subnet_id, dict)
    assert "Fn::ImportValue" in subnet_id


def test_transfer_bucket_is_secure(template):
    # ローカル <-> EC2 のファイル転送用バケット。
    template.resource_count_is("AWS::S3::Bucket", 1)
    template.has_resource_properties(
        "AWS::S3::Bucket",
        {
            "BucketEncryption": {
                "ServerSideEncryptionConfiguration": [{"ServerSideEncryptionByDefault": {"SSEAlgorithm": "AES256"}}]
            },
            "PublicAccessBlockConfiguration": {
                "BlockPublicAcls": True,
                "BlockPublicPolicy": True,
                "IgnorePublicAcls": True,
                "RestrictPublicBuckets": True,
            },
        },
    )
    # SSL 強制（aws:SecureTransport 条件で Deny）
    template.has_resource_properties(
        "AWS::S3::BucketPolicy",
        {
            "PolicyDocument": assertions.Match.object_like(
                {
                    "Statement": assertions.Match.array_with(
                        [
                            assertions.Match.object_like(
                                {
                                    "Effect": "Deny",
                                    "Condition": {"Bool": {"aws:SecureTransport": "false"}},
                                }
                            )
                        ]
                    )
                }
            )
        },
    )


def test_transfer_bucket_name_exposed_as_output(template):
    # 利用者が aws s3 cp で参照するため、バケット名を Output で公開する。
    outputs = template.find_outputs("TransferBucketName")
    assert len(outputs) == 1


def test_test_ec2_role_can_read_write_transfer_bucket(template):
    # TestEc2Role が転送バケットへ Get/Put/Delete を実行できることを確認。
    # grant_read_write は s3:GetObject* / s3:PutObject* / s3:DeleteObject* のように
    # ワイルドカード付きの action を生成するため、接頭辞マッチで検証する。
    policies = template.find_resources("AWS::IAM::Policy")
    assert len(policies) >= 1
    found = False
    for policy in policies.values():
        actions: set[str] = set()
        for stmt in policy["Properties"]["PolicyDocument"]["Statement"]:
            stmt_actions = stmt.get("Action", [])
            if isinstance(stmt_actions, str):
                actions.add(stmt_actions)
            else:
                actions.update(stmt_actions)
        has_get = any(a.startswith("s3:GetObject") for a in actions)
        has_put = any(a.startswith("s3:PutObject") for a in actions)
        has_delete = any(a.startswith("s3:DeleteObject") for a in actions)
        if has_get and has_put and has_delete:
            found = True
            break
    assert found, "TestEc2Role に s3 read/write 権限が付いていない"


def test_userdata_installs_kubectl_and_helm(template):
    # 接続直後に EKS / Kafka 検証を始められるよう、kubectl と helm を user-data で入れる。
    # intrinsic 参照を含まないので CFN 上は {"Fn::Base64": "<静的文字列>"} 形式になる。
    instances = template.find_resources("AWS::EC2::Instance")
    instance = next(iter(instances.values()))
    user_data = instance["Properties"]["UserData"]
    assert isinstance(user_data, dict)
    assert "Fn::Base64" in user_data
    inner = user_data["Fn::Base64"]
    if isinstance(inner, str):
        literals = inner
    else:
        join = inner["Fn::Join"]
        literals = "".join(part for part in join[1] if isinstance(part, str))
    assert "kubectl" in literals
    assert "helm" in literals
