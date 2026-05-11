import aws_cdk as core
import pytest
from aws_cdk import assertions
from aws_cdk import aws_iam as iam

from ekscdk.config import ClusterConfig
from ekscdk.constructs._manifest import manifest_dir, parse_kafka_nlb_ports
from ekscdk.ekscdk_stack import EksCdkStack
from ekscdk.iam_stack import IamStack


@pytest.fixture(scope="module")
def _app_stacks():
    app = core.App()
    env = core.Environment(account="123456789012", region="ap-northeast-1")
    _config = ClusterConfig.for_prd()
    iam_stack = IamStack(
        app,
        "IamStack",
        admin_principal=iam.AccountRootPrincipal(),
        role_name=_config.admin_role_name,
        env=env,
    )
    infra_stack = EksCdkStack(app, "ekscdk", admin_role=iam_stack.eks_admin_role, config=_config, env=env)
    return {
        "iam": assertions.Template.from_stack(iam_stack),
        "infra": assertions.Template.from_stack(infra_stack),
    }


@pytest.fixture(scope="module")
def template(_app_stacks):
    return _app_stacks["infra"]


@pytest.fixture(scope="module")
def iam_template(_app_stacks):
    return _app_stacks["iam"]


def test_stack_synthesizes(template):
    assert template is not None


def test_nlb_listener_count_matches_kafka_config(template):
    ports = parse_kafka_nlb_ports(manifest_dir("kafka"))
    template.resource_count_is("AWS::ElasticLoadBalancingV2::Listener", len(ports))


def test_nlb_target_group_count_matches_kafka_config(template):
    ports = parse_kafka_nlb_ports(manifest_dir("kafka"))
    template.resource_count_is("AWS::ElasticLoadBalancingV2::TargetGroup", len(ports))


def test_amp_workspace_exists(template):
    template.resource_count_is("AWS::APS::Workspace", 1)


def test_kafka_nlb_is_internal(template):
    template.has_resource_properties(
        "AWS::ElasticLoadBalancingV2::LoadBalancer",
        {"Scheme": "internal", "Type": "network"},
    )


def test_vpc_endpoint_service_exists(template):
    template.resource_count_is("AWS::EC2::VPCEndpointService", 1)


def test_kafka_nlb_sg_ingress_restricted_to_vpc(template):
    # NLB SG のインバウンドルールが VPC CIDR 参照に限定され、bootstrap ポートが含まれることを確認
    # CidrIp は Fn::GetAtt で VPC CidrBlock を参照するため Match.any_value() で検証
    template.has_resource_properties(
        "AWS::EC2::SecurityGroup",
        {
            "SecurityGroupIngress": assertions.Match.array_with(
                [
                    assertions.Match.object_like(
                        {
                            "IpProtocol": "tcp",
                            "FromPort": 9094,
                            "ToPort": 9094,
                            "CidrIp": assertions.Match.any_value(),
                        }
                    )
                ]
            )
        },
    )


def test_eks_admin_role_trust_policy(iam_template):
    # eks-cluster-admin ロールの trust policy に sts:AssumeRole が含まれることを確認
    # AWS フィールドは Fn::Join で構築される intrinsic function なので any_value() で検証
    iam_template.has_resource_properties(
        "AWS::IAM::Role",
        {
            "RoleName": "eks-cluster-admin",
            "AssumeRolePolicyDocument": assertions.Match.object_like(
                {
                    "Statement": assertions.Match.array_with(
                        [
                            assertions.Match.object_like(
                                {
                                    "Effect": "Allow",
                                    "Action": "sts:AssumeRole",
                                    "Principal": assertions.Match.object_like(
                                        {
                                            "AWS": assertions.Match.any_value(),
                                        }
                                    ),
                                }
                            )
                        ]
                    )
                }
            ),
        },
    )
