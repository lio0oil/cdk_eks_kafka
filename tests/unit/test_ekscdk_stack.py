import aws_cdk as core
import aws_cdk.assertions as assertions
from aws_cdk import aws_iam as iam
import pytest

from ekscdk.iam_stack import IamStack
from ekscdk.ekscdk_stack import EksCdkStack
from ekscdk.constructs._manifest import manifest_dir, parse_kafka_nlb_ports


@pytest.fixture(scope="module")
def template():
    app = core.App()
    env = core.Environment(account="123456789012", region="ap-northeast-1")
    iam_stack = IamStack(app, "IamStack", admin_principal=iam.AccountRootPrincipal(), env=env)
    stack = EksCdkStack(app, "ekscdk", admin_role=iam_stack.eks_admin_role, env=env)
    return assertions.Template.from_stack(stack)


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
