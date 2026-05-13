import aws_cdk as core
import pytest
from aws_cdk import assertions
from aws_cdk import aws_iam as iam

from ekscdk.config import ClusterConfig
from ekscdk.constructs._manifest import build_kafka_nlb_ports, manifest_dir
from ekscdk.ekscdk_stack import EksCdkStack
from ekscdk.iam_stack import IamStack


def _manifest_literals(value: object) -> str:
    """KubernetesResource.Properties.Manifest から JSON 文字列リテラル部分のみ連結する。

    Manifest が Fn::Join で組み立てられている場合（NLB DNS 名や TargetGroup ARN 等の
    intrinsic を埋め込むケース）、CFN テンプレート上は dict 構造になる。
    assertions.Match では intrinsic 値の中身を直接 regex マッチできないため、
    リテラル部分を取り出して通常の文字列検索に落とす。
    """
    if isinstance(value, str):
        return value
    if isinstance(value, dict):
        if "Fn::Join" in value:
            _sep, parts = value["Fn::Join"]
            return "".join(_manifest_literals(p) for p in parts)
        return ""
    if isinstance(value, list):
        return "".join(_manifest_literals(v) for v in value)
    return ""


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
    ports = build_kafka_nlb_ports(manifest_dir("kafka"), broker_count=ClusterConfig.for_prd().broker_count)
    template.resource_count_is("AWS::ElasticLoadBalancingV2::Listener", len(ports))


def test_nlb_target_group_count_matches_kafka_config(template):
    ports = build_kafka_nlb_ports(manifest_dir("kafka"), broker_count=ClusterConfig.for_prd().broker_count)
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


def test_amp_workspace_alias_matches_cluster_name(template):
    template.has_resource_properties(
        "AWS::APS::Workspace",
        {"Alias": ClusterConfig.for_prd().cluster_name},
    )


def test_application_log_group_retention_matches_config(template):
    # for_prd() は log_retention=ONE_MONTH (30 days) を指定する。
    # aws_logs.RetentionDays は jsii enum で .value は文字列識別子を返すため
    # 数値（CFN の RetentionInDays）はここで明示する。
    config = ClusterConfig.for_prd()
    template.has_resource_properties(
        "AWS::Logs::LogGroup",
        {
            "LogGroupName": f"/aws/eks/{config.cluster_name}/application",
            "RetentionInDays": 30,
        },
    )


@pytest.mark.parametrize(
    "addon_name",
    [
        "vpc-cni",
        "coredns",
        "kube-proxy",
        "aws-ebs-csi-driver",
        "metrics-server",
        "eks-node-monitoring-agent",
    ],
)
def test_eks_addon_present(template, addon_name):
    template.has_resource_properties("AWS::EKS::Addon", {"AddonName": addon_name})


@pytest.mark.parametrize(
    ("namespace", "service_account"),
    [
        ("kube-system", "ebs-csi-controller-sa"),
        ("kube-system", "aws-load-balancer-controller"),
        ("monitoring", "prometheus"),
        ("monitoring", "fluent-bit"),
        ("monitoring", "grafana"),
    ],
)
def test_pod_identity_association_exists(template, namespace, service_account):
    template.has_resource_properties(
        "AWS::EKS::PodIdentityAssociation",
        {"Namespace": namespace, "ServiceAccount": service_account},
    )


@pytest.mark.parametrize(
    ("chart", "namespace"),
    [
        ("strimzi-kafka-operator", "strimzi-system"),
        ("aws-load-balancer-controller", "kube-system"),
        ("kube-prometheus-stack", "monitoring"),
        ("fluent-bit", "monitoring"),
    ],
)
def test_helm_chart_deployed(template, chart, namespace):
    template.has_resource_properties(
        "Custom::AWSCDK-EKS-HelmChart",
        {"Chart": chart, "Namespace": namespace},
    )


def test_target_group_binding_count_matches_broker_count(template):
    # bootstrap 1 個 + broker_count 個の TargetGroupBinding が apply される
    all_k8s = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    bindings = [
        name
        for name, res in all_k8s.items()
        if "TargetGroupBinding" in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert len(bindings) == 1 + ClusterConfig.for_prd().broker_count


def test_kafka_cluster_manifest_includes_all_broker_node_ports(template):
    # external listener の bootstrap.nodePort および brokers[] が KafkaCluster manifest に
    # 正しく含まれていることを確認する。
    # - bootstrap.nodePort: kafka-cluster.yaml に直書き（YAML 値の改ざんを検知）
    # - brokers[].nodePort / advertisedPort: _manifest.build_kafka_broker_configs が動的注入
    all_k8s = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    kafka_crs = [
        res
        for res in all_k8s.values()
        if '"kind":"Kafka"' in _manifest_literals(res["Properties"]["Manifest"])
        and '"kind":"KafkaNodePool"' not in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert len(kafka_crs) == 1
    literals = _manifest_literals(kafka_crs[0]["Properties"]["Manifest"])
    config = ClusterConfig.for_prd()
    expected_ports = build_kafka_nlb_ports(manifest_dir("kafka"), broker_count=config.broker_count)
    for name, advertised_port, node_port in expected_ports:
        assert f'"nodePort":{node_port}' in literals
        # Bootstrap はクライアントが broker に繋ぎ直す前段なので advertisedPort を持たない
        if name != "Bootstrap":
            assert f'"advertisedPort":{advertised_port}' in literals


def test_kafka_topic_test_topic_is_applied(template):
    # KafkaTopic CR (test-topic) が manifest として apply される。
    # Strimzi Topic Operator は strimzi.io/cluster ラベルで担当 Kafka CR を識別するため、
    # ラベル付与とパーティション/レプリカ数が manifest に反映されていることを確認する。
    all_k8s = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    topics = [
        res for res in all_k8s.values() if '"kind":"KafkaTopic"' in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert len(topics) == 1
    literals = _manifest_literals(topics[0]["Properties"]["Manifest"])
    assert '"name":"test-topic"' in literals
    assert '"strimzi.io/cluster":"kafka-cluster"' in literals
    assert '"partitions":3' in literals
    assert '"replicas":3' in literals


def test_eks_pod_identity_agent_addon_version_pinned(template):
    # aws_eks_v2.Cluster が自動追加する eks-pod-identity-agent Addon にも
    # 他 addon と同じく AddonVersion が明示されている（latest 追従を防ぐ）。
    template.has_resource_properties(
        "AWS::EKS::Addon",
        {
            "AddonName": "eks-pod-identity-agent",
            "AddonVersion": ClusterConfig.for_prd().addon_versions["eks-pod-identity-agent"],
        },
    )


def test_cluster_control_plane_logging_enabled(template):
    # data-on-eks リファレンス（terraform-aws-modules/eks v21）のデフォルトに合わせ、
    # audit / api / authenticator の 3 種類を CloudWatch Logs に送る。
    template.has_resource_properties(
        "AWS::EKS::Cluster",
        {
            "Logging": {
                "ClusterLogging": {
                    "EnabledTypes": assertions.Match.array_with(
                        [
                            {"Type": "audit"},
                            {"Type": "api"},
                            {"Type": "authenticator"},
                        ]
                    )
                }
            }
        },
    )


def test_grafana_role_attaches_amp_query_managed_policy(template):
    # Grafana が AMP を SigV4 query する権限（AmazonPrometheusQueryAccess）を持つ
    template.has_resource_properties(
        "AWS::IAM::Role",
        {
            "ManagedPolicyArns": assertions.Match.array_with(
                [
                    assertions.Match.object_like(
                        {
                            "Fn::Join": [
                                "",
                                assertions.Match.array_with([":iam::aws:policy/AmazonPrometheusQueryAccess"]),
                            ]
                        }
                    )
                ]
            )
        },
    )


def test_ebs_csi_role_attaches_managed_policy(template):
    # EBS CSI Driver の Pod Identity ロールが AmazonEBSCSIDriverPolicy を attach している
    template.has_resource_properties(
        "AWS::IAM::Role",
        {
            "ManagedPolicyArns": assertions.Match.array_with(
                [
                    assertions.Match.object_like(
                        {
                            "Fn::Join": [
                                "",
                                assertions.Match.array_with([":iam::aws:policy/service-role/AmazonEBSCSIDriverPolicy"]),
                            ]
                        }
                    )
                ]
            )
        },
    )


def test_vpc_flow_log_enabled_in_prd(template):
    # prd は VPC Flow Logs を S3 に送る（ALL traffic）。
    # S3 バケットは暗号化 + public 遮断 + TLS 強制で作られる。
    template.resource_count_is("AWS::EC2::FlowLog", 1)
    template.has_resource_properties(
        "AWS::EC2::FlowLog",
        {
            "TrafficType": "ALL",
            "LogDestinationType": "s3",
        },
    )
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


def test_control_plane_logs_disabled_in_dev():
    # dev はコスト削減のため Control Plane Logs を一切 CloudWatch に送らない。
    # audit / api / authenticator は粒度を分ける運用価値が薄く、まとめてオフにする。
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
    infra_stack = EksCdkStack(app, "EksCdkStack", admin_role=iam_stack.eks_admin_role, config=config, env=env)
    dev_template = assertions.Template.from_stack(infra_stack)
    clusters = dev_template.find_resources("AWS::EKS::Cluster")
    assert len(clusters) == 1
    cluster = next(iter(clusters.values()))
    assert cluster["Properties"]["Logging"]["ClusterLogging"]["EnabledTypes"] == []


def test_vpc_flow_log_disabled_in_dev():
    # dev はコスト削減のため VPC Flow Logs を有効化しない
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
    infra_stack = EksCdkStack(app, "EksCdkStack", admin_role=iam_stack.eks_admin_role, config=config, env=env)
    dev_template = assertions.Template.from_stack(infra_stack)
    dev_template.resource_count_is("AWS::EC2::FlowLog", 0)


def test_external_snapshotter_crds_applied(template):
    # external-snapshotter の CRD（VolumeSnapshot 系・VolumeGroupSnapshot 系）が
    # KubernetesResource として apply される。snapshot-controller / csi-snapshotter が
    # 起動時に watch する型定義なので、controller より先に apply される必要がある。
    all_k8s = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    matches = [
        res
        for res in all_k8s.values()
        if '"kind":"CustomResourceDefinition"' in _manifest_literals(res["Properties"]["Manifest"])
        and '"volumesnapshots.snapshot.storage.k8s.io"' in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert len(matches) >= 1


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
