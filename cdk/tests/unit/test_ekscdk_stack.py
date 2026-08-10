import json

import aws_cdk as core
import pytest
from aws_cdk import assertions
from aws_cdk import aws_iam as iam

from ekscdk.config import ClusterConfig
from ekscdk.constructs._manifest import build_kafka_nlb_ports, manifest_dir, parse_kafka_external_listener
from ekscdk.constructs.addons import AddonsConstruct
from ekscdk.constructs.eks_cluster import EksClusterConstruct
from ekscdk.constructs.network import NetworkConstruct
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


@pytest.fixture(scope="module")
def config():
    return ClusterConfig.for_prd()


@pytest.fixture(scope="module")
def dev_template():
    # env 別の挙動差（SNS log forwarder 等）を検証するため dev config の template を別途合成する。
    app = core.App()
    env = core.Environment(account="123456789012", region="ap-northeast-1")
    _config = ClusterConfig.for_dev()
    iam_stack = IamStack(
        app,
        "IamStackDev",
        admin_principal=iam.AccountRootPrincipal(),
        role_name=_config.admin_role_name,
        env=env,
    )
    infra_stack = EksCdkStack(app, "ekscdkDev", admin_role=iam_stack.eks_admin_role, config=_config, env=env)
    return assertions.Template.from_stack(infra_stack)


@pytest.fixture(scope="module")
def addons_only_template():
    # Network → EksCluster → Addons までの段階的デプロイを模した最小 stack。
    # KafkaConstruct を意図的に含めず、Addons 単体でも synth 可能なことを検証する。
    app = core.App()
    env = core.Environment(account="123456789012", region="ap-northeast-1")
    _config = ClusterConfig.for_prd()
    iam_stack = IamStack(
        app,
        "IamStackAddonsOnly",
        admin_principal=iam.AccountRootPrincipal(),
        role_name=_config.admin_role_name,
        env=env,
    )

    class _AddonsOnlyStack(core.Stack):
        def __init__(self, scope: core.App, construct_id: str, **kwargs: object) -> None:
            super().__init__(scope, construct_id, **kwargs)
            nlb_ports = build_kafka_nlb_ports(manifest_dir("kafka"), broker_count=_config.broker_count)
            _, kafka_target_port = parse_kafka_external_listener(manifest_dir("kafka"))
            network = NetworkConstruct(
                self,
                "Network",
                nlb_ports=nlb_ports,
                kafka_target_port=kafka_target_port,
                config=_config,
            )
            eks_construct = EksClusterConstruct(
                self,
                "EksCluster",
                vpc=network.vpc,
                admin_role=iam_stack.eks_admin_role,
                broker_count=_config.broker_count,
                config=_config,
            )
            AddonsConstruct(self, "Addons", cluster=eks_construct.cluster, config=_config)

    stack = _AddonsOnlyStack(app, "ekscdkAddonsOnly", env=env)
    return assertions.Template.from_stack(stack)


def test_stack_synthesizes(template):
    assert template is not None


def test_nlb_listener_count_matches_kafka_config(template):
    ports = build_kafka_nlb_ports(manifest_dir("kafka"), broker_count=ClusterConfig.for_prd().broker_count)
    template.resource_count_is("AWS::ElasticLoadBalancingV2::Listener", len(ports))


def test_nlb_target_group_count_matches_kafka_config(template):
    ports = build_kafka_nlb_ports(manifest_dir("kafka"), broker_count=ClusterConfig.for_prd().broker_count)
    template.resource_count_is("AWS::ElasticLoadBalancingV2::TargetGroup", len(ports))


def test_amp_resources_removed(template):
    # AMP から self-hosted Prometheus に切り戻したため、AMP 系 CFN リソースは
    # 一切残らないことを invariant として固定する。
    assert template.find_resources("AWS::APS::Workspace") == {}
    assert template.find_resources("AWS::APS::Scraper") == {}
    assert template.find_resources("AWS::APS::RuleGroupsNamespace") == {}


def test_no_custom_imds_launch_template(template):
    # IMDS hop_limit=2 は ADOT（awscontainerinsightreceiver）が Pod から EC2
    # メタデータを取得するために追加された設定だったが、ADOT は kube-prometheus-stack
    # に移行済みで撤去されている。Pod Identity で動く現行コンポーネント
    # （EBS CSI Driver / AWS LBC / Fluent Bit 等）は IMDS に依存しないため、
    # ノードの IAM instance profile への Pod アクセスを遮断するデフォルトの
    # hop_limit=1 に戻し、custom LaunchTemplate は使わないことを invariant として固定する。
    assert template.find_resources("AWS::EC2::LaunchTemplate") == {}


def test_kafka_nlb_is_internal(template):
    template.has_resource_properties(
        "AWS::ElasticLoadBalancingV2::LoadBalancer",
        {"Scheme": "internal", "Type": "network"},
    )


def test_vpc_endpoint_service_exists(template):
    template.resource_count_is("AWS::EC2::VPCEndpointService", 1)


def test_vpc_endpoint_service_has_name_tag(template):
    # VPCEndpointService は CFN に名前プロパティを持たないため、コンソールでの識別用に
    # Name タグを付与する（ServiceName 自体は変わらず自動生成のまま）。
    template.has_resource_properties(
        "AWS::EC2::VPCEndpointService",
        {"Tags": assertions.Match.array_with([{"Key": "Name", "Value": "kafka-endpoint-service"}])},
    )


def test_kafka_private_hosted_zone_associated_with_vpc(template):
    # kafka.local は提供側 VPC と消費側 VPC の双方に Private Hosted Zone を作ることで、
    # 同一ホスト名が PrivateLink 消費側でも解決できるようにする設計（消費側は別リポジトリ管理）。
    zones = template.find_resources("AWS::Route53::HostedZone")
    matched = [z for z in zones.values() if z["Properties"].get("Name") == "kafka.local."]
    assert len(matched) == 1
    vpcs = matched[0]["Properties"].get("VPCs")
    assert vpcs is not None
    assert len(vpcs) == 1


def test_kafka_nlb_alias_record_in_private_hosted_zone(template):
    records = template.find_resources("AWS::Route53::RecordSet")
    matched = [
        r
        for r in records.values()
        if r["Properties"].get("Name") == "kafka.local." and r["Properties"].get("Type") == "A"
    ]
    assert len(matched) == 1
    assert "AliasTarget" in matched[0]["Properties"]


def test_kafka_advertised_host_uses_private_dns_name(template):
    # PrivateLink 消費側が自分の VPC に同名の Private Hosted Zone を作れば advertisedHost の
    # 再接続先も解決できるようにするため、NLB の生 DNS 名ではなくこのドメインを advertisedHost にする。
    all_k8s = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    kafka_crs = [
        res
        for res in all_k8s.values()
        if '"kind":"Kafka"' in _manifest_literals(res["Properties"]["Manifest"])
        and '"kind":"KafkaNodePool"' not in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert len(kafka_crs) == 1
    literals = _manifest_literals(kafka_crs[0]["Properties"]["Manifest"])
    assert '"advertisedHost":"kafka.local"' in literals


def test_kafka_cluster_disables_auto_topic_creation(template):
    # 存在しない Topic への接続で自動作成させず UNKNOWN_TOPIC_OR_PARTITION エラーにするため、
    # auto.create.topics.enable を明示的に false にする。
    all_k8s = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    kafka_crs = [
        res
        for res in all_k8s.values()
        if '"kind":"Kafka"' in _manifest_literals(res["Properties"]["Manifest"])
        and '"kind":"KafkaNodePool"' not in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert len(kafka_crs) == 1
    literals = _manifest_literals(kafka_crs[0]["Properties"]["Manifest"])
    assert '"auto.create.topics.enable":false' in literals


def test_kafka_and_cruise_control_use_separate_jmx_configmaps(template):
    # Strimzi 公式サンプルでも kafka-metrics と cruise-control-metrics は別 ConfigMap
    # （名前・key が異なる）のため、1 つに統合せずそれぞれ apply し、Kafka CR も
    # component ごとに対応する ConfigMap を参照する。
    all_k8s = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")

    kafka_metrics_cms = [
        res
        for res in all_k8s.values()
        if '"kind":"ConfigMap"' in _manifest_literals(res["Properties"]["Manifest"])
        and '"name":"kafka-metrics"' in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert len(kafka_metrics_cms) == 1

    cruise_control_cms = [
        res
        for res in all_k8s.values()
        if '"kind":"ConfigMap"' in _manifest_literals(res["Properties"]["Manifest"])
        and '"name":"cruise-control-metrics"' in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert len(cruise_control_cms) == 1

    kafka_crs = [
        res
        for res in all_k8s.values()
        if '"kind":"Kafka"' in _manifest_literals(res["Properties"]["Manifest"])
        and '"kind":"KafkaNodePool"' not in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert len(kafka_crs) == 1
    literals = _manifest_literals(kafka_crs[0]["Properties"]["Manifest"])
    assert '"name":"kafka-metrics","key":"kafka-metrics-config.yml"' in literals
    assert '"name":"cruise-control-metrics","key":"metrics-config.yml"' in literals


def _interface_endpoint_service_literals(template) -> list[str]:
    # Interface 型 VPC Endpoint の ServiceName をリテラル文字列化して返す。
    # ServiceName は com.amazonaws.<region>.<service> を Fn::Join で組み、region は
    # Ref AWS::Region なので _manifest_literals が空に畳む。suffix(".<service>") で判定する。
    eps = template.find_resources("AWS::EC2::VPCEndpoint")
    return [
        _manifest_literals(res["Properties"]["ServiceName"])
        for res in eps.values()
        if res["Properties"].get("VpcEndpointType") == "Interface"
    ]


def test_interface_endpoints_keep_sensitive_data_private(template):
    # 方針: 機微データ / クレデンシャルを運ぶ通信だけ PrivateLink に固定する（NAT は残す）。
    # ECR(イメージ) / CloudWatch Logs(ログ本文) / eks-auth(一時クレデンシャル) は private。
    # 制御メタデータのみの elb/ec2/sns は機微データを運ばないため NAT 経由のままにし Interface に含めない。
    names = _interface_endpoint_service_literals(template)

    def has(suffix: str) -> bool:
        return any(name.endswith(suffix) for name in names)

    # 機微データ / クレデンシャル経路（private 必須）
    assert has(".eks-auth"), "Pod Identity が受け取る一時クレデンシャルは private 経路に固定する"
    assert has(".ecr.api")
    assert has(".ecr.dkr")
    assert has(".logs")  # Fluent Bit が送るコンテナログ本文

    # 持たないべき
    assert not has(".aps-workspaces"), "AMP 廃止で送信先が無く orphaned"
    assert not has(".sts"), "Pod Identity では未使用（IRSA 前提の誤記の名残）"


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
        "aws-ebs-csi-driver",
        "metrics-server",
        "eks-node-monitoring-agent",
        "snapshot-controller",
    ],
)
def test_eks_addon_present(template, addon_name):
    template.has_resource_properties("AWS::EKS::Addon", {"AddonName": addon_name})


@pytest.mark.parametrize("addon_name", ["vpc-cni", "coredns", "kube-proxy"])
def test_self_managed_networking_addon_not_managed(template, addon_name):
    # vpc-cni/coredns/kube-proxy は bootstrap_self_managed_addons のデフォルト（True）
    # による self-managed 版をそのまま使うため、EKS Managed Addon としては作成しない。
    addons = template.find_resources("AWS::EKS::Addon")
    matched = [res for res in addons.values() if res["Properties"].get("AddonName") == addon_name]
    assert matched == []


@pytest.mark.parametrize(
    ("namespace", "service_account"),
    [
        ("kube-system", "aws-load-balancer-controller"),
        ("monitoring", "fluent-bit"),
        ("monitoring", "grafana"),
        ("monitoring", "alertmanager"),
    ],
)
def test_pod_identity_association_exists(template, namespace, service_account):
    template.has_resource_properties(
        "AWS::EKS::PodIdentityAssociation",
        {"Namespace": namespace, "ServiceAccount": service_account},
    )


def test_ebs_csi_driver_addon_manages_own_pod_identity(template):
    # aws-ebs-csi-driver addon は ebs-csi-controller-sa という ServiceAccount を
    # 自身で作成するため、CDK 側で add_service_account による事前作成はせず、
    # addon の PodIdentityAssociations に IAM Role を渡して addon に管理させる。
    template.has_resource_properties(
        "AWS::EKS::Addon",
        {
            "AddonName": "aws-ebs-csi-driver",
            "PodIdentityAssociations": assertions.Match.array_with(
                [assertions.Match.object_like({"ServiceAccount": "ebs-csi-controller-sa"})]
            ),
        },
    )


def test_aws_lbc_pod_identity_role_has_explicit_name(template, config):
    template.has_resource_properties(
        "AWS::IAM::Role",
        {"RoleName": f"aws-lbc-pod-identity-{config.cluster_name}"},
    )


def test_in_cluster_prometheus_no_pod_identity(template):
    # in-cluster Prometheus は AWS API を叩かないため Pod Identity 不要。
    # chart 同梱の SA をそのまま使い、CDK は prometheus 用 PodIdentityAssociation を作らない。
    associations = template.find_resources("AWS::EKS::PodIdentityAssociation")
    sa_names = [res["Properties"].get("ServiceAccount") for res in associations.values()]
    assert "prometheus" not in sa_names


@pytest.mark.parametrize(
    "pod_monitor_name",
    ["kafka-resources-metrics", "cluster-operator-metrics", "entity-operator-metrics"],
)
def test_pod_monitor_manifest_applied(template, pod_monitor_name):
    # in-cluster Prometheus + Operator が動くため、Strimzi 系の PodMonitor 3 件は manifest
    # として apply され、Operator がこれを scrape config に翻訳する。
    all_k8s = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    matched = [
        res
        for res in all_k8s.values()
        if '"kind":"PodMonitor"' in _manifest_literals(res["Properties"]["Manifest"])
        and f'"name":"{pod_monitor_name}"' in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert len(matched) == 1, f"PodMonitor {pod_monitor_name} が apply されていない"


@pytest.mark.parametrize(
    "dashboard_name",
    [
        "strimzi-kafka-dashboard",
        "strimzi-exporter-dashboard",
        "strimzi-operators-dashboard",
        "strimzi-cruise-control-dashboard",
        "strimzi-kraft-dashboard",
    ],
)
def test_grafana_dashboard_configmap_applied(template, dashboard_name):
    # grafana_dashboard=1 ラベル付き ConfigMap を kube-prometheus-stack の sidecar が
    # 自動取り込みする。Strimzi公式dashboard起点の5件が全て apply されていることを確認する。
    all_k8s = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    matched = [
        res
        for res in all_k8s.values()
        if '"kind":"ConfigMap"' in _manifest_literals(res["Properties"]["Manifest"])
        and f'"name":"{dashboard_name}"' in _manifest_literals(res["Properties"]["Manifest"])
        and '"grafana_dashboard":"1"' in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert len(matched) == 1, f"Dashboard ConfigMap {dashboard_name} が apply されていない"


def test_kube_prometheus_stack_enables_prometheus_and_operator(template, config):
    # chart values で prometheus / Operator を有効化していること（無効化していた頃の
    # 設定を誤って残すと in-cluster Prometheus が立たず Grafana にデータが入らない）。
    charts = template.find_resources("Custom::AWSCDK-EKS-HelmChart")
    kps = [res for res in charts.values() if res["Properties"].get("Chart") == "kube-prometheus-stack"]
    assert len(kps) == 1
    values_literals = _manifest_literals(kps[0]["Properties"]["Values"])
    # 無効化していたときは `enabled: false` を明示していたため、両 enable: false が無いことを assert
    assert '"prometheus":{"enabled":false' not in values_literals
    assert '"prometheusOperator":{"enabled":false' not in values_literals
    # replicas / retention / storage size / scrapeInterval は ClusterConfig が真実の源（環境別に
    # 変更しうる値のため config.py 側の値をそのまま assert し、二重管理を避ける）
    assert f'"replicas":{config.prometheus_replicas}' in values_literals
    assert f'"retention":"{config.prometheus_resources.retention}"' in values_literals
    assert f'"scrapeInterval":"{config.prometheus_resources.scrape_interval}"' in values_literals
    assert f'"storage":"{config.prometheus_resources.storage_size}"' in values_literals
    # AZ 跨ぎの topologySpread が外れると 2 replica が同 AZ に乗りうる
    assert "topology.kubernetes.io/zone" in values_literals


def test_prometheus_resources_come_from_config(template, config):
    # broker_count 増設等でスクレイプ対象が増えた際に resources を見直せるよう、
    # config.prometheus_resources の値がそのまま chart values に反映されていることを検証する。
    charts = template.find_resources("Custom::AWSCDK-EKS-HelmChart")
    kps = [res for res in charts.values() if res["Properties"].get("Chart") == "kube-prometheus-stack"]
    assert len(kps) == 1
    literals = _manifest_literals(kps[0]["Properties"]["Values"])
    resources = config.prometheus_resources

    assert f'"requests":{{"memory":"{resources.memory_request}","cpu":"{resources.cpu_request}"}}' in literals
    assert f'"limits":{{"memory":"{resources.memory_limit}","cpu":"{resources.cpu_limit}"}}' in literals


def test_alertmanager_resources_come_from_config(template, config):
    charts = template.find_resources("Custom::AWSCDK-EKS-HelmChart")
    kps = [res for res in charts.values() if res["Properties"].get("Chart") == "kube-prometheus-stack"]
    assert len(kps) == 1
    literals = _manifest_literals(kps[0]["Properties"]["Values"])
    resources = config.alertmanager_resources

    assert f'"requests":{{"memory":"{resources.memory_request}","cpu":"{resources.cpu_request}"}}' in literals
    assert f'"limits":{{"memory":"{resources.memory_limit}","cpu":"{resources.cpu_limit}"}}' in literals


def test_alertmanager_storage_size_comes_from_config(template, config):
    charts = template.find_resources("Custom::AWSCDK-EKS-HelmChart")
    kps = [res for res in charts.values() if res["Properties"].get("Chart") == "kube-prometheus-stack"]
    assert len(kps) == 1
    literals = _manifest_literals(kps[0]["Properties"]["Values"])
    assert f'"storage":"{config.alertmanager_resources.storage_size}"' in literals


def test_alertmanager_sns_topic_exists(template):
    # 通知配送用 SNS Topic がちょうど 1 つ作られる。subscriber（Email / Chatbot 等）は
    # 手動 / 別 PR で追加する設計のため、CDK 側は Topic だけを管理する。
    template.resource_count_is("AWS::SNS::Topic", 1)


def test_alertmanager_sa_iam_policy_grants_sns_publish(template):
    """Alertmanager の Pod Identity SA に紐づく IAM Policy が sns:Publish のみで、
    Resource が "*" でなく特定 Topic ARN への参照（intrinsic）に絞られていること。

    chart に webhook URL を平文で書かない / Secrets Manager + ESO を介在させない
    設計の根拠が「IAM 最小権限で sns:Publish だけを Topic ARN に絞る」点に依存する。
    広い Resource や追加 Action がリグレッションすると設計前提が崩れる。
    """
    policies = template.find_resources("AWS::IAM::Policy")
    matching: list[dict] = []
    for p in policies.values():
        for stmt in p["Properties"]["PolicyDocument"]["Statement"]:
            action = stmt.get("Action")
            actions = action if isinstance(action, list) else [action]
            if "sns:Publish" in actions:
                matching.append(stmt)
    assert len(matching) == 1, "sns:Publish を持つ Statement がちょうど 1 個ではない"
    stmt = matching[0]
    # 単独アクション、または sns:Publish のみのリスト
    action = stmt.get("Action")
    assert action in ("sns:Publish", ["sns:Publish"])
    # Resource は Topic ARN への intrinsic 参照（Ref / Fn::GetAtt 等の dict）であり "*" でない
    resource = stmt.get("Resource")
    assert resource != "*"
    assert isinstance(resource, (dict, list))


def test_kube_prometheus_stack_enables_alertmanager(template, config):
    """alertmanager が 3 replica HA / AZ 分散 / PDB maxUnavailable: 1 で有効化される。

    enabled: false 時代に書いていた `"alertmanager":{"enabled":false}` リテラルが
    無いことを assert することで「無効化に戻す」リグレッションも同時に検知する。
    """
    charts = template.find_resources("Custom::AWSCDK-EKS-HelmChart")
    kps = [res for res in charts.values() if res["Properties"].get("Chart") == "kube-prometheus-stack"]
    assert len(kps) == 1
    literals = _manifest_literals(kps[0]["Properties"]["Values"])
    assert '"alertmanager":{"enabled":false' not in literals
    assert '"alertmanager":{"enabled":true' in literals
    # gossip cluster の replica 数（標準サイズ）は ClusterConfig が真実の源
    assert f'"replicas":{config.alertmanager_replicas}' in literals
    # AZ 跨ぎの topologySpread が外れると replica が同 AZ に乗りうる
    assert "topology.kubernetes.io/zone" in literals
    # PDB は Prometheus と Alertmanager の両方で enabled（chart デフォルト minAvailable: 1）。
    # 同一リテラルが 2 箇所以上出現することで両方有効を invariant 化する。片方が消えた
    # リグレッションをここで検知する。
    assert literals.count('"podDisruptionBudget":{"enabled":true}') >= 2


def test_dev_kafka_nodegroups_pinned_to_single_az(dev_template):
    # dev はコスト最適化のため Kafka broker/controller/system 全ノードグループを 1AZ に固定する
    # （config.kafka_single_az=True）。AZ 障害時の復旧手段を用意していないため、system だけ
    # multi-AZ を維持しても「アラートだけ生き残る」以上の実効性が無く、AZ 間 scrape/転送料が
    # 無駄になる。
    nodegroups = dev_template.find_resources("AWS::EKS::Nodegroup")
    by_name = {res["Properties"]["NodegroupName"]: res["Properties"]["Subnets"] for res in nodegroups.values()}
    assert len(by_name["kafka-broker-nodegroup"]) == 1
    assert len(by_name["kafka-controller-nodegroup"]) == 1
    assert len(by_name["system-nodegroup"]) == 1


def test_prd_kafka_nodegroups_span_multiple_az(template):
    # stg/prd は HA 優先のため Kafka broker/controller/system 全ノードグループを multi-AZ に
    # 維持する（config.kafka_single_az=False）。
    nodegroups = template.find_resources("AWS::EKS::Nodegroup")
    by_name = {res["Properties"]["NodegroupName"]: res["Properties"]["Subnets"] for res in nodegroups.values()}
    assert len(by_name["kafka-broker-nodegroup"]) == 3
    assert len(by_name["kafka-controller-nodegroup"]) == 3
    assert len(by_name["system-nodegroup"]) == 3


def test_dev_kafka_nlb_pinned_to_single_az_matching_nodegroups(dev_template):
    # dev は Kafka broker/controller ノードグループと同じ 1AZ に NLB を固定し、
    # cross-zone load balancing を無効化する（同一 AZ 内で完結するため不要、データ転送料も削減）。
    lbs = dev_template.find_resources("AWS::ElasticLoadBalancingV2::LoadBalancer")
    nlb = next(iter(lbs.values()))
    assert len(nlb["Properties"]["Subnets"]) == 1

    nodegroups = dev_template.find_resources("AWS::EKS::Nodegroup")
    by_name = {res["Properties"]["NodegroupName"]: res["Properties"]["Subnets"] for res in nodegroups.values()}
    assert nlb["Properties"]["Subnets"] == by_name["kafka-broker-nodegroup"]

    attrs = {a["Key"]: a["Value"] for a in nlb["Properties"]["LoadBalancerAttributes"]}
    assert attrs["load_balancing.cross_zone.enabled"] == "false"


def test_prd_kafka_nlb_spans_multiple_az_with_cross_zone_enabled(template):
    # stg/prd は broker が multi-AZ に分散するため cross-zone load balancing を維持する。
    lbs = template.find_resources("AWS::ElasticLoadBalancingV2::LoadBalancer")
    nlb = next(iter(lbs.values()))
    assert len(nlb["Properties"]["Subnets"]) == 3
    attrs = {a["Key"]: a["Value"] for a in nlb["Properties"]["LoadBalancerAttributes"]}
    assert attrs["load_balancing.cross_zone.enabled"] == "true"


def test_dev_system_nodegroup_uses_larger_instance(dev_template):
    # system は監視 HA（prometheus×2 / alertmanager×3 / grafana / ...）+ Strimzi operator 群
    # + 全ノード共通 DaemonSet で steady ~12-13 pod/node。Cluster Autoscaler / Karpenter が
    # 無く managed nodegroup が固定サイズのため、3 ノード中 1 台喪失で pod が残り 2 ノードに
    # 寄ると max-pods 17 の t4g.medium では収まらない。max-pods 35 の t4g.large で耐える。
    dev_template.has_resource_properties(
        "AWS::EKS::Nodegroup",
        {"NodegroupName": "system-nodegroup", "InstanceTypes": ["t4g.large"]},
    )


def test_kube_prometheus_stack_disables_managed_control_plane_components(template):
    # EKS のコントロールプレーン（kube-controller-manager / kube-scheduler）は AWS マネージドで
    # 外部 scrape 不可。chart デフォルトの enabled: true のままだと到達不能な ServiceMonitor が残り、
    # それに gate された KubeControllerManagerDown / KubeSchedulerDown が常時 firing する。
    # component ごと無効化することで ServiceMonitor とデフォルトアラートルールの両方を消す。
    charts = template.find_resources("Custom::AWSCDK-EKS-HelmChart")
    kps = [res for res in charts.values() if res["Properties"].get("Chart") == "kube-prometheus-stack"]
    assert len(kps) == 1
    literals = _manifest_literals(kps[0]["Properties"]["Values"])
    assert '"kubeControllerManager":{"enabled":false}' in literals
    assert '"kubeScheduler":{"enabled":false}' in literals


def test_alertmanager_sns_log_forwarder_present_in_dev(dev_template):
    # dev は Email/Teams を用意せず通知本文を CloudWatch Logs で確認するため、SNS Topic に
    # Lambda subscriber を付ける（config.enable_alertmanager_sns_log_forwarder=True）。
    dev_template.has_resource_properties("AWS::SNS::Subscription", {"Protocol": "lambda"})


def test_alertmanager_sns_log_forwarder_absent_in_prd(template):
    # prd は実通知先（Email/Teams 等）に配送するため検証用 Lambda subscriber を作らない。
    subscriptions = template.find_resources("AWS::SNS::Subscription")
    lambda_subs = [s for s in subscriptions.values() if s["Properties"].get("Protocol") == "lambda"]
    assert lambda_subs == []


def test_kube_prometheus_stack_alertmanager_uses_sns_receiver(template):
    """alertmanager.config.receivers[] が sns_configs を使い、sigv4 署名で SNS Publish を行う。

    Topic ARN は intrinsic (Token) として埋まるため文字列リテラルでは現れない。
    receiver の設定キー名（sns_configs / sigv4）の存在で間接検証する。webhook_configs や
    slack_configs に差し替わったリグレッションをここで検知する。
    """
    charts = template.find_resources("Custom::AWSCDK-EKS-HelmChart")
    kps = [res for res in charts.values() if res["Properties"].get("Chart") == "kube-prometheus-stack"]
    assert len(kps) == 1
    literals = _manifest_literals(kps[0]["Properties"]["Values"])
    assert "sns_configs" in literals
    assert "sigv4" in literals


@pytest.mark.parametrize(
    "rule_group_name",
    [
        "kafka-rules",
        "kafka-exporter-topic",
        "strimzi-cluster-operator-rules",
        "strimzi-entity-operator",
        "kafka-certificates",
    ],
)
def test_prometheus_rule_manifest_applied(template, rule_group_name):
    """PrometheusRule CR（Strimzi公式 prometheus-rules 起点、稼働コンポーネント分）が apply される。"""
    all_k8s = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    matched = [
        res
        for res in all_k8s.values()
        if '"kind":"PrometheusRule"' in _manifest_literals(res["Properties"]["Manifest"])
        and f'"name":"{rule_group_name}"' in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert len(matched) == 1, f"PrometheusRule {rule_group_name} が apply されていない"


@pytest.mark.parametrize(
    ("kind", "name"),
    [
        ("ServiceAccount", "strimzi-kube-state-metrics"),
        ("ClusterRole", "strimzi-kube-state-metrics"),
        ("ClusterRoleBinding", "strimzi-kube-state-metrics"),
        ("Service", "strimzi-kube-state-metrics"),
        ("Deployment", "strimzi-kube-state-metrics"),
        ("ServiceMonitor", "strimzi-kube-state-metrics"),
    ],
)
def test_strimzi_kube_state_metrics_manifest_applied(template, kind, name):
    # kube-prometheus-stack 同梱の kube-state-metrics は Strimzi CRD を知らないため、
    # Strimzi 公式の専用インスタンス（ServiceAccount/ClusterRole/ClusterRoleBinding/
    # Service/Deployment/ServiceMonitor の 6 リソース）を別途 apply している。
    all_k8s = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    matched = [
        res
        for res in all_k8s.values()
        if f'"kind":"{kind}"' in _manifest_literals(res["Properties"]["Manifest"])
        and f'"name":"{name}"' in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert len(matched) == 1, f"{kind} {name} が apply されていない"


def test_strimzi_kube_state_metrics_cluster_role_is_read_only(template):
    # CRD の .status を読むだけの用途のため、list/watch 以外の verb（write/delete 等）を
    # 持たせない。誤って書き込み権限を付与するリグレッションを検知する。
    all_k8s = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    matched = [
        res
        for res in all_k8s.values()
        if '"kind":"ClusterRole"' in _manifest_literals(res["Properties"]["Manifest"])
        and '"name":"strimzi-kube-state-metrics"' in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert len(matched) == 1
    manifest_str = matched[0]["Properties"]["Manifest"]
    assert isinstance(manifest_str, str)
    docs = json.loads(manifest_str)
    cluster_role = next(
        d for d in docs if d.get("kind") == "ClusterRole" and d["metadata"]["name"] == "strimzi-kube-state-metrics"
    )
    verbs = {v for rule in cluster_role["rules"] for v in rule["verbs"]}
    assert verbs == {"list", "watch"}


def test_strimzi_kube_state_metrics_configmap_applied(template):
    all_k8s = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    matched = [
        res
        for res in all_k8s.values()
        if '"kind":"ConfigMap"' in _manifest_literals(res["Properties"]["Manifest"])
        and '"name":"strimzi-kube-state-metrics-config"' in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert len(matched) == 1, "kube-state-metrics 用 ConfigMap が apply されていない"


def test_strimzi_kube_state_metrics_prometheus_rule_applied(template):
    all_k8s = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    matched = [
        res
        for res in all_k8s.values()
        if '"kind":"PrometheusRule"' in _manifest_literals(res["Properties"]["Manifest"])
        and '"name":"strimzi-kube-state-metrics"' in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert len(matched) == 1
    literals = _manifest_literals(matched[0]["Properties"]["Manifest"])
    assert '"alert":"KafkaTopicNotReady"' in literals
    assert '"alert":"KafkaNotReady"' in literals


def test_kube_prometheus_stack_enables_prometheus_pdb(template):
    """Prometheus は 2 replica HA active-active 構成だが、chart デフォルトは
    prometheus.podDisruptionBudget.enabled: false で PDB が生成されない。
    enabled: true だけ渡せば chart デフォルトの minAvailable: 1 がそのまま乗る。
    """
    charts = template.find_resources("Custom::AWSCDK-EKS-HelmChart")
    kps = [res for res in charts.values() if res["Properties"].get("Chart") == "kube-prometheus-stack"]
    assert len(kps) == 1
    values_literals = _manifest_literals(kps[0]["Properties"]["Values"])
    assert '"podDisruptionBudget":{"enabled":true}' in values_literals


def test_fluent_bit_resources_and_buffer_limit_come_from_config(template, config):
    # ノード当たりのログ流量が変わった場合に見直せるよう、config.fluent_bit_resources /
    # fluent_bit_mem_buf_limit の値がそのまま chart values に反映されていることを検証する。
    charts = template.find_resources("Custom::AWSCDK-EKS-HelmChart")
    fluent_bit = [res for res in charts.values() if res["Properties"].get("Chart") == "fluent-bit"]
    assert len(fluent_bit) == 1
    literals = _manifest_literals(fluent_bit[0]["Properties"]["Values"])
    resources = config.fluent_bit_resources

    assert f'"requests":{{"memory":"{resources.memory_request}","cpu":"{resources.cpu_request}"}}' in literals
    assert f'"limits":{{"memory":"{resources.memory_limit}","cpu":"{resources.cpu_limit}"}}' in literals
    assert f"Mem_Buf_Limit     {config.fluent_bit_resources.mem_buf_limit}" in literals


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


@pytest.mark.parametrize(
    "chart",
    [
        "strimzi-kafka-operator",
        "aws-load-balancer-controller",
    ],
)
def test_helm_chart_has_topology_spread_constraints(template, chart):
    # operator 系 Pod (replicas > 1) を AZ に分散させるための制約。
    # AZ 単一障害で 2 Pod とも消えないよう topology.kubernetes.io/zone を指定する。
    charts = template.find_resources("Custom::AWSCDK-EKS-HelmChart")
    matched = [res for res in charts.values() if res["Properties"].get("Chart") == chart]
    assert len(matched) == 1, f"chart {chart} not found"
    values_literals = _manifest_literals(matched[0]["Properties"]["Values"])
    assert "topologySpreadConstraints" in values_literals
    assert "topology.kubernetes.io/zone" in values_literals


def test_aws_lbc_helm_chart_declares_pdb(template):
    """AWS LBC は replicaCount: 2 だが、chart の podDisruptionBudget デフォルトは空 dict {} で
    templates/pdb.yaml の `if .Values.podDisruptionBudget` が falsy 評価されて PDB が生成されない。
    明示的に値を渡して PDB を生成させていることを invariant 化する。

    AWS LBC が完全停止すると MutatingWebhook が止まり、TargetGroupBinding 等の apply が詰まる。
    Strimzi Operator の minAvailable: 1 と非対称に maxUnavailable: 1 を選ぶ理由は、replicaCount
    を将来増やしたときに「同時に 1 Pod までしか evict 不可」を維持できるため（minAvailable: 1 だと
    replicaCount 増えるほど許容喪失が増えてしまう）。
    """
    charts = template.find_resources("Custom::AWSCDK-EKS-HelmChart")
    lbc = [res for res in charts.values() if res["Properties"].get("Chart") == "aws-load-balancer-controller"]
    assert len(lbc) == 1
    values_literals = _manifest_literals(lbc[0]["Properties"]["Values"])
    assert '"podDisruptionBudget":{"maxUnavailable":1}' in values_literals


def test_strimzi_operator_helm_chart_enables_pdb(template):
    """Strimzi Cluster Operator は replicas: 2 の leader-election 構成だが、chart デフォルトでは
    PodDisruptionBudget が enabled: false になっている。これを明示的に有効化していること。
    enabled: true だけ渡せば chart デフォルトの minAvailable: 1 がそのまま乗る。
    """
    charts = template.find_resources("Custom::AWSCDK-EKS-HelmChart")
    strimzi = [res for res in charts.values() if res["Properties"].get("Chart") == "strimzi-kafka-operator"]
    assert len(strimzi) == 1
    values_literals = _manifest_literals(strimzi[0]["Properties"]["Values"])
    assert '"podDisruptionBudget":{"enabled":true}' in values_literals


def test_strimzi_operator_waits_for_kafka_namespace(template):
    """Strimzi chart は `watchNamespaces: ["kafka"]` を渡しており、対象 NS が事前に
    存在しないと RoleBinding 作成時に `namespaces "kafka" not found` で Helm install
    が失敗する。kafka Namespace の作成完了を明示的な DependsOn で担保する。
    """
    manifests = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    namespace_ids = [
        name
        for name, res in manifests.items()
        if '"kind":"Namespace"' in _manifest_literals(res["Properties"]["Manifest"])
        and '"name":"kafka"' in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert len(namespace_ids) == 1
    namespace_id = namespace_ids[0]

    charts = template.find_resources("Custom::AWSCDK-EKS-HelmChart")
    strimzi = [res for res in charts.values() if res["Properties"].get("Chart") == "strimzi-kafka-operator"]
    assert len(strimzi) == 1
    assert namespace_id in strimzi[0].get("DependsOn", [])


def test_kafka_node_pools_wait_for_strimzi_operator(template):
    """KafkaNodePool CRD は Strimzi Operator chart が導入するため、chart 導入前に
    apply すると CRD 未登録で失敗する。chart の DependsOn を担保する。
    """
    charts = template.find_resources("Custom::AWSCDK-EKS-HelmChart")
    strimzi_ids = [name for name, res in charts.items() if res["Properties"].get("Chart") == "strimzi-kafka-operator"]
    assert len(strimzi_ids) == 1
    strimzi_id = strimzi_ids[0]

    manifests = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    node_pools = [
        res
        for res in manifests.values()
        if '"kind":"KafkaNodePool"' in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert len(node_pools) == 2
    for pool in node_pools:
        assert strimzi_id in pool.get("DependsOn", [])


def test_addons_alone_creates_kafka_namespace_for_strimzi(addons_only_template):
    """段階的デプロイ（Network/EksCluster/Addons のみ、KafkaConstruct 無し）でも、
    Strimzi chart が要求する kafka Namespace が Addons 側で用意されることを固定する。
    KafkaConstruct 側に namespace 作成を残すと、Addons だけを先に導入する段階で
    `namespaces "kafka" not found` により Strimzi の Helm install が失敗する。
    """
    manifests = addons_only_template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    namespace_ids = [
        name
        for name, res in manifests.items()
        if '"kind":"Namespace"' in _manifest_literals(res["Properties"]["Manifest"])
        and '"name":"kafka"' in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert len(namespace_ids) == 1

    charts = addons_only_template.find_resources("Custom::AWSCDK-EKS-HelmChart")
    strimzi = [res for res in charts.values() if res["Properties"].get("Chart") == "strimzi-kafka-operator"]
    assert len(strimzi) == 1
    assert namespace_ids[0] in strimzi[0].get("DependsOn", [])


def test_target_group_binding_count_matches_broker_count(template):
    # bootstrap 1 個 + broker_count 個の TargetGroupBinding が apply される
    all_k8s = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    bindings = [
        name
        for name, res in all_k8s.items()
        if "TargetGroupBinding" in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert len(bindings) == 1 + ClusterConfig.for_prd().broker_count


def test_target_group_binding_service_name_uses_broker_pool_name(template):
    # broker 個別の per-pod Service は KafkaNodePool 名（node-pool-broker.yaml の
    # metadata.name = broker）に依存し、kafka-cluster-broker-<id> を参照する。
    # 一方 bootstrap Service は Strimzi が pool 名と無関係に常に固定文字列 `kafka` で
    # 生成する（実クラスタで kafka-cluster-kafka-external-bootstrap の存在を確認済み）ため、
    # bootstrap 用 TargetGroupBinding だけは kafka-cluster-kafka-* を参照する。
    all_k8s = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    bindings_literals = [
        _manifest_literals(res["Properties"]["Manifest"])
        for res in all_k8s.values()
        if "TargetGroupBinding" in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert bindings_literals

    bootstrap_bindings = [lit for lit in bindings_literals if '-bootstrap"' in lit]
    broker_bindings = [lit for lit in bindings_literals if '-bootstrap"' not in lit]
    assert bootstrap_bindings
    assert broker_bindings

    for literals in bootstrap_bindings:
        assert '"name":"kafka-cluster-kafka-' in literals
    for literals in broker_bindings:
        assert '"name":"kafka-cluster-broker-' in literals


def test_kafka_target_groups_use_ip_target_type_with_client_ip_preserved(template):
    # targetType=instance + nodeSelector（role: kafka-broker）で broker nodegroup に絞っても、
    # 個別 broker 用 target group には該当 Pod のいない broker ノードまで一緒に登録されて
    # しまう（nodeSelector は Node label までしか絞れず、特定 Pod が乗っているノード単位の
    # 絞り込みはできないため）。ip target type にすると AWS LBC が EndpointSlice から
    # Pod IP を直接 target 登録するため、per-broker Service が選択する 1 Pod だけが
    # 正確に 1 target として登録される。
    # port は broker ごとに割り振った NodePort ではなく、external listener の内部 port
    # （kafka-cluster.yaml の spec.kafka.listeners[].port、全 broker 共通で固定値）を使う。
    # TCP プロトコルの ip target type はデフォルトで client IP preservation が無効になる
    # ため、externalTrafficPolicy: Local が担っていたクライアント送信元 IP 保持を維持する
    # には target group 側で明示的に有効化する必要がある。
    _, external_listener_port = parse_kafka_external_listener(manifest_dir("kafka"))
    target_groups = template.find_resources("AWS::ElasticLoadBalancingV2::TargetGroup")
    assert target_groups
    for res in target_groups.values():
        props = res["Properties"]
        assert props["TargetType"] == "ip"
        assert props["Port"] == external_listener_port
        attrs = {a["Key"]: a["Value"] for a in props.get("TargetGroupAttributes", [])}
        assert attrs.get("preserve_client_ip.enabled") == "true"


def test_target_group_binding_uses_ip_target_type_without_node_selector(template):
    # ip target type では nodeSelector（Node 単位の絞り込み）は意味を持たない
    # （AWS LBC が Pod IP を直接 EndpointSlice から解決するため、Node のラベルとは無関係）。
    # 残したままだと「絞り込みが効いている」という誤解を招くため、targetType=ip への
    # 変更と合わせて nodeSelector は削除する。
    all_k8s = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    bindings_literals = [
        _manifest_literals(res["Properties"]["Manifest"])
        for res in all_k8s.values()
        if "TargetGroupBinding" in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert bindings_literals
    for literals in bindings_literals:
        assert '"targetType":"ip"' in literals
        assert "nodeSelector" not in literals


def test_target_group_binding_networking_ingress_uses_listener_container_port(template):
    # networking.ingress は AWS LBC が SG に自動追加する許可ポートを決める。ip mode では
    # 実トラフィックが Pod の container port（external listener の port、全 broker 共通で
    # 固定値）に直接届くため、broker ごとに異なる NodePort ではなく、この固定値を参照する
    # 必要がある。
    _, external_listener_port = parse_kafka_external_listener(manifest_dir("kafka"))
    all_k8s = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    bindings_literals = [
        _manifest_literals(res["Properties"]["Manifest"])
        for res in all_k8s.values()
        if "TargetGroupBinding" in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert bindings_literals
    for literals in bindings_literals:
        assert f'"port":{external_listener_port}' in literals


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


def test_kafka_topic_test_topic_retention_matches_config(template, config):
    # retention.ms は ClusterConfig.kafka_topic_retention_ms が真実の源（環境別に
    # 上書きできるよう config 化）。KafkaTopic CR の spec.config.retention.ms に
    # そのまま反映されていることを確認する。
    all_k8s = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    topics = [
        res for res in all_k8s.values() if '"kind":"KafkaTopic"' in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert len(topics) == 1
    literals = _manifest_literals(topics[0]["Properties"]["Manifest"])
    assert f'"retention.ms":{config.kafka_topic_retention_ms}' in literals


def test_gp3_storage_classes_split_by_workload(template):
    # Kafka broker/controller と Prometheus/Alertmanager は I/O 特性が異なるため、
    # gp3 StorageClass を共有せず gp3（default, 監視系用）と gp3-kafka（Kafka 専用）に分離する。
    # default StorageClass が 2 つ存在すると provisioning 先が曖昧になるため、
    # default を名乗るのは gp3 のみであることも検証する。
    all_k8s = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    storage_classes = [
        res for res in all_k8s.values() if '"kind":"StorageClass"' in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert len(storage_classes) == 2

    literals_by_name = {}
    for res in storage_classes:
        literals = _manifest_literals(res["Properties"]["Manifest"])
        for name in ("gp3", "gp3-kafka"):
            if f'"name":"{name}"' in literals:
                literals_by_name[name] = literals
    assert set(literals_by_name) == {"gp3", "gp3-kafka"}

    assert '"storageclass.kubernetes.io/is-default-class":"true"' in literals_by_name["gp3"]
    assert '"storageclass.kubernetes.io/is-default-class":"true"' not in literals_by_name["gp3-kafka"]


def test_kafka_node_pools_use_dedicated_storage_class(template):
    # broker/controller の EBS 性能要件は監視系（gp3 default）と切り離して個別チューニング
    # できるようにするため、gp3-kafka を参照していること（旧 default gp3 の参照が
    # 残っていないこと）を確認する。
    all_k8s = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    node_pools = [
        res for res in all_k8s.values() if '"kind":"KafkaNodePool"' in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert len(node_pools) == 2
    for res in node_pools:
        literals = _manifest_literals(res["Properties"]["Manifest"])
        assert '"class":"gp3-kafka"' in literals


def test_kafka_broker_pool_resources_come_from_config(template, config):
    # broker の resources/storage/jvmOptions は環境別に上書きできる必要がある
    # (dev 固定値が stg/prd にそのまま流用されていた TODO 項目の解消)。
    # config.kafka_broker_resources の値がそのまま manifest に反映されていることを検証する。
    all_k8s = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    node_pools = [
        res for res in all_k8s.values() if '"kind":"KafkaNodePool"' in _manifest_literals(res["Properties"]["Manifest"])
    ]
    broker_pool = next(
        res
        for res in node_pools
        if '"metadata":{"name":"broker","namespace"' in _manifest_literals(res["Properties"]["Manifest"])
    )
    literals = _manifest_literals(broker_pool["Properties"]["Manifest"])
    resources = config.kafka_broker_resources

    assert f'"requests":{{"memory":"{resources.memory_request}","cpu":"{resources.cpu_request}"}}' in literals
    assert f'"limits":{{"memory":"{resources.memory_limit}","cpu":"{resources.cpu_limit}"}}' in literals
    assert f'"size":"{resources.storage_size}"' in literals
    assert f'"-Xms":"{resources.jvm_xms}","-Xmx":"{resources.jvm_xmx}"' in literals


def test_kafka_controller_pool_resources_come_from_config(template, config):
    all_k8s = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    node_pools = [
        res for res in all_k8s.values() if '"kind":"KafkaNodePool"' in _manifest_literals(res["Properties"]["Manifest"])
    ]
    controller_pool = next(
        res
        for res in node_pools
        if '"metadata":{"name":"controller","namespace"' in _manifest_literals(res["Properties"]["Manifest"])
    )
    literals = _manifest_literals(controller_pool["Properties"]["Manifest"])
    resources = config.kafka_controller_resources

    assert f'"requests":{{"memory":"{resources.memory_request}","cpu":"{resources.cpu_request}"}}' in literals
    assert f'"limits":{{"memory":"{resources.memory_limit}","cpu":"{resources.cpu_limit}"}}' in literals
    assert f'"size":"{resources.storage_size}"' in literals
    assert f'"-Xms":"{resources.jvm_xms}","-Xmx":"{resources.jvm_xmx}"' in literals


def test_eks_pod_identity_agent_addon_version_not_pinned(template):
    # aws_eks_v2.Cluster が自動追加する eks-pod-identity-agent Addon は、
    # vpc-cni/coredns/kube-proxy と同様に EKS のデフォルトバージョンに追従させるため
    # AddonVersion を明示しない。
    addons = template.find_resources("AWS::EKS::Addon")
    matched = [res for res in addons.values() if res["Properties"].get("AddonName") == "eks-pod-identity-agent"]
    assert len(matched) == 1
    assert "AddonVersion" not in matched[0]["Properties"]


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
