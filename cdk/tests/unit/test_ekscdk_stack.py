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


def test_kube_prometheus_stack_enables_prometheus_and_operator(template):
    # chart values で prometheus / Operator を有効化していること（無効化していた頃の
    # 設定を誤って残すと in-cluster Prometheus が立たず Grafana にデータが入らない）。
    charts = template.find_resources("Custom::AWSCDK-EKS-HelmChart")
    kps = [res for res in charts.values() if res["Properties"].get("Chart") == "kube-prometheus-stack"]
    assert len(kps) == 1
    values_literals = _manifest_literals(kps[0]["Properties"]["Values"])
    # 無効化していたときは `enabled: false` を明示していたため、両 enable: false が無いことを assert
    assert '"prometheus":{"enabled":false' not in values_literals
    assert '"prometheusOperator":{"enabled":false' not in values_literals
    # 2 replica HA / 15 day retention を assert（コスト・容量・可用性の前提）
    assert '"replicas":2' in values_literals
    assert '"retention":"15d"' in values_literals
    # AZ 跨ぎの topologySpread が外れると 2 replica が同 AZ に乗りうる
    assert "topology.kubernetes.io/zone" in values_literals


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


def test_kube_prometheus_stack_enables_alertmanager(template):
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
    # 3 replica gossip cluster（標準サイズ）
    assert '"replicas":3' in literals
    # AZ 跨ぎの topologySpread が外れると 3 replica が同 AZ に乗りうる
    assert "topology.kubernetes.io/zone" in literals
    # PDB は Prometheus と Alertmanager の両方で enabled（chart デフォルト minAvailable: 1）。
    # 同一リテラルが 2 箇所以上出現することで両方有効を invariant 化する。片方が消えた
    # リグレッションをここで検知する。
    assert literals.count('"podDisruptionBudget":{"enabled":true}') >= 2


def test_dev_system_nodegroup_uses_larger_instance(dev_template):
    # system は監視 HA（prometheus×2 / alertmanager×3 / grafana / ...）+ Strimzi operator 群
    # + 全ノード共通 DaemonSet で steady ~12-13 pod/node。Cluster Autoscaler / Karpenter が
    # 無く managed nodegroup が固定サイズのため、3 ノード中 1 台喪失で pod が残り 2 ノードに
    # 寄ると max-pods 17 の t4g.medium では収まらない。max-pods 35 の t4g.large で耐える。
    dev_template.has_resource_properties(
        "AWS::EKS::Nodegroup",
        {"NodegroupName": "system-nodegroup", "InstanceTypes": ["t4g.large"]},
    )


def test_kube_prometheus_stack_disables_default_kubelet_too_many_pods(template):
    # chart 同梱の KubeletTooManyPods は severity: info 固定で個別上書き手段が無いため、
    # defaultRules.disabled で無効化して prometheus-rules-node.yaml 側で warning 再定義する。
    charts = template.find_resources("Custom::AWSCDK-EKS-HelmChart")
    kps = [res for res in charts.values() if res["Properties"].get("Chart") == "kube-prometheus-stack"]
    assert len(kps) == 1
    literals = _manifest_literals(kps[0]["Properties"]["Values"])
    assert '"disabled":{"KubeletTooManyPods":true}' in literals


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


def test_node_capacity_rule_overrides_kubelet_too_many_pods_as_warning(template):
    # 無効化した chart default を写し、severity を info -> warning に上げたルールが apply される。
    # autoscaler が無い本環境では Pod capacity 到達が即 Pending 固定に直結するため info では弱い。
    all_k8s = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    matched = [
        res
        for res in all_k8s.values()
        if '"name":"node-capacity-rules"' in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert len(matched) == 1
    literals = _manifest_literals(matched[0]["Properties"]["Manifest"])
    assert '"alert":"KubeletTooManyPods"' in literals
    assert '"severity":"warning"' in literals


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
    ["strimzi-kafka-rules", "alertmanager-smoke-rules", "node-capacity-rules"],
)
def test_prometheus_rule_manifest_applied(template, rule_group_name):
    """PrometheusRule CR が apply される（Kafka 系本番候補 + 動作確認用 smoke の 2 系統）。

    smoke ルール（alertmanager-smoke-rules）は動作確認後に削除予定。削除時はこの
    パラメータと対応 manifest を一緒に消す。
    """
    all_k8s = template.find_resources("Custom::AWSCDK-EKS-KubernetesResource")
    matched = [
        res
        for res in all_k8s.values()
        if '"kind":"PrometheusRule"' in _manifest_literals(res["Properties"]["Manifest"])
        and f'"name":"{rule_group_name}"' in _manifest_literals(res["Properties"]["Manifest"])
    ]
    assert len(matched) == 1, f"PrometheusRule {rule_group_name} が apply されていない"


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
