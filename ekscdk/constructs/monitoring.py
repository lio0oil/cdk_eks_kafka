from typing import cast

from aws_cdk import Duration, Stack
from aws_cdk import aws_aps as aps
from aws_cdk import aws_ec2 as ec2
from aws_cdk import aws_eks_v2 as eks
from aws_cdk import aws_iam as iam
from aws_cdk import aws_logs as logs
from constructs import Construct

from ekscdk.config import ClusterConfig
from ekscdk.constructs._manifest import load, load_text_with_subs, load_with_subs, manifest_dir
from ekscdk.constructs.addons import AddonsConstruct

_DIR = manifest_dir("monitoring")
_DIR_AMP = manifest_dir("amp")


class MonitoringConstruct(Construct):
    """監視環境 Construct

    AWS リソース:
      - AMP Workspace（Prometheus メトリクス保管・query）
      - AMP Managed Scraper（agentless で Pod / Node を scrape して remote_write）
      - CloudWatch Log Group（コンテナログ）

    Kubernetes リソース（CDK 管理 Helm / manifest）:
      - kube-prometheus-stack: Grafana / kube-state-metrics / node-exporter のみ
        （Prometheus / Prometheus Operator / Alertmanager は無効化、AMP に移譲）
      - Fluent Bit DaemonSet: ログ → CloudWatch Logs

    Grafana は AMP datasource のみで query する。
    """

    def __init__(
        self,
        scope: Construct,
        construct_id: str,
        vpc: ec2.IVpc,
        cluster: eks.ICluster,
        config: ClusterConfig,
        addons: AddonsConstruct,
    ) -> None:
        super().__init__(scope, construct_id)

        region = Stack.of(self).region

        # ── AMP Workspace ────────────────────────────────────────────────────
        amp_workspace = aps.CfnWorkspace(self, "AmpWorkspace", alias=config.cluster_name)

        # ── AMP Managed Scraper ──────────────────────────────────────────────
        # AWS マネージドの agentless scraper が EKS API server 経由で Pod / Node を
        # discovery し、scrape して AMP workspace に remote_write する。
        # aws_eks_v2.Cluster の AccessEntry ベース認証と組み合わさり、CfnScraper 作成時に
        # EKS Access Entry policy が自動生成されて cluster API への read 権限が付く。
        # EKS managed nodegroup は cluster SG が node に自動 attach されるため、
        # cluster SG に scraper SG からの ingress を許可すれば Pod の /metrics に到達できる。
        scraper_sg = ec2.SecurityGroup(
            self,
            "AmpScraperSg",
            vpc=vpc,
            description="AMP Managed Scraper ENIs",
            allow_all_outbound=True,
        )
        cluster_sg = ec2.SecurityGroup.from_security_group_id(
            self,
            "ClusterSgRef",
            cluster.cluster_security_group_id,
            mutable=True,
        )
        cluster_sg.add_ingress_rule(
            peer=scraper_sg,
            connection=ec2.Port.all_traffic(),
            description="AMP Scraper to cluster pods/nodes",
        )
        scrape_yaml = load_text_with_subs(_DIR_AMP, "scrape-config.yaml", CLUSTER_NAME=config.cluster_name)
        private_subnets = vpc.select_subnets(subnet_type=ec2.SubnetType.PRIVATE_WITH_EGRESS)
        # AWS::APS::Scraper の CFN handler は ConfigurationBlob を内部で base64 encode する。
        # CFN 公式 doc には "base 64 encoded" と書かれているが、実装は raw 文字列を期待し
        # 自前で encode する。base64 を渡すと二重 encode で AMP 側の YAML parse が失敗し、
        # "ValidationException: Invalid Prometheus scrape configuration" として返ってくる。
        # 参考: aws/aws-cdk-lib aws_aps の type は str だが、内部実装は CLI 同等の base64
        # ではなく CFN handler 経由の自動 encode が走る。
        scraper = aps.CfnScraper(
            self,
            "AmpScraper",
            alias=f"{config.cluster_name}-scraper",
            destination=aps.CfnScraper.DestinationProperty(
                amp_configuration=aps.CfnScraper.AmpConfigurationProperty(
                    workspace_arn=amp_workspace.attr_arn,
                ),
            ),
            scrape_configuration=aps.CfnScraper.ScrapeConfigurationProperty(
                configuration_blob=scrape_yaml,
            ),
            source=aps.CfnScraper.SourceProperty(
                eks_configuration=aps.CfnScraper.EksConfigurationProperty(
                    cluster_arn=cluster.cluster_arn,
                    subnet_ids=private_subnets.subnet_ids,
                    security_group_ids=[scraper_sg.security_group_id],
                ),
            ),
        )
        # CreateScraper は cluster の internal state（access entry 伝播 / API 認可）が
        # settle するまで失敗するため、AWS LBC chart (wait=True で Pod Ready まで待つ) と
        # addon 群への依存で cluster が確実に動く状態まで scraper 作成を遅延させる。
        scraper.node.add_dependency(addons)
        scraper.node.add_dependency(addons.aws_lbc_chart)

        # ── AMP Recording Rules ──────────────────────────────────────────────
        # kube-prometheus-stack 84.5.0 chart 同梱の PrometheusRule 群から recording
        # rule のみ抽出して AMP の Rule Manager に登録。chart の dashboards が参照する
        # 事前集計メトリクス（node_namespace_pod_container:*, cluster:namespace:* 等）が
        # AMP 側で発火し、"Kubernetes / Compute Resources / *" 系ダッシュボードがそのまま動く。
        # 抽出元: helm template prometheus-community/kube-prometheus-stack --version 84.5.0
        # の templates/prometheus/rules-1.14/*.yaml （94 rules / 16 groups）。
        # AWS observability solution v3.0.0 の rule は古く一部 dashboard と乖離（例:
        # node_namespace_pod_container:container_cpu_usage_seconds_total:sum_irate vs sum_rate5m）
        # していたため、chart 由来の最新ルールを採用する。
        rules_yaml = load_text_with_subs(_DIR_AMP, "recording-rules.yaml")
        aps.CfnRuleGroupsNamespace(
            self,
            "AmpRecordingRules",
            workspace=amp_workspace.attr_arn,
            name="kube-prometheus-stack-recording-rules",
            data=rules_yaml,
        )

        # ── CloudWatch Log Group ──────────────────────────────────────────────
        log_group = logs.LogGroup(
            self,
            "ApplicationLogGroup",
            log_group_name=f"/aws/eks/{config.cluster_name}/application",
            retention=config.log_retention,
            removal_policy=config.log_removal_policy,
        )

        # ── monitoring Namespace ──────────────────────────────────────────────
        namespace = cluster.add_manifest("MonitoringNamespace", load(_DIR, "namespace.yaml"))

        # ── Fluent Bit Pod Identity ───────────────────────────────────────────
        fluent_bit_sa = cluster.add_service_account(
            "FluentBitSa",
            name="fluent-bit",
            namespace="monitoring",
            identity_type=eks.IdentityType.POD_IDENTITY,
        )
        fluent_bit_sa.node.add_dependency(namespace)
        cast(iam.Role, fluent_bit_sa.role).add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "logs:DescribeLogGroups",
                    "logs:DescribeLogStreams",
                ],
                resources=[log_group.log_group_arn, log_group.log_group_arn + ":*"],
            )
        )

        # ── Grafana Pod Identity ───────────────────────────────────────────────
        # self-hosted Grafana が AMP を data source として SigV4 で query するため。
        # AmazonPrometheusQueryAccess: aps:QueryMetrics / GetSeries / GetLabels /
        # GetMetricMetadata / DescribeWorkspace 等 read 系を一括付与。
        grafana_sa = cluster.add_service_account(
            "GrafanaSa",
            name="grafana",
            namespace="monitoring",
            identity_type=eks.IdentityType.POD_IDENTITY,
        )
        grafana_sa.node.add_dependency(namespace)
        cast(iam.Role, grafana_sa.role).add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonPrometheusQueryAccess")
        )

        # ── kube-prometheus-stack（Helm）──────────────────────────────────────
        # Prometheus / Operator / Alertmanager は無効化済み（values 参照）。
        # Grafana / kube-state-metrics / node-exporter のみが deploy される。
        amp_query_url = amp_workspace.attr_prometheus_endpoint.rstrip("/")
        kps_values = load_with_subs(
            _DIR,
            "kube-prometheus-stack-values.yaml",
            REGION=region,
            AMP_QUERY_URL=amp_query_url,
        )
        kps = cluster.add_helm_chart(
            "KubePrometheusStack",
            chart="kube-prometheus-stack",
            repository=config.kube_prometheus_stack_chart_repo,
            namespace="monitoring",
            version=config.kube_prometheus_stack_chart_version,
            values=kps_values,
            timeout=Duration.minutes(15),
        )
        kps.node.add_dependency(namespace)
        kps.node.add_dependency(grafana_sa)

        # ── Grafana Dashboard ConfigMap ───────────────────────────────────────
        # kube-prometheus-stack の sidecar が monitoring namespace の
        # ConfigMap でラベル grafana_dashboard=1 を持つものを自動取り込みする。
        for fname in (
            "grafana-strimzi-kafka-dashboard.yaml",
            "grafana-strimzi-exporter-dashboard.yaml",
            "grafana-strimzi-operators-dashboard.yaml",
        ):
            cm_id = "Dash" + fname.removeprefix("grafana-strimzi-").removesuffix("-dashboard.yaml").title().replace(
                "-", ""
            )
            cm = cluster.add_manifest(cm_id, load(_DIR, f"dashboards/{fname}"))
            cm.node.add_dependency(kps)

        # ── Fluent Bit DaemonSet（Helm）───────────────────────────────────────
        fluent_bit = cluster.add_helm_chart(
            "FluentBit",
            chart="fluent-bit",
            repository=config.fluent_bit_chart_repo,
            namespace="monitoring",
            version=config.fluent_bit_chart_version,
            values=load_with_subs(
                _DIR,
                "fluent-bit-values.yaml",
                REGION=region,
                LOG_GROUP_NAME=log_group.log_group_name,
            ),
        )
        fluent_bit.node.add_dependency(fluent_bit_sa)
