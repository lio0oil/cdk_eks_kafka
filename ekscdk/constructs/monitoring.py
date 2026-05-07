from typing import cast

from aws_cdk import Stack
from aws_cdk import aws_aps as aps
from aws_cdk import aws_eks_v2 as eks
from aws_cdk import aws_grafana as grafana
from aws_cdk import aws_iam as iam
from aws_cdk import aws_logs as logs
from constructs import Construct


class MonitoringConstruct(Construct):
    """監視環境 Construct

    AWS リソース:
      - AMP（Prometheus メトリクス）
      - AMG（Grafana ダッシュボード / AWS SSO 認証）
      - CloudWatch Log Group（コンテナログ）

    Kubernetes リソース（CDK 管理 Helm）:
      - ADOT DaemonSet: メトリクス → AMP + CloudWatch、トレース → X-Ray
      - Fluent Bit DaemonSet: ログ → CloudWatch Logs
      - kminion: Kafka Consumer Lag メトリクス → ADOT 経由で AMP へ
    """

    def __init__(self, scope: Construct, construct_id: str, cluster: eks.ICluster) -> None:
        super().__init__(scope, construct_id)

        region = Stack.of(self).region

        # ── AMP ──────────────────────────────────────────────────────────────
        amp_workspace = aps.CfnWorkspace(self, "AmpWorkspace", alias="eks-cluster")
        amp_remote_write_url = f"{amp_workspace.attr_prometheus_endpoint}api/v1/remote_write"

        # ── CloudWatch Log Group ──────────────────────────────────────────────
        log_group = logs.LogGroup(
            self,
            "ApplicationLogGroup",
            log_group_name="/aws/eks/eks-cluster/application",
            retention=logs.RetentionDays.ONE_MONTH,
        )

        # ── monitoring Namespace ──────────────────────────────────────────────
        namespace = cluster.add_manifest(
            "MonitoringNamespace",
            {"apiVersion": "v1", "kind": "Namespace", "metadata": {"name": "monitoring"}},
        )

        # ── ADOT IRSA ─────────────────────────────────────────────────────────
        adot_sa = cluster.add_service_account("AdotSa", name="adot-collector", namespace="monitoring")
        adot_sa.node.add_dependency(namespace)
        for stmt in [
            iam.PolicyStatement(
                actions=["aps:RemoteWrite", "aps:GetSeries", "aps:GetLabels", "aps:GetMetricMetadata"],
                resources=[amp_workspace.attr_arn],
            ),
            iam.PolicyStatement(
                actions=[
                    "xray:PutTraceSegments",
                    "xray:PutTelemetryRecords",
                    "xray:GetSamplingRules",
                    "xray:GetSamplingTargets",
                ],
                resources=["*"],
            ),
            iam.PolicyStatement(
                actions=[
                    "cloudwatch:PutMetricData",
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "logs:DescribeLogGroups",
                    "logs:DescribeLogStreams",
                    "ec2:DescribeVolumes",
                    "ec2:DescribeTags",
                ],
                resources=["*"],
            ),
        ]:
            cast(iam.Role, adot_sa.role).add_to_policy(stmt)

        # ── Fluent Bit IRSA ───────────────────────────────────────────────────
        fluent_bit_sa = cluster.add_service_account("FluentBitSa", name="fluent-bit", namespace="monitoring")
        fluent_bit_sa.node.add_dependency(namespace)
        cast(iam.Role, fluent_bit_sa.role).add_to_policy(
            iam.PolicyStatement(
                actions=[
                    "logs:CreateLogGroup",
                    "logs:CreateLogStream",
                    "logs:PutLogEvents",
                    "logs:DescribeLogGroups",
                    "logs:DescribeLogStreams",
                ],
                resources=["*"],
            )
        )

        # ── AMG ──────────────────────────────────────────────────────────────
        # AWS SSO が有効なアカウントで利用可能。SAML を使う場合は authentication_providers=["SAML"] に変更。
        amg_role = iam.Role(
            self,
            "AmgRole",
            assumed_by=iam.ServicePrincipal("grafana.amazonaws.com"),  # type: ignore[arg-type]
        )
        amg_role.add_managed_policy(
            iam.ManagedPolicy.from_aws_managed_policy_name("AmazonPrometheusQueryAccess")
        )
        grafana.CfnWorkspace(
            self,
            "AmgWorkspace",
            name="eks-cluster-grafana",
            account_access_type="CURRENT_ACCOUNT",
            authentication_providers=["AWS_SSO"],
            permission_type="SERVICE_MANAGED",
            role_arn=amg_role.role_arn,
            data_sources=["PROMETHEUS"],
            grafana_version="10.4",
        )

        # ── ADOT DaemonSet（Helm）────────────────────────────────────────────
        # amazon/aws-otel-collector イメージ: awscontainerinsightreceiver / awsxray exporter を内包
        adot = cluster.add_helm_chart(
            "AdotCollector",
            chart="opentelemetry-collector",
            repository="https://open-telemetry.github.io/opentelemetry-helm-charts",
            namespace="monitoring",
            version="0.108.0",
            values={
                "mode": "daemonset",
                "image": {
                    "repository": "amazon/aws-otel-collector",
                    "tag": "v0.40.0",
                },
                "serviceAccount": {"create": False, "name": "adot-collector"},
                "tolerations": [{"operator": "Exists"}],
                "resources": {
                    "requests": {"memory": "256Mi", "cpu": "100m"},
                    "limits": {"memory": "512Mi", "cpu": "200m"},
                },
                "config": {
                    "receivers": {
                        "otlp": {
                            "protocols": {
                                "grpc": {"endpoint": "0.0.0.0:4317"},
                                "http": {"endpoint": "0.0.0.0:4318"},
                            }
                        },
                        "awscontainerinsightreceiver": {},
                        "prometheus": {
                            "config": {
                                "scrape_configs": [
                                    {
                                        "job_name": "kminion",
                                        "static_configs": [
                                            {"targets": ["kminion.monitoring.svc.cluster.local:8080"]}
                                        ],
                                    }
                                ]
                            }
                        },
                    },
                    "processors": {
                        "batch": {},
                        "memory_limiter": {
                            "check_interval": "1s",
                            "limit_percentage": 75,
                            "spike_limit_percentage": 15,
                        },
                    },
                    "exporters": {
                        "awsxray": {"region": region},
                        "prometheusremotewrite": {
                            "endpoint": amp_remote_write_url,
                            "auth": {"authenticator": "sigv4auth"},
                        },
                        "awscloudwatch": {
                            "region": region,
                            "namespace": "EKS/ContainerInsights",
                            "log_group_name": "/aws/eks/eks-cluster/performance",
                        },
                    },
                    "extensions": {
                        "sigv4auth": {"region": region, "service": "aps"},
                        "health_check": {},
                    },
                    "service": {
                        "extensions": ["sigv4auth", "health_check"],
                        "pipelines": {
                            "traces": {
                                "receivers": ["otlp"],
                                "processors": ["batch"],
                                "exporters": ["awsxray"],
                            },
                            "metrics": {
                                "receivers": ["awscontainerinsightreceiver", "prometheus"],
                                "processors": ["memory_limiter", "batch"],
                                "exporters": ["prometheusremotewrite", "awscloudwatch"],
                            },
                        },
                    },
                },
            },
        )
        adot.node.add_dependency(adot_sa)

        # ── Fluent Bit DaemonSet（Helm）───────────────────────────────────────
        fluent_bit = cluster.add_helm_chart(
            "FluentBit",
            chart="fluent-bit",
            repository="https://fluent.github.io/helm-charts",
            namespace="monitoring",
            version="0.47.9",
            values={
                "serviceAccount": {"create": False, "name": "fluent-bit"},
                "tolerations": [{"operator": "Exists"}],
                "resources": {
                    "requests": {"memory": "128Mi", "cpu": "50m"},
                    "limits": {"memory": "256Mi", "cpu": "200m"},
                },
                "config": {
                    "inputs": (
                        "[INPUT]\n"
                        "    Name              tail\n"
                        "    Tag               kube.*\n"
                        "    Path              /var/log/containers/*.log\n"
                        "    multiline.parser  docker, cri\n"
                        "    DB                /var/log/flb_kube.db\n"
                        "    Mem_Buf_Limit     5MB\n"
                    ),
                    "filters": (
                        "[FILTER]\n"
                        "    Name                kubernetes\n"
                        "    Match               kube.*\n"
                        "    Kube_URL            https://kubernetes.default.svc:443\n"
                        "    Kube_CA_File        /var/run/secrets/kubernetes.io/serviceaccount/ca.crt\n"
                        "    Kube_Token_File     /var/run/secrets/kubernetes.io/serviceaccount/token\n"
                        "    Merge_Log           On\n"
                        "    Keep_Log            Off\n"
                    ),
                    "outputs": (
                        "[OUTPUT]\n"
                        "    Name              cloudwatch_logs\n"
                        "    Match             kube.*\n"
                        f"    region            {region}\n"
                        f"    log_group_name    {log_group.log_group_name}\n"
                        "    log_stream_prefix from-fluent-bit-\n"
                        "    auto_create_group false\n"
                    ),
                },
            },
        )
        fluent_bit.node.add_dependency(fluent_bit_sa)

        # ── kminion（Helm）────────────────────────────────────────────────────
        cluster.add_helm_chart(
            "Kminion",
            chart="kminion",
            repository="https://cloudhut.github.io/charts",
            namespace="monitoring",
            version="0.3.0",
            values={
                "resources": {
                    "requests": {"memory": "128Mi", "cpu": "50m"},
                    "limits": {"memory": "256Mi", "cpu": "200m"},
                },
                "affinity": {
                    "nodeAffinity": {
                        "requiredDuringSchedulingIgnoredDuringExecution": {
                            "nodeSelectorTerms": [
                                {
                                    "matchExpressions": [
                                        {"key": "role", "operator": "In", "values": ["system"]}
                                    ]
                                }
                            ]
                        }
                    }
                },
                "kminion": {
                    "kafka": {
                        "brokers": ["kafka-cluster-kafka-bootstrap.kafka.svc.cluster.local:9092"],
                        "tls": {"enabled": False},
                    },
                    "minion": {
                        "consumerGroups": {"enabled": True, "allowedGroupIdExpr": ".*"},
                        "topics": {"enabled": True},
                    },
                    "exporter": {"port": 8080},
                },
            },
        )
