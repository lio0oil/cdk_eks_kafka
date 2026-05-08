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
      - Kafka Exporter（Strimzi 組み込み）: Consumer Lag メトリクス → ADOT 経由で AMP へ
      - JMX Prometheus Exporter（Strimzi 組み込み）: ブローカー内部メトリクス → ADOT kubernetes_sd 経由で AMP へ
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

        # ── ADOT RBAC（Pod ディスカバリ用）────────────────────────────────────
        adot_cluster_role = cluster.add_manifest(
            "AdotClusterRole",
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "ClusterRole",
                "metadata": {"name": "adot-collector"},
                "rules": [
                    {
                        "apiGroups": [""],
                        "resources": ["nodes", "pods", "services", "endpoints", "namespaces"],
                        "verbs": ["get", "list", "watch"],
                    },
                ],
            },
        )
        adot_cluster_role_binding = cluster.add_manifest(
            "AdotClusterRoleBinding",
            {
                "apiVersion": "rbac.authorization.k8s.io/v1",
                "kind": "ClusterRoleBinding",
                "metadata": {"name": "adot-collector"},
                "roleRef": {
                    "apiGroup": "rbac.authorization.k8s.io",
                    "kind": "ClusterRole",
                    "name": "adot-collector",
                },
                "subjects": [
                    {
                        "kind": "ServiceAccount",
                        "name": "adot-collector",
                        "namespace": "monitoring",
                    }
                ],
            },
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
        # デフォルトは AWS_SSO（IAM Identity Center が有効なアカウントで利用可能）。
        # SSO 未導入の場合は -c amg-auth-provider=SAML を指定する。
        amg_auth_provider: str = self.node.try_get_context("amg-auth-provider") or "AWS_SSO"

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
            authentication_providers=[amg_auth_provider],
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
                "extraEnvs": [
                    {
                        "name": "K8S_NODE_NAME",
                        "valueFrom": {"fieldRef": {"fieldPath": "spec.nodeName"}},
                    }
                ],
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
                                        "job_name": "kafka-exporter",
                                        "static_configs": [
                                            {"targets": ["kafka-cluster-kafka-exporter.kafka.svc.cluster.local:9404"]}
                                        ],
                                    },
                                    {
                                        "job_name": "kafka-jmx",
                                        "kubernetes_sd_configs": [
                                            {
                                                "role": "pod",
                                                "namespaces": {"names": ["kafka"]},
                                            }
                                        ],
                                        "relabel_configs": [
                                            {
                                                "source_labels": ["__meta_kubernetes_pod_label_strimzi_io_kind"],
                                                "action": "keep",
                                                "regex": "Kafka",
                                            },
                                            {
                                                "source_labels": ["__meta_kubernetes_pod_node_name"],
                                                "action": "keep",
                                                "regex": "${K8S_NODE_NAME}",
                                            },
                                            {
                                                "source_labels": ["__meta_kubernetes_pod_container_port_name"],
                                                "action": "keep",
                                                "regex": "tcp-prometheus",
                                            },
                                            {
                                                "source_labels": ["__meta_kubernetes_namespace"],
                                                "target_label": "namespace",
                                            },
                                            {
                                                "source_labels": ["__meta_kubernetes_pod_name"],
                                                "target_label": "pod",
                                            },
                                            {
                                                "source_labels": ["__meta_kubernetes_pod_label_strimzi_io_cluster"],
                                                "target_label": "kafka_cluster",
                                            },
                                            {
                                                "source_labels": ["__meta_kubernetes_pod_label_strimzi_io_pool_name"],
                                                "target_label": "pool",
                                            },
                                        ],
                                    },
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
        adot.node.add_dependency(adot_cluster_role_binding)

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

