from __future__ import annotations

from dataclasses import dataclass

from aws_cdk import RemovalPolicy
from aws_cdk import aws_eks_v2 as eks
from aws_cdk import aws_logs as logs

# Kubernetes 1.35 向けアドオン最新バージョン（2026-05 時点）
# vpc-cni/coredns/kube-proxy は bootstrap_self_managed_addons のデフォルト（True）による
# self-managed 版をそのまま使うためここでは管理しない（EKS のデフォルトバージョンに追従する）。
# eks-pod-identity-agent は aws_eks_v2.Cluster が `IdentityType.POD_IDENTITY` の SA を
# 作る際に内部で自動追加する Addon で、同様に EKS のデフォルトバージョンに追従させる。
# 更新コマンド:
#   for addon in aws-ebs-csi-driver metrics-server eks-node-monitoring-agent snapshot-controller; do
#     echo -n "$addon: "
#     aws eks describe-addon-versions --addon-name "$addon" \
#       --kubernetes-version 1.35 \
#       --query 'addons[0].addonVersions[0].addonVersion' --output text
#   done
_ADDON_VERSIONS_K8S_135: dict[str, str] = {
    "aws-ebs-csi-driver": "v1.59.0-eksbuild.1",
    "metrics-server": "v0.8.1-eksbuild.6",
    "eks-node-monitoring-agent": "v1.6.4-eksbuild.1",
    "snapshot-controller": "v8.5.0-eksbuild.3",
}


@dataclass
class KafkaPoolResourceConfig:
    """KafkaNodePool（broker/controller）の resources / storage / jvmOptions。

    node-pool-broker.yaml / node-pool-controller.yaml のプレースホルダーに
    そのまま注入する値。stg/prd 運用前に見直しが必要な値（TODO.md 参照）で、
    現状は暫定的に dev 想定値を全環境共通で設定している。
    """

    memory_request: str
    memory_limit: str
    cpu_request: str
    cpu_limit: str
    storage_size: str
    jvm_xms: str
    jvm_xmx: str


@dataclass
class PrometheusResourceConfig:
    """Prometheus の resources / storageSpec / retention / scrapeInterval。

    kube-prometheus-stack-values.yaml の prometheusSpec 用プレースホルダーに
    そのまま注入する値。broker_count 増設等でスクレイプ対象が増えると
    見直しが必要になる値（TODO.md 参照）で、現状は暫定的に全環境共通で設定している。
    """

    memory_request: str
    memory_limit: str
    cpu_request: str
    cpu_limit: str
    storage_size: str
    retention: str
    scrape_interval: str


@dataclass
class AlertmanagerResourceConfig:
    """Alertmanager の resources / storage（notification log / silence 永続化用）。

    kube-prometheus-stack-values.yaml の alertmanagerSpec 用プレースホルダーに
    そのまま注入する値。broker_count 増設等で通知量が増えると見直しが必要になる値
    （TODO.md 参照）で、現状は暫定的に全環境共通で設定している。
    """

    memory_request: str
    memory_limit: str
    cpu_request: str
    cpu_limit: str
    storage_size: str


@dataclass
class FluentBitResourceConfig:
    """Fluent Bit の resources / INPUT バッファ上限（Mem_Buf_Limit）。

    fluent-bit-values.yaml のプレースホルダーにそのまま注入する値。ノード当たりの
    ログ流量が増えると見直しが必要になる値で、現状は暫定的に全環境共通で設定している。
    """

    memory_request: str
    memory_limit: str
    cpu_request: str
    cpu_limit: str
    mem_buf_limit: str


@dataclass
class ClusterConfig:
    """EKS クラスター構成値の一元管理クラス。

    環境ごとの差分は for_dev / for_stg / for_prd ファクトリで定義する。
    バージョン・インスタンスタイプ・スケール設定をすべてここで管理し、
    各 Construct へ引数として渡すことでハードコードを排除する。
    prd が閉域網の場合など環境ごとに *_chart_repo フィールドで内部ミラーを指定できる。
    """

    cluster_name: str
    admin_role_name: str
    nat_gateways: int
    system_instance_type: str
    system_min_size: int
    system_max_size: int
    system_desired_size: int
    kafka_broker_instance_type: str
    kafka_controller_instance_type: str
    nodegroup_ami_type: eks.NodegroupAmiType
    addon_versions: dict[str, str]
    strimzi_version: str
    strimzi_chart_repo: str
    kube_prometheus_stack_chart_version: str
    kube_prometheus_stack_chart_repo: str
    aws_lbc_chart_version: str
    aws_lbc_chart_repo: str
    fluent_bit_chart_version: str
    fluent_bit_chart_repo: str
    log_retention: logs.RetentionDays
    log_removal_policy: RemovalPolicy
    enable_interface_endpoints: bool
    # KafkaNodePool 削除時に PVC を一緒に削除するか（Strimzi の deleteClaim フィールド）
    # dev は True（環境破棄時に PVC ごとクリーンアップ）、stg/prd は False（データ保護）
    kafka_delete_claim: bool
    # KRaft Controller の replica 数（KRaft は奇数推奨、通常 3）
    # node-pool-controller.yaml の replicas と kafka nodegroup サイズの両方に反映
    kafka_controller_count: int
    # Kafka broker/controller ノードグループを VPC の 1 AZ 目だけに固定するか。
    # dev は True（AZ 跨ぎのデータ転送料を避けてコスト最適化）、
    # stg/prd は False（AZ 障害時も broker/controller が全滅しないよう multi-AZ を維持）。
    kafka_single_az: bool
    # KafkaNodePool（broker/controller）の resources/storage/jvmOptions。
    # 現状は暫定的に dev 想定値を全環境共通で設定している（stg/prd 運用前に見直しが必要、TODO.md 参照）。
    kafka_broker_resources: KafkaPoolResourceConfig
    kafka_controller_resources: KafkaPoolResourceConfig
    # Kafka Broker の replica 数。NLB target group / nodegroup capacity /
    # KafkaNodePool replicas / kafka-cluster.yaml の brokers[] すべての単一の真実の源。
    # 既存ブローカーの advertisedPort/nodePort は変えない（クライアント接続が壊れる）ため、
    # 増設は末尾追加・縮退は Cruise Control での reassign 後に行うこと。
    broker_count: int
    # EKS クラスターの削除保護（CloudFormation の DeletionProtection）
    # 有効化すると aws eks delete-cluster が拒否される（誤削除防止）
    # dev は False（環境破棄を容易に）、stg/prd は True（事故防止）
    deletion_protection: bool
    # VPC Flow Logs を CloudWatch Logs に送るか
    # dev は False（コスト削減）、stg/prd は True（監査・インシデント調査用）
    enable_vpc_flow_logs: bool
    # EKS Control Plane Logs（audit / api / authenticator）を CloudWatch Logs に送るか
    # dev は False（コスト削減、監査要件なし）、stg/prd は True（インシデント調査・監査用）
    # 3 種類の粒度を分ける運用価値が薄いためまとめて on/off する
    enable_control_plane_logs: bool
    # Alertmanager の SNS Topic に Lambda subscriber を付け、通知本文を CloudWatch Logs に
    # 出力するか。dev=True（Email/Teams を用意せず CloudWatch Logs で通知内容を検証する
    # ためのテスト用経路）、stg/prd=False（実通知先に配送するため検証用 Lambda は不要）。
    enable_alertmanager_sns_log_forwarder: bool
    # S3 Tables の table-bucket 名 (アカウント内ユニーク・3-63 文字 lowercase/numbers/hyphens)。
    # consumer (kafka/consumer) の Iceberg 書き込み先。
    s3_table_bucket_name: str
    # S3 Tables の table-bucket を CDK destroy 時に残すか削除するか。
    # dev は DESTROY (環境破棄を容易に)、stg/prd は RETAIN (データ保護)。
    s3_table_bucket_removal_policy: RemovalPolicy
    # consumer (Spark Structured Streaming) の checkpointLocation 用 S3 バケット名 suffix。
    # 実名は `kafka-consumer-checkpoint-{account}-{suffix}` (アカウント+環境でグローバル衝突を回避)。
    s3_consumer_checkpoint_suffix: str
    # in-cluster Prometheus の resources/storageSpec/retention/scrapeInterval。
    # broker_count 増設で per-broker metrics（kafka-exporter/JMX）が増えた際や、SLO 月次
    # レポート要件で retention を伸ばす際に見直せるよう config 化（TODO.md 参照）。
    # 現状は暫定的に全環境共通値。
    prometheus_resources: PrometheusResourceConfig
    # in-cluster Alertmanager の resources/storage（notification log / silence 永続化用）。
    # broker_count 増設で通知量が増えた際に見直せるよう config 化。
    alertmanager_resources: AlertmanagerResourceConfig
    # in-cluster Prometheus / Alertmanager の replica 数（HA 構成のサイズ）。
    # 値は現状 dev/stg/prd 共通の標準サイズのままだが、他の性能パラメータと同じ
    # 仕組みに揃えて config 化する。
    prometheus_replicas: int
    alertmanager_replicas: int
    # Fluent Bit の resources / INPUT バッファ上限（Mem_Buf_Limit）。ノード当たりの
    # ログ流量が変わった場合に見直せるよう config 化。
    fluent_bit_resources: FluentBitResourceConfig

    @classmethod
    def for_dev(cls, cluster_name: str = "eks-cluster-dev") -> ClusterConfig:
        return cls(
            cluster_name=cluster_name,
            admin_role_name=f"eks-cluster-admin-{cluster_name}",
            nat_gateways=1,
            # dev はテスト用途のためコスト最適化（Graviton2 burstable t4g）。
            # ボトルネックは memory ではなく pod 数: system には監視 HA（prometheus×2 /
            # alertmanager×3 / grafana / kube-state-metrics / operator）+ Strimzi operator 群
            # + 全ノード共通 DaemonSet が乗り steady ~12-13 pod/node。Cluster Autoscaler /
            # Karpenter が無く nodegroup が固定サイズのため、3 ノード中 1 台喪失で pod が残り
            # 2 ノードに寄ると max-pods 17 の t4g.medium では収まらず Pending が発生する。
            # max-pods 35 の t4g.large にして 1 ノード喪失時の集中に耐える headroom を確保する。
            system_instance_type="t4g.large",
            system_min_size=2,
            system_max_size=4,
            system_desired_size=3,
            # broker は JVM heap 2GB + page cache 用に余裕が必要なため t4g.large 維持
            # （t4g.medium にすると page cache が枯渇し dev でも本番と挙動が乖離する）
            kafka_broker_instance_type="t4g.large",
            # controller はメタデータ管理のみで負荷軽微なため broker より小型インスタンス
            kafka_controller_instance_type="t4g.medium",
            nodegroup_ami_type=eks.NodegroupAmiType.AL2023_ARM_64_STANDARD,
            addon_versions=dict(_ADDON_VERSIONS_K8S_135),
            strimzi_version="1.0.0",
            strimzi_chart_repo="https://strimzi.io/charts/",
            kube_prometheus_stack_chart_version="84.5.0",
            kube_prometheus_stack_chart_repo="https://prometheus-community.github.io/helm-charts",
            aws_lbc_chart_version="3.3.0",
            aws_lbc_chart_repo="https://aws.github.io/eks-charts",
            fluent_bit_chart_version="0.57.3",
            fluent_bit_chart_repo="https://fluent.github.io/helm-charts",
            log_retention=logs.RetentionDays.ONE_WEEK,
            log_removal_policy=RemovalPolicy.DESTROY,
            enable_interface_endpoints=False,
            kafka_delete_claim=True,
            kafka_controller_count=3,
            kafka_single_az=True,
            kafka_broker_resources=KafkaPoolResourceConfig(
                memory_request="2Gi",
                memory_limit="4Gi",
                cpu_request="250m",
                cpu_limit="500m",
                storage_size="20Gi",
                jvm_xms="1024m",
                jvm_xmx="2048m",
            ),
            kafka_controller_resources=KafkaPoolResourceConfig(
                memory_request="1Gi",
                memory_limit="2Gi",
                cpu_request="250m",
                cpu_limit="500m",
                storage_size="20Gi",
                jvm_xms="512m",
                jvm_xmx="1024m",
            ),
            broker_count=3,
            deletion_protection=False,
            enable_vpc_flow_logs=False,
            enable_control_plane_logs=False,
            enable_alertmanager_sns_log_forwarder=True,
            s3_table_bucket_name="kafka-events-dev",
            s3_table_bucket_removal_policy=RemovalPolicy.DESTROY,
            s3_consumer_checkpoint_suffix="dev",
            prometheus_resources=PrometheusResourceConfig(
                memory_request="512Mi",
                memory_limit="1Gi",
                cpu_request="250m",
                cpu_limit="500m",
                storage_size="20Gi",
                retention="15d",
                scrape_interval="30s",
            ),
            alertmanager_resources=AlertmanagerResourceConfig(
                memory_request="64Mi",
                memory_limit="128Mi",
                cpu_request="50m",
                cpu_limit="100m",
                storage_size="1Gi",
            ),
            prometheus_replicas=2,
            alertmanager_replicas=3,
            fluent_bit_resources=FluentBitResourceConfig(
                memory_request="128Mi",
                memory_limit="256Mi",
                cpu_request="50m",
                cpu_limit="200m",
                mem_buf_limit="5MB",
            ),
        )

    @classmethod
    def for_stg(cls, cluster_name: str = "eks-cluster-stg") -> ClusterConfig:
        return cls(
            cluster_name=cluster_name,
            admin_role_name=f"eks-cluster-admin-{cluster_name}",
            nat_gateways=1,
            system_instance_type="m8g.large",
            system_min_size=3,
            system_max_size=6,
            system_desired_size=3,
            kafka_broker_instance_type="r8g.large",
            # controller はメタデータ管理のみで負荷軽微なため broker (memory-optimized) より小型・汎用
            kafka_controller_instance_type="m8g.medium",
            nodegroup_ami_type=eks.NodegroupAmiType.AL2023_ARM_64_STANDARD,
            addon_versions=dict(_ADDON_VERSIONS_K8S_135),
            strimzi_version="1.0.0",
            strimzi_chart_repo="https://strimzi.io/charts/",
            kube_prometheus_stack_chart_version="84.5.0",
            kube_prometheus_stack_chart_repo="https://prometheus-community.github.io/helm-charts",
            aws_lbc_chart_version="3.3.0",
            aws_lbc_chart_repo="https://aws.github.io/eks-charts",
            fluent_bit_chart_version="0.57.3",
            fluent_bit_chart_repo="https://fluent.github.io/helm-charts",
            log_retention=logs.RetentionDays.ONE_MONTH,
            log_removal_policy=RemovalPolicy.RETAIN,
            enable_interface_endpoints=True,
            kafka_delete_claim=False,
            kafka_controller_count=3,
            kafka_single_az=False,
            kafka_broker_resources=KafkaPoolResourceConfig(
                memory_request="2Gi",
                memory_limit="4Gi",
                cpu_request="250m",
                cpu_limit="500m",
                storage_size="20Gi",
                jvm_xms="1024m",
                jvm_xmx="2048m",
            ),
            kafka_controller_resources=KafkaPoolResourceConfig(
                memory_request="1Gi",
                memory_limit="2Gi",
                cpu_request="250m",
                cpu_limit="500m",
                storage_size="20Gi",
                jvm_xms="512m",
                jvm_xmx="1024m",
            ),
            broker_count=3,
            deletion_protection=True,
            enable_vpc_flow_logs=True,
            enable_control_plane_logs=True,
            enable_alertmanager_sns_log_forwarder=False,
            s3_table_bucket_name="kafka-events-stg",
            s3_table_bucket_removal_policy=RemovalPolicy.RETAIN,
            s3_consumer_checkpoint_suffix="stg",
            prometheus_resources=PrometheusResourceConfig(
                memory_request="512Mi",
                memory_limit="1Gi",
                cpu_request="250m",
                cpu_limit="500m",
                storage_size="20Gi",
                retention="15d",
                scrape_interval="30s",
            ),
            alertmanager_resources=AlertmanagerResourceConfig(
                memory_request="64Mi",
                memory_limit="128Mi",
                cpu_request="50m",
                cpu_limit="100m",
                storage_size="1Gi",
            ),
            prometheus_replicas=2,
            alertmanager_replicas=3,
            fluent_bit_resources=FluentBitResourceConfig(
                memory_request="128Mi",
                memory_limit="256Mi",
                cpu_request="50m",
                cpu_limit="200m",
                mem_buf_limit="5MB",
            ),
        )

    @classmethod
    def for_prd(cls, cluster_name: str = "eks-cluster") -> ClusterConfig:
        return cls(
            cluster_name=cluster_name,
            admin_role_name="eks-cluster-admin",
            nat_gateways=3,
            system_instance_type="m8g.large",
            system_min_size=3,
            system_max_size=6,
            system_desired_size=3,
            kafka_broker_instance_type="r8g.large",
            # controller はメタデータ管理のみで負荷軽微なため broker (memory-optimized) より小型・汎用
            kafka_controller_instance_type="m8g.medium",
            nodegroup_ami_type=eks.NodegroupAmiType.AL2023_ARM_64_STANDARD,
            addon_versions=dict(_ADDON_VERSIONS_K8S_135),
            strimzi_version="1.0.0",
            strimzi_chart_repo="https://strimzi.io/charts/",
            kube_prometheus_stack_chart_version="84.5.0",
            kube_prometheus_stack_chart_repo="https://prometheus-community.github.io/helm-charts",
            aws_lbc_chart_version="3.3.0",
            aws_lbc_chart_repo="https://aws.github.io/eks-charts",
            fluent_bit_chart_version="0.57.3",
            fluent_bit_chart_repo="https://fluent.github.io/helm-charts",
            log_retention=logs.RetentionDays.ONE_MONTH,
            log_removal_policy=RemovalPolicy.RETAIN,
            enable_interface_endpoints=True,
            kafka_delete_claim=False,
            kafka_controller_count=3,
            kafka_single_az=False,
            kafka_broker_resources=KafkaPoolResourceConfig(
                memory_request="2Gi",
                memory_limit="4Gi",
                cpu_request="250m",
                cpu_limit="500m",
                storage_size="20Gi",
                jvm_xms="1024m",
                jvm_xmx="2048m",
            ),
            kafka_controller_resources=KafkaPoolResourceConfig(
                memory_request="1Gi",
                memory_limit="2Gi",
                cpu_request="250m",
                cpu_limit="500m",
                storage_size="20Gi",
                jvm_xms="512m",
                jvm_xmx="1024m",
            ),
            broker_count=3,
            deletion_protection=True,
            enable_vpc_flow_logs=True,
            enable_control_plane_logs=True,
            enable_alertmanager_sns_log_forwarder=False,
            s3_table_bucket_name="kafka-events",
            s3_table_bucket_removal_policy=RemovalPolicy.RETAIN,
            s3_consumer_checkpoint_suffix="prd",
            prometheus_resources=PrometheusResourceConfig(
                memory_request="512Mi",
                memory_limit="1Gi",
                cpu_request="250m",
                cpu_limit="500m",
                storage_size="20Gi",
                retention="15d",
                scrape_interval="30s",
            ),
            alertmanager_resources=AlertmanagerResourceConfig(
                memory_request="64Mi",
                memory_limit="128Mi",
                cpu_request="50m",
                cpu_limit="100m",
                storage_size="1Gi",
            ),
            prometheus_replicas=2,
            alertmanager_replicas=3,
            fluent_bit_resources=FluentBitResourceConfig(
                memory_request="128Mi",
                memory_limit="256Mi",
                cpu_request="50m",
                cpu_limit="200m",
                mem_buf_limit="5MB",
            ),
        )
