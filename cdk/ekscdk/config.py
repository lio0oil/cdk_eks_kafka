from __future__ import annotations

from dataclasses import dataclass

from aws_cdk import RemovalPolicy
from aws_cdk import aws_eks_v2 as eks
from aws_cdk import aws_logs as logs

# Kubernetes 1.35 向けアドオン最新バージョン（2026-05 時点）
# 更新コマンド:
#   for addon in vpc-cni coredns kube-proxy aws-ebs-csi-driver \
#                metrics-server eks-node-monitoring-agent eks-pod-identity-agent; do
#     echo -n "$addon: "
#     aws eks describe-addon-versions --addon-name "$addon" \
#       --kubernetes-version 1.35 \
#       --query 'addons[0].addonVersions[0].addonVersion' --output text
#   done
#
# `eks-pod-identity-agent` は aws_eks_v2.Cluster が `IdentityType.POD_IDENTITY` の
# SA を作る際に内部で自動追加する Addon。デフォルトでは AddonVersion が未指定で
# latest 追従になるため、他 addon と整合性を取って明示 pin し、CDK が作成済みの
# Construct に CFN プロパティ override で AddonVersion を注入する。
_ADDON_VERSIONS_K8S_135: dict[str, str] = {
    "vpc-cni": "v1.21.1-eksbuild.8",
    "coredns": "v1.14.2-eksbuild.4",
    "kube-proxy": "v1.35.3-eksbuild.5",
    "aws-ebs-csi-driver": "v1.59.0-eksbuild.1",
    "metrics-server": "v0.8.1-eksbuild.6",
    "eks-node-monitoring-agent": "v1.6.4-eksbuild.1",
    "eks-pod-identity-agent": "v1.3.10-eksbuild.3",
}


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
    delete_claim: bool
    # KRaft Controller の replica 数（KRaft は奇数推奨、通常 3）
    # node-pool-controller.yaml の replicas と kafka nodegroup サイズの両方に反映
    kafka_controller_count: int
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
    # S3 Tables の table-bucket 名 (アカウント内ユニーク・3-63 文字 lowercase/numbers/hyphens)。
    # consumer (kafka/consumer) の Iceberg 書き込み先。
    s3_table_bucket_name: str
    # S3 Tables の table-bucket を CDK destroy 時に残すか削除するか。
    # dev は DESTROY (環境破棄を容易に)、stg/prd は RETAIN (データ保護)。
    s3_table_bucket_removal_policy: RemovalPolicy
    # consumer (Spark Structured Streaming) の checkpointLocation 用 S3 バケット名 suffix。
    # 実名は `kafka-consumer-checkpoint-{account}-{suffix}` (アカウント+環境でグローバル衝突を回避)。
    s3_consumer_checkpoint_suffix: str
    # external-snapshotter のリリースタグ。
    # manifests/snapshotter/{crds.yaml, controller.yaml} は以下で再生成する:
    #   VER=v8.5.0
    #   BASE=https://github.com/kubernetes-csi/external-snapshotter
    #   kubectl kustomize "$BASE/client/config/crd?ref=$VER" > manifests/snapshotter/crds.yaml
    #   kubectl kustomize "$BASE/deploy/kubernetes/snapshot-controller?ref=$VER" \
    #     > manifests/snapshotter/controller.yaml
    external_snapshotter_version: str

    @classmethod
    def for_dev(cls, cluster_name: str = "eks-cluster-dev") -> ClusterConfig:
        return cls(
            cluster_name=cluster_name,
            admin_role_name=f"eks-cluster-admin-{cluster_name}",
            nat_gateways=1,
            # dev はテスト用途のためコスト最適化（Graviton2 burstable t4g）
            # system は監視 / Operator / アドオンのみで実使用 ~1.5GB/ノード のため
            # t4g.medium (2vCPU/4GB) で十分（allocatable 3.2GB に対し約 50% 余裕）
            system_instance_type="t4g.medium",
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
            delete_claim=True,
            kafka_controller_count=3,
            broker_count=3,
            deletion_protection=False,
            enable_vpc_flow_logs=False,
            enable_control_plane_logs=False,
            s3_table_bucket_name="kafka-events-dev",
            s3_table_bucket_removal_policy=RemovalPolicy.DESTROY,
            s3_consumer_checkpoint_suffix="dev",
            external_snapshotter_version="v8.5.0",
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
            delete_claim=False,
            kafka_controller_count=3,
            broker_count=3,
            deletion_protection=True,
            enable_vpc_flow_logs=True,
            enable_control_plane_logs=True,
            s3_table_bucket_name="kafka-events-stg",
            s3_table_bucket_removal_policy=RemovalPolicy.RETAIN,
            s3_consumer_checkpoint_suffix="stg",
            external_snapshotter_version="v8.5.0",
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
            delete_claim=False,
            kafka_controller_count=3,
            broker_count=3,
            deletion_protection=True,
            enable_vpc_flow_logs=True,
            enable_control_plane_logs=True,
            s3_table_bucket_name="kafka-events",
            s3_table_bucket_removal_policy=RemovalPolicy.RETAIN,
            s3_consumer_checkpoint_suffix="prd",
            external_snapshotter_version="v8.5.0",
        )
