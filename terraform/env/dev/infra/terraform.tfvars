region      = "ap-northeast-1"
environment = "dev"

cluster_name       = "eks-cluster-dev"
kubernetes_version = "1.35"
admin_role_name    = "eks-cluster-admin-eks-cluster-dev"

# 管理者として追加登録する IAM Role（SSO など）。CDK の `-c console-role-arns` に相当
console_role_arns = []

enable_interface_endpoints = false
enable_vpc_flow_logs       = false
log_force_destroy          = true

deletion_protection       = false
enable_control_plane_logs = false

# Kubernetes 1.35 向けアドオンの最新バージョン。更新コマンドは cdk/ekscdk/config.py を参照
# NOTE: vpc-cni / coredns / kube-proxy は暫定値。デプロイ前に実際の最新バージョンで置き換えること
addon_versions = {
  "aws-ebs-csi-driver"        = "v1.59.0-eksbuild.1"
  "metrics-server"            = "v0.8.1-eksbuild.6"
  "eks-node-monitoring-agent" = "v1.6.4-eksbuild.1"
  "snapshot-controller"       = "v8.5.0-eksbuild.3"
  "vpc-cni"                   = "v1.20.1-eksbuild.1"
  "coredns"                   = "v1.12.4-eksbuild.1"
  "kube-proxy"                = "v1.35.0-eksbuild.1"
}

nodegroup_ami_type = "AL2023_ARM_64_STANDARD"

# dev はテスト用途のためコスト最適化（Graviton2 burstable t4g）。ボトルネックは memory ではなく
# pod 数: system には監視 HA と Strimzi operator 群、全ノード共通 DaemonSet が乗り steady で
# 約 12-13 pod/node になる。nodegroup が固定サイズのため、3 ノード中 1 台喪失で残り 2 ノードに
# 寄ると max-pods 17 の t4g.medium では収まらず Pending が発生する。max-pods 35 の t4g.large にして
# 1 ノード喪失時の集中に耐える headroom を確保する。
system_node_group = {
  instance_type = "t4g.large"
  min_size      = 2
  max_size      = 4
  desired_size  = 3
}

broker_count = 3
# broker は JVM heap 2GB + page cache 用に余裕が必要なため t4g.large を維持する
# （t4g.medium にすると page cache が枯渇し dev でも本番と挙動が乖離する）
kafka_broker_instance_type = "t4g.large"
# controller はメタデータ管理のみで負荷軽微なため broker より小型インスタンス
kafka_controller_instance_type = "t4g.medium"
kafka_controller_count         = 3

# 既存の VPC と subnet の ID（Terraform の管理外）。下の値は仮のもので、適用前に VPC の管理側から受け取った値へ置き換える。
# NAT Gateway・ルートテーブル・S3 Gateway Endpoint も VPC の管理側が用意する。
vpc_id = "vpc-00000000000000000"
# EKS のコントロールプレーン用の private subnet。2 つ以上の AZ にまたがること
private_subnet_ids = ["subnet-0aaaaaaaaaaaaaaaa", "subnet-0bbbbbbbbbbbbbbbb", "subnet-0cccccccccccccccc"]
# Kafka・system のノード、NLB、ESM を置く subnet。上の private_subnet_ids の中から 1 つ選ぶ
node_subnet_id = "subnet-0aaaaaaaaaaaaaaaa"

# 機微データやクレデンシャルを運ぶ API だけを PrivateLink に固定する（enable_interface_endpoints=true のとき）
interface_endpoint_services = ["ecr.api", "ecr.dkr", "logs", "eks-auth"]

# NLB と TargetGroup の名前はリージョン内で一意が必要なため、環境ごとに変える。
# NLB を再作成すると ARN が変わり PrivateLink が壊れるため、変更は慎重に行う
kafka_nlb_name = "kafka-shared-nlb-dev"
kafka_target_group_health_check = {
  interval            = 10
  healthy_threshold   = 2
  unhealthy_threshold = 2
}

# EKS。stg/prd では endpoint を private のみ・作成者への admin 付与なしにすることを検討する（.claude/TODO.md 参照）
eks_endpoint_public_access                      = true
eks_endpoint_private_access                     = true
eks_bootstrap_cluster_creator_admin_permissions = true
# controllerManager / scheduler はコスト対監査価値が低いため有効にしない
eks_control_plane_log_types = ["audit", "api", "authenticator"]
# STANDARD は無償・約 14 か月。EXTENDED は追加課金
eks_upgrade_support_type = "STANDARD"
nodegroup_capacity_type  = "ON_DEMAND"

test_ec2_instance_type = "t4g.small"
