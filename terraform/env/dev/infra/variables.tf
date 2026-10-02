variable "region" {
  type        = string
  description = "デプロイ先の AWS リージョン"
}

variable "environment" {
  type        = string
  description = "環境名（dev / stg / prd）。default_tags に付与する"
}

variable "cluster_name" {
  type        = string
  description = "EKS クラスター名"
}

variable "kubernetes_version" {
  type        = string
  description = "Kubernetes のバージョン"
}

variable "admin_role_name" {
  type        = string
  description = "cluster-admin 相当の IAM Role 名"
}

variable "console_role_arns" {
  type        = list(string)
  description = "管理者として追加登録する IAM Role の ARN（SSO など）"
}

variable "nat_gateways" {
  type        = number
  description = "NAT Gateway の台数（1 または 3）"
}

variable "kafka_single_az" {
  type        = bool
  description = "Kafka 系の nodegroup と NLB を 1 AZ 目に固定するか"
}

variable "enable_interface_endpoints" {
  type        = bool
  description = "ECR / CloudWatch Logs / eks-auth の Interface VPC Endpoint を作るか"
}

variable "enable_vpc_flow_logs" {
  type        = bool
  description = "VPC Flow Logs を S3 に保存するか"
}

variable "log_force_destroy" {
  type        = bool
  description = "ログ用バケットを中身ごと削除可能にするか"
}

variable "deletion_protection" {
  type        = bool
  description = "EKS クラスターの削除保護"
}

variable "enable_control_plane_logs" {
  type        = bool
  description = "EKS コントロールプレーンログを CloudWatch Logs に送るか"
}

variable "addon_versions" {
  type        = map(string)
  description = "Managed Addon のバージョン"
}

variable "nodegroup_ami_type" {
  type        = string
  description = "nodegroup の AMI タイプ"
}

variable "system_node_group" {
  type = object({
    instance_type = string
    min_size      = number
    max_size      = number
    desired_size  = number
  })
  description = "system nodegroup の instance type とサイズ"
}

variable "broker_count" {
  type        = number
  description = "Kafka broker の replica 数。NLB の port 採番と broker nodegroup の容量の単一の真実の源"
}

variable "kafka_broker_instance_type" {
  type        = string
  description = "Kafka broker nodegroup の instance type"
}

variable "kafka_controller_instance_type" {
  type        = string
  description = "Kafka controller nodegroup の instance type"
}

variable "kafka_controller_count" {
  type        = number
  description = "KRaft controller の replica 数（奇数推奨、通常 3）"
}

variable "vpc_cidr" {
  type        = string
  description = "VPC の CIDR"
}

variable "public_subnet_cidrs" {
  type        = list(string)
  description = "public subnet の CIDR。AZ ごとに 1 つ"
}

variable "private_subnet_cidrs" {
  type        = list(string)
  description = "private subnet の CIDR。AZ ごとに 1 つ。この数が使う AZ の数になる"
}

variable "interface_endpoint_services" {
  type        = list(string)
  description = "enable_interface_endpoints=true のとき作る Interface VPC Endpoint のサービス名。機微データやクレデンシャルを運ぶ API に限る"
}

variable "kafka_nlb_name" {
  type        = string
  description = "Kafka 共有 NLB の名前。TargetGroup 名もここから導出する。同じアカウントの複数環境では環境ごとに変える"
}

variable "kafka_target_group_health_check" {
  type = object({
    interval            = number
    healthy_threshold   = number
    unhealthy_threshold = number
  })
  description = "NLB の TargetGroup の TCP health check"
}

variable "eks_endpoint_public_access" {
  type        = bool
  description = "EKS の API endpoint を public network から到達可能にするか"
}

variable "eks_endpoint_private_access" {
  type        = bool
  description = "EKS の API endpoint を VPC 内から到達可能にするか"
}

variable "eks_bootstrap_cluster_creator_admin_permissions" {
  type        = bool
  description = "クラスターを作成した IAM identity を自動で cluster-admin にするか。false なら Access Entry 経由のみ"
}

variable "eks_control_plane_log_types" {
  type        = list(string)
  description = "enable_control_plane_logs=true のとき CloudWatch Logs に送るコントロールプレーンログの種別"
}

variable "eks_upgrade_support_type" {
  type        = string
  description = "Kubernetes バージョンのサポート種別。STANDARD は無償、EXTENDED は追加課金"
}

variable "nodegroup_capacity_type" {
  type        = string
  description = "nodegroup の購入オプション（ON_DEMAND / SPOT）"
}

variable "test_ec2_instance_type" {
  type        = string
  description = "検証用踏み台の instance type"
}
