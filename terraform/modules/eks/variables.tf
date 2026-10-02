variable "cluster_name" {
  type        = string
  description = "EKS クラスター名"
}

variable "kubernetes_version" {
  type        = string
  description = "Kubernetes のバージョン（例: 1.35）"
}

variable "subnet_ids" {
  type        = list(string)
  description = "コントロールプレーンの ENI を置く private subnet。複数 AZ が必須"
}

variable "nodegroup_subnet_ids" {
  type        = list(string)
  description = "nodegroup を置く subnet。kafka_single_az=true の環境では 1 AZ 目だけを渡す"
}

variable "admin_role_arn" {
  type        = string
  description = "cluster-admin 相当の権限を与える IAM Role の ARN"
}

variable "console_role_arns" {
  type        = list(string)
  description = "管理者として追加登録する IAM Role の ARN（SSO など）"
}

variable "deletion_protection" {
  type        = bool
  description = "クラスターの削除保護。有効だと delete-cluster が拒否される"
}

variable "enable_control_plane_logs" {
  type        = bool
  description = "audit / api / authenticator のコントロールプレーンログを CloudWatch Logs に送るか"
}

variable "addon_versions" {
  type        = map(string)
  description = "Managed Addon のバージョン。eks-pod-identity-agent は EKS の既定に追従させるため含めない"

  validation {
    condition = alltrue([
      for name in ["vpc-cni", "kube-proxy", "coredns", "aws-ebs-csi-driver", "metrics-server", "eks-node-monitoring-agent", "snapshot-controller"] :
      contains(keys(var.addon_versions), name)
    ])
    error_message = "addon_versions に vpc-cni / kube-proxy / coredns / aws-ebs-csi-driver / metrics-server / eks-node-monitoring-agent / snapshot-controller のバージョンを指定してください。"
  }
}

variable "nodegroup_ami_type" {
  type        = string
  description = "nodegroup の AMI タイプ（例: AL2023_ARM_64_STANDARD）"
}

variable "system_node_group" {
  type = object({
    instance_type = string
    min_size      = number
    max_size      = number
    desired_size  = number
  })
  description = "監視 / Operator / アドオン用 nodegroup。taint は付けない"
}

variable "kafka_broker_node_group" {
  type = object({
    instance_type = string
    count         = number
  })
  description = "Kafka broker 用 nodegroup。min=desired=count、max=count+1"
}

variable "kafka_controller_node_group" {
  type = object({
    instance_type = string
    count         = number
  })
  description = "Kafka controller 用 nodegroup。min=desired=count、max=count+1"
}

variable "endpoint_public_access" {
  type        = bool
  description = "API endpoint を public network から到達可能にするか"
}

variable "endpoint_private_access" {
  type        = bool
  description = "API endpoint を VPC 内から到達可能にするか"
}

variable "bootstrap_cluster_creator_admin_permissions" {
  type        = bool
  description = "クラスターを作成した IAM identity を自動で cluster-admin にするか。false なら Access Entry 経由のみ"
}

variable "control_plane_log_types" {
  type        = list(string)
  description = "enable_control_plane_logs=true のとき CloudWatch Logs に送るコントロールプレーンログの種別"
}

variable "upgrade_support_type" {
  type        = string
  description = "Kubernetes バージョンのサポート種別。STANDARD は無償、EXTENDED は追加課金"
}

variable "nodegroup_capacity_type" {
  type        = string
  description = "nodegroup の購入オプション（ON_DEMAND / SPOT）"
}
