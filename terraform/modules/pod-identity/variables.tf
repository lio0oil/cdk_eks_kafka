variable "cluster_name" {
  type        = string
  description = "EKS クラスター名"
}

variable "role_name" {
  type        = string
  description = "ワークロードが引き受ける IAM Role の名前"
}

variable "namespace" {
  type        = string
  description = "ServiceAccount の namespace"
}

variable "service_account" {
  type        = string
  description = "Role を紐付ける ServiceAccount 名"
}

variable "policy_statements" {
  type        = any
  description = "Role に付与するインラインポリシーの Statement 一覧。空なら ポリシーを付与しない（AWS API を叩かないワークロード用）"
  default     = []
}
