variable "cluster_name" {
  type        = string
  description = "リソース名の接頭辞に使うクラスター名"
}

variable "vpc_id" {
  type        = string
  description = "ESM ポーラー ENI の SG を作る VPC"
}

variable "kafka_nlb_security_group_id" {
  type        = string
  description = "Kafka 共有 NLB の SG。ESM からの送信先をこの SG に限定する"
}

variable "force_destroy" {
  type        = bool
  description = "バケットを中身ごと削除可能にするか。環境を破棄する dev のみ true"
}
