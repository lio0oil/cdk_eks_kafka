variable "manifest_dir" {
  type        = string
  description = "kafka-cluster.yaml を含むディレクトリ"
}

variable "broker_count" {
  type        = number
  description = "Kafka broker の replica 数。NLB の port 採番と broker 設定の単一の真実の源"

  validation {
    condition     = var.broker_count >= 0
    error_message = "broker_count は 0 以上を指定してください。"
  }
}

variable "advertised_host" {
  type        = string
  description = "各 broker の advertisedHost に設定するホスト名"
}
