variable "region" {
  type        = string
  description = "デプロイ先の AWS リージョン。infra と同じ値にする"
}

variable "environment" {
  type        = string
  description = "環境名（dev / stg / prd）。default_tags に付与する"
}

variable "strimzi_version" {
  type        = string
  description = "Strimzi Operator の Helm chart バージョン"
}

variable "strimzi_chart_repo" {
  type        = string
  description = "Strimzi Operator の Helm chart リポジトリ。閉域網では内部ミラーを指定する"
}

variable "aws_lbc_chart_version" {
  type        = string
  description = "AWS Load Balancer Controller の Helm chart バージョン"
}

variable "aws_lbc_chart_repo" {
  type        = string
  description = "AWS Load Balancer Controller の Helm chart リポジトリ"
}

variable "kube_prometheus_stack_chart_version" {
  type        = string
  description = "kube-prometheus-stack の Helm chart バージョン"
}

variable "kube_prometheus_stack_chart_repo" {
  type        = string
  description = "kube-prometheus-stack の Helm chart リポジトリ"
}

variable "fluent_bit_chart_version" {
  type        = string
  description = "Fluent Bit の Helm chart バージョン"
}

variable "fluent_bit_chart_repo" {
  type        = string
  description = "Fluent Bit の Helm chart リポジトリ"
}

variable "kafka_delete_claim" {
  type        = bool
  description = "KafkaNodePool 削除時に PVC を一緒に削除するか。dev は true、stg/prd は false（データ保護）"
}

variable "kafka_broker_resources" {
  type = object({
    memory_request = string
    memory_limit   = string
    cpu_request    = string
    cpu_limit      = string
    storage_size   = string
    jvm_xms        = string
    jvm_xmx        = string
  })
  description = "broker の resources / storage / jvmOptions"
}

variable "kafka_controller_resources" {
  type = object({
    memory_request = string
    memory_limit   = string
    cpu_request    = string
    cpu_limit      = string
    storage_size   = string
    jvm_xms        = string
    jvm_xmx        = string
  })
  description = "controller の resources / storage / jvmOptions"
}

variable "kafka_topic_retention_ms" {
  type        = string
  description = "test-topic の retention.ms（ミリ秒の文字列）"
}

variable "log_retention_days" {
  type        = number
  description = "CloudWatch Logs の保持日数"
}

variable "log_force_destroy" {
  type        = bool
  description = "ログ用バケット・ロググループを destroy 時に削除可能にするか。環境を破棄する dev のみ true"
}

variable "enable_alertmanager_sns_log_forwarder" {
  type        = bool
  description = "SNS に Lambda を購読させ、通知本文を CloudWatch Logs へ出すか。dev のテスト用経路"
}

variable "prometheus_replicas" {
  type        = number
  description = "Prometheus の replica 数"
}

variable "alertmanager_replicas" {
  type        = number
  description = "Alertmanager の replica 数"
}

variable "prometheus_resources" {
  type = object({
    memory_request  = string
    memory_limit    = string
    cpu_request     = string
    cpu_limit       = string
    storage_size    = string
    retention       = string
    scrape_interval = string
  })
  description = "Prometheus の resources / storage / retention / scrapeInterval"
}

variable "alertmanager_resources" {
  type = object({
    memory_request = string
    memory_limit   = string
    cpu_request    = string
    cpu_limit      = string
    storage_size   = string
  })
  description = "Alertmanager の resources / storage"
}

variable "fluent_bit_resources" {
  type = object({
    memory_request = string
    memory_limit   = string
    cpu_request    = string
    cpu_limit      = string
    mem_buf_limit  = string
  })
  description = "Fluent Bit の resources / INPUT バッファ上限"
}

variable "strimzi_replicas" {
  type        = number
  description = "Strimzi Operator の replica 数。2 以上なら leader election で冗長化される"
}

variable "strimzi_log_level" {
  type        = string
  description = "Strimzi Operator のログレベル"
}

variable "aws_lbc_replica_count" {
  type        = number
  description = "AWS Load Balancer Controller の replica 数"
}

variable "aws_lbc_wait_timeout_seconds" {
  type        = number
  description = "AWS LBC の Pod が Ready になるまで待つ上限（秒）"
}

variable "kube_prometheus_stack_wait_timeout_seconds" {
  type        = number
  description = "kube-prometheus-stack の Pod が Ready になるまで待つ上限（秒）"
}

variable "sns_log_forwarder_runtime" {
  type        = string
  description = "SNS 通知本文を CloudWatch Logs に出す Lambda の runtime（dev のテスト用経路）"
}

variable "sns_log_forwarder_timeout_seconds" {
  type        = number
  description = "SNS 通知本文を CloudWatch Logs に出す Lambda の timeout（秒）"
}
