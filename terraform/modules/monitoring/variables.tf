variable "cluster_name" {
  type        = string
  description = "EKS クラスター名"
}

variable "region" {
  type        = string
  description = "Alertmanager の SNS 送信と Fluent Bit の CloudWatch Logs 送信に使うリージョン"
}

variable "manifest_dir" {
  type        = string
  description = "監視関連の manifests/monitoring ディレクトリ"
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

variable "log_retention_days" {
  type        = number
  description = "CloudWatch Logs の保持日数"
}

variable "retain_log_groups" {
  type        = bool
  description = "destroy 時にロググループを残すか。dev は false、stg/prd は true"
}

variable "enable_alertmanager_sns_log_forwarder" {
  type        = bool
  description = "SNS に Lambda を購読させ、通知本文を CloudWatch Logs へ出すか。実通知先を持たない dev のテスト用経路"
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
