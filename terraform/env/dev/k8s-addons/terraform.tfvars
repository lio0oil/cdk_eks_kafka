region      = "ap-northeast-1"
environment = "dev"

strimzi_version                     = "1.0.0"
strimzi_chart_repo                  = "https://strimzi.io/charts/"
aws_lbc_chart_version               = "3.3.0"
aws_lbc_chart_repo                  = "https://aws.github.io/eks-charts"
kube_prometheus_stack_chart_version = "84.5.0"
kube_prometheus_stack_chart_repo    = "https://prometheus-community.github.io/helm-charts"
fluent_bit_chart_version            = "0.57.3"
fluent_bit_chart_repo               = "https://fluent.github.io/helm-charts"

log_retention_days = 7
# dev は環境破棄を容易にする。stg/prd は false でロググループを残す
log_force_destroy                     = true
enable_alertmanager_sns_log_forwarder = true

prometheus_replicas   = 2
alertmanager_replicas = 3

prometheus_resources = {
  memory_request  = "512Mi"
  memory_limit    = "1Gi"
  cpu_request     = "250m"
  cpu_limit       = "500m"
  storage_size    = "20Gi"
  retention       = "15d"
  scrape_interval = "30s"
}
alertmanager_resources = {
  memory_request = "64Mi"
  memory_limit   = "128Mi"
  cpu_request    = "50m"
  cpu_limit      = "100m"
  storage_size   = "1Gi"
}
fluent_bit_resources = {
  memory_request = "128Mi"
  memory_limit   = "256Mi"
  cpu_request    = "50m"
  cpu_limit      = "200m"
  mem_buf_limit  = "5MB"
}

# Operator の replica 数・ログレベルと、Helm の Ready 待ちの上限
strimzi_replicas                           = 2
strimzi_log_level                          = "WARN"
aws_lbc_replica_count                      = 2
aws_lbc_wait_timeout_seconds               = 600
kube_prometheus_stack_wait_timeout_seconds = 900

# dev のテスト用経路（Alertmanager の通知本文を CloudWatch Logs に出す Lambda）
sns_log_forwarder_runtime         = "python3.13"
sns_log_forwarder_timeout_seconds = 30
