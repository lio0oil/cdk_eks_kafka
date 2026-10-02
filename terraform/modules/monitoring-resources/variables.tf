variable "manifest_dir" {
  type        = string
  description = "Strimzi の監視設定（PodMonitor / dashboard / PrometheusRule / kube-state-metrics）の manifests/monitoring ディレクトリ"
}
