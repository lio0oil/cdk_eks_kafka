# Prometheus Operator が提供する CRD（PodMonitor / ServiceMonitor / PrometheusRule）に依存する manifest。
# kubernetes_manifest は plan 時に CRD の OpenAPI スキーマを要求するため、CRD を作る Helm
# （kube-prometheus-stack・Strimzi）とは別の root（後に適用する層）で使う。公式 examples/metrics/ の
# アラート名・閾値・メトリクス定義は編集せず、稼働コンポーネント分のみを採用している。

locals {
  pod_monitor_files = { for f in fileset(var.manifest_dir, "prometheus-install/pod-monitors/*.yaml") : trimsuffix(basename(f), ".yaml") => f }
  dashboard_files   = { for f in fileset(var.manifest_dir, "grafana-dashboards/*.yaml") : trimsuffix(basename(f), ".yaml") => f }
  rule_files        = { for f in fileset(var.manifest_dir, "prometheus-install/prometheus-rules/*.yaml") : trimsuffix(basename(f), ".yaml") => f }

  # ksm.yaml は `---` 区切りの複数ドキュメント。先頭の `---` も拾えるよう改行を前置してから分割する
  ksm_documents = {
    for d in [for doc in split("\n---\n", "\n${file("${var.manifest_dir}/kube-state-metrics/ksm.yaml")}") : doc if trimspace(doc) != ""] :
    "${yamldecode(d).kind}-${yamldecode(d).metadata.name}" => d
  }
}

# broker / controller / cruise-control / kafka-exporter を kafka と entity-operator、cluster-operator の
# PodMonitor で収集する。strimzi-system namespace は Strimzi chart が作る。
resource "kubernetes_manifest" "pod_monitor" {
  for_each = local.pod_monitor_files

  manifest = yamldecode(file("${var.manifest_dir}/${each.value}"))
}

# kube-prometheus-stack の sidecar が monitoring namespace の grafana_dashboard=1 ラベルの ConfigMap を
# 自動で取り込む。
resource "kubernetes_manifest" "grafana_dashboard" {
  for_each = local.dashboard_files

  manifest = yamldecode(file("${var.manifest_dir}/${each.value}"))
}

resource "kubernetes_manifest" "prometheus_rule" {
  for_each = local.rule_files

  manifest = yamldecode(file("${var.manifest_dir}/${each.value}"))
}

# kube-prometheus-stack 同梱の kube-state-metrics は Strimzi の CRD を知らないため、Strimzi 公式の専用
# インスタンスを別途 apply する。Kafka / KafkaTopic / KafkaUser 等の .status.conditions を
# メトリクス化し、対応する PrometheusRule でアラートする。ClusterRole が Strimzi の CRD を参照する。
resource "kubernetes_manifest" "strimzi_ksm_config_map" {
  manifest = yamldecode(file("${var.manifest_dir}/kube-state-metrics/configmap.yaml"))
}

resource "kubernetes_manifest" "strimzi_kube_state_metrics" {
  for_each = local.ksm_documents

  manifest   = yamldecode(each.value)
  depends_on = [kubernetes_manifest.strimzi_ksm_config_map]
}

resource "kubernetes_manifest" "strimzi_ksm_prometheus_rule" {
  manifest = yamldecode(file("${var.manifest_dir}/kube-state-metrics/prometheus-rules.yaml"))
}
