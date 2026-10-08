mock_provider "kubernetes" {}

variables {
  manifest_dir = "../../manifests/monitoring"
}

run "strimzi_pod_monitors_scrape_kafka_and_operators" {
  command = apply

  assert {
    condition = (
      toset(keys(kubernetes_manifest.pod_monitor)) == toset(["kafka-pod-monitor", "cluster-operator-pod-monitor", "entity-operator-pod-monitor"])
      && alltrue([for m in values(kubernetes_manifest.pod_monitor) : m.manifest.kind == "PodMonitor"])
    )
    error_message = "Kafka / Cluster Operator / Entity Operator の PodMonitor 3 件になっていない"
  }
}

run "grafana_dashboards_are_picked_up_by_the_sidecar" {
  command = apply

  assert {
    condition     = length(kubernetes_manifest.grafana_dashboard) == 5
    error_message = "Grafana dashboard が 5 件（kafka / exporter / operators / cruise-control / kraft）になっていない"
  }

  assert {
    condition = alltrue([
      for d in values(kubernetes_manifest.grafana_dashboard) :
      d.manifest.kind == "ConfigMap" && d.manifest.metadata.labels["grafana_dashboard"] == "1"
    ])
    error_message = "dashboard が grafana_dashboard=1 ラベルの ConfigMap になっていない（sidecar が取り込めない）"
  }
}

run "strimzi_prometheus_rules_are_applied_unmodified" {
  command = apply

  assert {
    condition = (
      length(kubernetes_manifest.prometheus_rule) == 5
      && alltrue([for r in values(kubernetes_manifest.prometheus_rule) : r.manifest.kind == "PrometheusRule"])
    )
    error_message = "Strimzi の PrometheusRule 5 件が適用されていない"
  }
}

# ruleNamespaceSelector 未指定の Prometheus は自身の namespace（monitoring）の PrometheusRule しか
# 評価しない。namespace 未指定だと default に作られ、アラートが評価されない
run "strimzi_prometheus_rules_live_in_the_monitoring_namespace" {
  command = apply

  assert {
    condition = alltrue([
      for r in values(kubernetes_manifest.prometheus_rule) : r.manifest.metadata.namespace == "monitoring"
    ])
    error_message = "PrometheusRule が monitoring namespace に作られていない（Prometheus に評価されない）"
  }
}

# kube-prometheus-stack 同梱の kube-state-metrics は Strimzi の CRD を知らないため、
# Strimzi 公式の専用インスタンスを別途 apply して CR のステータスを監視する
run "strimzi_kube_state_metrics_has_its_own_instance" {
  command = apply

  assert {
    condition = toset([for d in values(kubernetes_manifest.strimzi_kube_state_metrics) : d.manifest.kind]) == toset([
      "ServiceAccount", "ClusterRole", "ClusterRoleBinding", "Service", "Deployment", "ServiceMonitor",
    ]) && length(kubernetes_manifest.strimzi_kube_state_metrics) == 6
    error_message = "Strimzi kube-state-metrics の 6 ドキュメントが分割して適用されていない"
  }

  assert {
    condition = (
      kubernetes_manifest.strimzi_ksm_config_map.manifest.kind == "ConfigMap"
      && kubernetes_manifest.strimzi_ksm_prometheus_rule.manifest.kind == "PrometheusRule"
    )
    error_message = "Strimzi kube-state-metrics の ConfigMap / PrometheusRule が適用されていない"
  }
}
