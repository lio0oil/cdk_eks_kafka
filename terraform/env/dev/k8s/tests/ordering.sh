#!/usr/bin/env bash
# terraform test は depends_on を検証できないため、依存グラフでワークロードの作成順序を固定する。
set -euo pipefail

cd "$(dirname "$0")/.."
source ../../../scripts/graph.sh

# KafkaNodePool / Kafka CR の CRD は Strimzi chart が提供する
expect_dependency module.kafka.kubectl_manifest.controller_pool module.addons.helm_release.strimzi
expect_dependency module.kafka.kubectl_manifest.broker_pool module.addons.helm_release.strimzi
expect_dependency module.kafka.kubectl_manifest.kafka_cluster module.addons.helm_release.strimzi

# TargetGroupBinding の CRD と mutating webhook は AWS LBC が提供する。webhook の endpoint が
# 立ち上がる前に apply すると失敗するため、chart（wait=true）の Ready を待つ
expect_dependency module.kafka.kubectl_manifest.target_group_binding module.addons.helm_release.aws_lbc

# Prometheus / Alertmanager の PVC は gp3 StorageClass を要求する。未作成だと PVC が Pending のまま
# kube-prometheus-stack の install（wait=true）がタイムアウトする
expect_dependency module.monitoring.helm_release.kube_prometheus_stack module.addons.kubectl_manifest.gp3_storage_class

# PodMonitor の対象 Namespace（kafka / strimzi-system）と Strimzi の CRD は addons module が用意する
expect_dependency module.monitoring.kubectl_manifest.pod_monitor module.addons.kubectl_manifest.kafka_namespace
expect_dependency module.monitoring.kubectl_manifest.pod_monitor module.addons.helm_release.strimzi
expect_dependency module.monitoring.kubectl_manifest.strimzi_kube_state_metrics module.addons.helm_release.strimzi
