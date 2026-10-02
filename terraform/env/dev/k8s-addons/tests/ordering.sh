#!/usr/bin/env bash
# terraform test は depends_on を検証できないため、依存グラフでワークロードの作成順序を固定する。
set -euo pipefail

cd "$(dirname "$0")/.."
source ../../../scripts/graph.sh

# Prometheus / Alertmanager の PVC は gp3 StorageClass を要求する。未作成だと PVC が Pending のまま
# kube-prometheus-stack の install（wait=true）がタイムアウトする
expect_dependency module.monitoring.helm_release.kube_prometheus_stack module.addons.kubernetes_manifest.gp3_storage_class
