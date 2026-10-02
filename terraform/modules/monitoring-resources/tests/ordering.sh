#!/usr/bin/env bash
# terraform test は depends_on を検証できないため、依存グラフで作成順序を固定する。
set -euo pipefail

cd "$(dirname "$0")/.."
source ../../scripts/graph.sh

# kube-state-metrics の Deployment は設定の ConfigMap を参照する
expect_dependency kubernetes_manifest.strimzi_kube_state_metrics kubernetes_manifest.strimzi_ksm_config_map
