#!/usr/bin/env bash
# terraform test は depends_on を検証できないため、依存グラフで作成順序を固定する。
set -euo pipefail

cd "$(dirname "$0")/.."
source ../../scripts/graph.sh

# Pod Identity の紐付け先 ServiceAccount は Namespace が無いと作れない
for sa in fluent_bit grafana alertmanager; do
  expect_dependency "kubernetes_service_account_v1.${sa}" kubernetes_manifest.namespace
done

# kube-prometheus-stack は Namespace と、紐付け先の ServiceAccount・Pod Identity が揃ってから導入する
expect_dependency helm_release.kube_prometheus_stack kubernetes_manifest.namespace
expect_dependency helm_release.kube_prometheus_stack kubernetes_service_account_v1.grafana
expect_dependency helm_release.kube_prometheus_stack kubernetes_service_account_v1.alertmanager
expect_dependency helm_release.kube_prometheus_stack module.alertmanager_identity.aws_eks_pod_identity_association.this

# Fluent Bit は紐付け先 ServiceAccount と Pod Identity が無いとクレデンシャルを取得できない
expect_dependency helm_release.fluent_bit kubernetes_service_account_v1.fluent_bit
expect_dependency helm_release.fluent_bit module.fluent_bit_identity.aws_eks_pod_identity_association.this
