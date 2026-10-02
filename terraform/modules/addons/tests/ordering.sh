#!/usr/bin/env bash
# terraform test は depends_on を検証できないため、依存グラフで作成順序を固定する。
set -euo pipefail

cd "$(dirname "$0")/.."
source ../../scripts/graph.sh

# watchNamespaces の対象 namespace が無いと Strimzi の RoleBinding 作成で Helm install が失敗する
expect_dependency helm_release.strimzi kubectl_manifest.kafka_namespace

# Pod Identity の Role と紐付け、紐付け先の ServiceAccount が無いうちに LBC の Pod が起動すると
# クレデンシャルを取得できない
expect_dependency helm_release.aws_lbc kubernetes_service_account_v1.aws_lbc
expect_dependency helm_release.aws_lbc module.lbc_pod_identity.aws_eks_pod_identity_association.this
