#!/usr/bin/env bash
# terraform test は depends_on を検証できないため、依存グラフで作成順序を固定する。
set -euo pipefail

cd "$(dirname "$0")/.."
source ../../scripts/graph.sh

# ESM のポーラーは ENI 管理権限が無いと Kafka に接続できない
expect_dependency aws_lambda_event_source_mapping.kafka aws_iam_role_policy.consumer
