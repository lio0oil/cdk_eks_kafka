#!/usr/bin/env bash
# terraform test は depends_on を検証できないため、依存グラフで作成順序を固定する。
set -euo pipefail

cd "$(dirname "$0")/.."
source ../../scripts/graph.sh

# ESM のポーラーは ENI 管理権限が無いと Kafka に接続できない
expect_dependency aws_lambda_event_source_mapping.kafka aws_iam_role_policy.writer

# Lambda が先にロググループを自動作成すると、Terraform の作成が失敗する
expect_dependency aws_lambda_function.writer aws_cloudwatch_log_group.writer

# 権限が無いうちに起動すると、最初の実行が AccessDenied で失敗する
expect_dependency aws_scheduler_schedule.merge aws_iam_role_policy.scheduler
