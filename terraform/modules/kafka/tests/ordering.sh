#!/usr/bin/env bash
# terraform test は depends_on を検証できないため、依存グラフで作成順序を固定する。
set -euo pipefail

cd "$(dirname "$0")/.."
source ../../scripts/graph.sh

# Kafka CR は JMX メトリクス ConfigMap と KafkaNodePool を参照するため、これらの後に作る
expect_dependency kubectl_manifest.kafka_cluster kubectl_manifest.kafka_metrics
expect_dependency kubectl_manifest.kafka_cluster kubectl_manifest.cruise_control_metrics
expect_dependency kubectl_manifest.kafka_cluster kubectl_manifest.controller_pool
expect_dependency kubectl_manifest.kafka_cluster kubectl_manifest.broker_pool

# TargetGroupBinding の serviceRef 先 Service と KafkaTopic の実体は Kafka CR から生成される
expect_dependency kubectl_manifest.target_group_binding kubectl_manifest.kafka_cluster
expect_dependency kubectl_manifest.test_topic kubectl_manifest.kafka_cluster
