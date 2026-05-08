# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## コマンド

```bash
uv sync                      # 依存関係インストール
uv run pytest                # テスト実行
uv run pytest tests/unit/test_ekscdk_stack.py::test_stack_synthesizes  # 単一テスト
cdk synth                                                              # synth
```

## 非自明な設計判断

### monitoring Namespace は CDK が管理する
`MonitoringConstruct` 内の `monitoring` Namespace は `manifests/` ではなく `cluster.add_manifest()` で CDK が直接管理している。IRSA の `add_service_account()` は kubectl apply 時に Namespace が存在していないと失敗するため、ArgoCD に任せると順序が保証できない。

### ADOT RBAC も CDK 管理（`monitoring.py`）
`AdotClusterRole` / `AdotClusterRoleBinding` は `manifests/monitoring/` ではなく `monitoring.py` 内の `add_manifest()` で管理している。ADOT Helm chart の `node.add_dependency()` で依存関係を CDK 内で完結させるため。

### ADOT はノードローカルフィルターで重複スクレイプを防ぐ
ADOT は DaemonSet（ノード 1 台に 1 Pod）で動くため、`kubernetes_sd_configs` でKafka Pod を全台が発見すると N×M の重複が発生する。`${K8S_NODE_NAME}` 環境変数（`spec.nodeName` を `fieldRef` で注入）を relabel_config の regex に使い、各 ADOT が自ノード上の Pod だけをスクレイプする。

### Kafka 外部接続のポート設計
`kafka-cluster.yaml` の external listener は NodePort 型。共有 NLB → NodePort → Kafka broker の経路で、bootstrap に 30094、broker 0〜2 に 30095〜30097 を使用する。advertised port（9095〜9097）はクライアントがブローカーに繋ぎ直す際のポートで NodePort とは別。

### Shared NLB の ARN 固定
NLB 本体と VPC Endpoint Service は CDK（`NetworkConstruct`）で作成して ARN を固定する。リスナーとターゲットグループは `KafkaConstruct` が CDK ELBv2 で直接管理する。NLB を再作成すると ARN が変わって PrivateLink が壊れるため、`NetworkConstruct` の変更は慎重に行う。

### `manifests/kafka/` は CDK がロードする静的 YAML
`kafka.py` の `_load()` が `manifests/kafka/` の YAML をディスクからロードして `cluster.add_manifest()` に渡す。NLB Listener / TargetGroup は CDK ELBv2 構造体（`_BROKER_PORTS` リスト）で管理するため、YAML ではなく Python コードを編集する。
