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

### メトリクススタックは kube-prometheus-stack
当初 ADOT (opentelemetry-collector) を採用していたが、Strimzi 標準ダッシュボードが kube-prometheus-stack の標準ラベル（`strimzi_io_*` / `kubelet_volume_stats_*` 等）に依存していたため kube-prometheus-stack に置き換えた。Prometheus が AMP に SigV4 remote_write、AMG が AMP からクエリする構成。

### Kafka 外部接続のポート設計
`kafka-cluster.yaml` の external listener は NodePort 型。共有 NLB → NodePort → Kafka broker の経路で、bootstrap に 30094、broker 0〜2 に 30095〜30097 を使用する。advertised port（9095〜9097）はクライアントがブローカーに繋ぎ直す際のポートで NodePort とは別。`externalTrafficPolicy: Local` でクライアント送信元 IP 保持・余分なホップ排除。

### Shared NLB の ARN 固定 + AWS LBC TargetGroupBinding
NLB 本体・VPC Endpoint Service・**TargetGroup / Listener** は CDK（`NetworkConstruct`）で作成して ARN を固定する。NLB を再作成すると ARN が変わって PrivateLink が壊れるため、`NetworkConstruct` の変更は慎重に行う。Strimzi の per-broker NodePort Service とのバインドは AWS LBC の `TargetGroupBinding` CRD（`KafkaConstruct` が manifest として apply）が動的に行う。Pod 移動時にも追従。

### Strimzi の apiVersion は `kafka.strimzi.io/v1`
Strimzi 1.0.0 で `v1` が正式 API として昇格し、`v1beta2` / `v1beta1` / `v1alpha1` は廃止された。`kafka-cluster.yaml`・`node-pool-*.yaml`・`kafka-rebalance.yaml` はすべて `apiVersion: kafka.strimzi.io/v1` が正しい。`v1beta2` への変更は誤り。

### `kafka-cluster.yaml` が唯一の真実の源（broker 設定）
ブローカー数・ポート設定は `kafka-cluster.yaml` の external listener `configuration` が唯一の真実の源。`_manifest.py` の `parse_kafka_nlb_ports()` がこれをパースし、`NetworkConstruct` が NLB Listener / TargetGroup を生成、`BROKER_COUNT` を `node-pool-broker.yaml` の `<BROKER_REPLICAS>` プレースホルダーに注入する。ブローカーを増減する場合は `kafka-cluster.yaml` の `configuration.brokers` リストを編集するだけでよい。

### Controller 数・PVC 削除挙動・インスタンスタイプは config.py
controller の replicas（`kafka_controller_count`、デフォルト 3）と KafkaNodePool 削除時の PVC 削除挙動（`delete_claim`、dev=True / stg/prd=False）、ノードグループのインスタンスタイプ（broker 用 `kafka_broker_instance_type` と controller 用 `kafka_controller_instance_type`）は `ekscdk/config.py` の各 env factory で管理し、CDK 経由で YAML プレースホルダーや nodegroup 定義に注入する。

### EKS 設定は CfnCluster エスケープハッチ
`aws_eks_v2.Cluster` は `UpgradePolicy.SupportType` / `DeletionProtection` を直接プロパティ化していないため、`eks_cluster.py` で `cfn_cluster.add_property_override()` で設定する。`UpgradePolicy=STANDARD`（追加課金回避）、`DeletionProtection` は env 別（dev=False, stg/prd=True）。

### Broker と Controller は別 nodegroup に配置
EKS nodegroup を `kafka-broker-nodegroup` と `kafka-controller-nodegroup` に分離し、`node-pool-broker.yaml` / `node-pool-controller.yaml` の nodeAffinity でそれぞれ `role=kafka-broker` / `role=kafka-controller` ラベルを要求して**物理的に別ノードに配置**する。これにより (1) 1 ノード障害で broker と controller を同時に失うリスクを排除、(2) controller を broker より小型のインスタンス（`kafka_controller_instance_type`）に変更可能、の 2 点を達成する。各 nodegroup の `podAntiAffinity` は同一プール内の HA 確保（broker 同士 / controller 同士の分散）にのみ使う。

### cdk deploy はユーザーが行う
`cdk deploy` は本リポジトリの自動化対象外（ユーザーが手動で実行）。リポジトリで管理するのは synth 可能な状態までで、デプロイの判断・タイミングはユーザー側。

### 以下をベースにする
/home/vscode/workspace/data-on-eks/data-stacks/
https://awslabs.github.io/data-on-eks/docs/datastacks/streaming/kafka-on-eks/infra
https://github.com/awslabs/data-on-eks

### AWSのアカウントID等、セキュリティ情報はgitで管理しない
