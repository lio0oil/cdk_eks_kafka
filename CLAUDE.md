# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## コマンド

```bash
# 依存関係インストール
uv sync

# CDK synth（CloudFormation テンプレート生成）
cdk synth -c repo-url=https://github.com/example/ekscdk

# デプロイ
cdk deploy IamStack
cdk deploy EksCdkStack -c repo-url=<GitリポジトリURL>

# テスト実行
uv run pytest

# 単一テスト実行
uv run pytest tests/unit/test_ekscdk_stack.py::test_stack_synthesizes
```

## アーキテクチャ

2つの CDK スタックで構成される。

**`IamStack`** — EKS cluster-admin ロール（`eks-cluster-admin`）のみを作成。`EksCdkStack` より先にデプロイする。

**`EksCdkStack`** — 以下の4つの Construct で構成される。

```
EksCdkStack
├── NetworkConstruct     VPC / Kafka 共有 NLB / VPC Endpoint Service
├── EksClusterConstruct  EKS 1.35 / system ノードグループ(m8g.large) / kafka ノードグループ(r8g.large)
├── AddonsConstruct      EKS アドオン / ArgoCD / Strimzi Operator / ACK(EC2・ELBv2)
└── MonitoringConstruct  AMP / AMG / ADOT / Fluent Bit / IRSA
```

`MonitoringConstruct` は `AddonsConstruct`（ArgoCD）の完了後に動作する依存関係は現在設定されていない。

## CDK と ArgoCD の使い分け

- **CDK 管理**: インフラ・監視スタック全体。AMP エンドポイントや CloudWatch Log Group 名など CloudFormation トークン（デプロイ後に確定する動的値）を設定に注入する必要があるため。
- **ArgoCD 管理（`manifests/kafka/`）**: Kafka ブローカー設定・パーティション・Rebalance・PrivateLink。運用中に変更頻度が高く、CDK 再デプロイなしに git push だけで反映したいため。

## Kafka 外部接続（PrivateLink）

Shared NLB の本体は CDK（`NetworkConstruct`）で作成して ARN を固定化する。NLB のリスナーとターゲットグループは ACK（ELBv2 Controller）が `manifests/kafka/privatelink.yaml` を通じて管理する。デプロイ後に出力される `VpcId` と `KafkaSharedNlbArn` を `privatelink.yaml` に書き込んで push する。

## 必須コンテキスト

| キー | 説明 |
|---|---|
| `repo-url` | ArgoCD が `manifests/kafka/` を同期するための Git リポジトリ URL |
| `amg-auth-provider` | AMG 認証方式（デフォルト: `AWS_SSO`、代替: `SAML`） |
