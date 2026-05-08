# EKS CDK

AWS CDK（Python）で EKS クラスター上に Kafka 基盤を構築するプロジェクト。

## アーキテクチャ

```
┌─────────────────────────────────────────────────────────────┐
│ VPC (10.0.0.0/16 / 3 AZ)                                   │
│                                                             │
│  ┌─────────────────────────────────────────────────────┐   │
│  │ EKS 1.35                                            │   │
│  │                                                     │   │
│  │  system-nodegroup (m8g.large x3)                   │   │
│  │    ArgoCD / Strimzi Operator / ACK / CoreDNS        │   │
│  │                                                     │   │
│  │  kafka-nodegroup (r8g.large x3)                    │   │
│  │    Kafka broker x3 + controller x3 (KRaft)         │   │
│  │    ADOT DaemonSet / Fluent Bit DaemonSet           │   │
│  └─────────────────────────────────────────────────────┘   │
│                                                             │
│  Kafka Shared NLB ──▶ VPC Endpoint Service (PrivateLink)   │
└─────────────────────────────────────────────────────────────┘
         │ メトリクス              │ ログ
         ▼                        ▼
      AMP + AMG              CloudWatch Logs
```

## 技術スタック

| 項目 | 採用技術 |
|---|---|
| IaC | AWS CDK v2（Python）/ `aws_eks_v2` |
| Kubernetes | 1.35 / Managed NodeGroup / Node Auto Repair |
| GitOps | ArgoCD 7.8.23 |
| Kafka | Strimzi 0.45.0 / Kafka 3.9.0（KRaft）/ broker x3 + controller x3 |
| 外部接続 | Shared NLB + PrivateLink（VPC Endpoint Service） |
| ストレージ | EBS gp3（EBS CSI Driver） |
| メトリクス | ADOT v0.40.0 → AMP + CloudWatch |
| ログ | Fluent Bit → CloudWatch Logs |
| トレース | ADOT → X-Ray |

## スタック構成

### `IamStack`

EKS cluster-admin ロール（`eks-cluster-admin`）を作成する。`EksCdkStack` より先にデプロイする。

複数の運用者・CI/CD ロールに cluster-admin 権限を付与する場合は `ekscdk/iam_stack.py` の `assumed_by` を `CompositePrincipal` で列挙する。

### `EksCdkStack`

| Construct | 管理対象 |
|---|---|
| `NetworkConstruct` | VPC / Kafka 共有 NLB / VPC Endpoint Service |
| `EksClusterConstruct` | EKS クラスター / system・kafka ノードグループ |
| `AddonsConstruct` | EKS アドオン / ArgoCD / Strimzi Operator / ACK（EC2・ELBv2） |
| `MonitoringConstruct` | AMP / AMG / ADOT / Fluent Bit / IRSA |

## GitOps 設計

```
CDK 管理（変更時は cdk deploy）
  ├─ インフラ全体（VPC / EKS / ノードグループ）
  ├─ 監視スタック（ADOT / Fluent Bit / AMP / AMG）
  └─ ArgoCD Application 定義

ArgoCD 管理（git push で自動反映）
  └─ manifests/kafka/   ← ブローカー設定 / パーティション / Rebalance / PrivateLink
```

Kafka の設定変更は `manifests/kafka/` を編集して push するだけで ArgoCD が自動反映する。

## ディレクトリ構成

```
ekscdk/
├── constructs/
│   ├── network.py        # VPC / Shared NLB / Endpoint Service
│   ├── eks_cluster.py    # EKS クラスター / ノードグループ
│   ├── addons.py         # EKS アドオン / ArgoCD / Strimzi / ACK
│   └── monitoring.py     # AMP / AMG / ADOT / Fluent Bit / IRSA
├── iam_stack.py          # IamStack
└── ekscdk_stack.py       # EksCdkStack

manifests/
├── kafka/
│   ├── kafka-cluster.yaml        # Kafka CR（ブローカー設定）
│   ├── node-pool-broker.yaml     # KafkaNodePool（broker）
│   ├── node-pool-controller.yaml # KafkaNodePool（controller）
│   ├── cm.yaml                   # JMX メトリクス設定 ConfigMap
│   ├── kafka-rebalance.yaml      # Cruise Control リバランス定義
│   └── privatelink.yaml          # NLB リスナー / TargetGroup（ACK 管理）
└── monitoring/
    └── dashboards/               # AMG インポート用 Grafana ダッシュボード JSON
```

## デプロイ手順

### 前提条件

- AWS CLI（認証済み）
- Python 3.x / AWS CDK v2
- このリポジトリを push 済みの Git リポジトリ

```bash
pip install -r requirements.txt
export CDK_DEFAULT_ACCOUNT=<AWSアカウントID>
export CDK_DEFAULT_REGION=ap-northeast-1
```

### 1. IAM ロール作成

```bash
cdk deploy IamStack
```

### 2. インフラ全体デプロイ

```bash
cdk deploy EksCdkStack -c repo-url=<GitリポジトリURL>
```

| コンテキストキー | デフォルト | 説明 |
|---|---|---|
| `repo-url` | 必須 | ArgoCD が参照する Git リポジトリ URL |
| `amg-auth-provider` | `AWS_SSO` | AMG 認証方式（IAM Identity Center 未使用の場合は `SAML`） |

### 3. PrivateLink 設定

デプロイ出力から値を取得して `manifests/kafka/privatelink.yaml` を更新する。

```
Outputs:
  EksCdkStack.VpcId             = vpc-xxxxxxxxxxxxxxxxx
  EksCdkStack.KafkaSharedNlbArn = arn:aws:elasticloadbalancing:...
```

`privatelink.yaml` の `vpcID` と `loadBalancerARN` を書き換えて push すると ACK が AWS 側の NLB 設定を反映する。

### 4. Grafana ダッシュボード

AMG は ConfigMap によるダッシュボード自動プロビジョニング非対応のため手動インポートが必要。

`manifests/monitoring/dashboards/` 以下の各 YAML ファイルから `data:` 内の JSON を取り出し、AMG の「Import dashboard」で貼り付ける。

| ファイル | 内容 |
|---|---|
| `grafana-strimzi-kafka-dashboard.yaml` | Kafka JMX メトリクス（スループット・レイテンシ等） |
| `grafana-strimzi-exporter-dashboard.yaml` | Consumer Lag |
| `grafana-strimzi-operators-dashboard.yaml` | Strimzi Operator 健全性 |

## Kafka 設定変更（GitOps）

### ブローカー設定変更

```bash
vi manifests/kafka/kafka-cluster.yaml
git commit -am "kafka: min.insync.replicas を変更"
git push
# ArgoCD が自動で apply する
```

### Cruise Control によるリバランス

```bash
kubectl apply -f manifests/kafka/kafka-rebalance.yaml -n kafka
# 提案内容を確認
kubectl describe kafkarebalance kafka-rebalance -n kafka
# 承認
kubectl annotate kafkarebalance kafka-rebalance strimzi.io/rebalance=approve -n kafka
```

## 監視対象メトリクス

| 収集元 | メトリクス内容 | 送信先 |
|---|---|---|
| Kafka Exporter（Strimzi 組み込み） | Consumer Lag | AMP |
| JMX Exporter（Strimzi 組み込み） | ブローカー内部（スループット・レイテンシ等） | AMP |
| Strimzi Cluster Operator | Reconciliation 件数・エラー率 | AMP |
| Strimzi Entity Operator | Topic/User Operator 健全性 | AMP |
| awscontainerinsightreceiver | ノード・Pod リソース使用率 | CloudWatch |
| OTLP | アプリケーショントレース | X-Ray |
| Fluent Bit | コンテナログ | CloudWatch Logs |
