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
| Kafka | Strimzi 0.45.0 / Kafka 3.9.0（KRaft）/ broker x3 + controller x3 / 3AZ |
| 外部接続 | Shared NLB（Internal）+ PrivateLink（VPC Endpoint Service） |
| ストレージ | EBS gp3（EBS CSI Driver） |
| メトリクス | ADOT v0.40.0 → AMP + CloudWatch |
| ログ | Fluent Bit → CloudWatch Logs |
| トレース | ADOT → X-Ray |

## CDK と ArgoCD の使い分け

| 管理手段 | 対象 | 理由 |
|---|---|---|
| **CDK** | インフラ・監視スタック | AMP エンドポイントや CloudWatch Log Group 名など、デプロイ後に確定する動的値（CloudFormation トークン）を設定に注入する必要があるため |
| **ArgoCD（GitOps）** | Kafka 設定 | ブローカー設定・パーティション数・レプリカ数は運用中に変更頻度が高く、CDK 再デプロイなしに git push だけで反映できる方が運用しやすいため |

監視スタック（ADOT・Fluent Bit）はほぼ変更しないインフラに近い性質のため、CDK 管理で十分と判断している。

## CDK スタック構成

### `IamStack`（Stack 0）

EKS cluster-admin ロール（`eks-cluster-admin`）を作成する。`EksCdkStack` より先にデプロイする。

複数の運用者・CI/CD ロールに cluster-admin 権限を付与する場合は `ekscdk/iam_stack.py` の `assumed_by` を `CompositePrincipal` で列挙する。

### `EksCdkStack`（Stack 1）

- VPC（3AZ / Public・Private サブネット）
- **Kafka 共有 NLB / Endpoint Service**（本体のみ CDK で作成。ARN を固定化）
- EKS クラスター（Kubernetes 1.35）
- EKS マネージドアドオン（VPC CNI / CoreDNS / kube-proxy / Pod Identity Agent / EBS CSI Driver）
- ArgoCD（Helm / System NodeGroup）
- **ACK EC2 / ELBv2 Controller**（NLB リスナーと PrivateLink をマニフェストで管理するために導入）
- Strimzi Operator・Kafka Cluster ArgoCD Application（CDK 直接管理）
- 監視環境（ADOT / Fluent Bit / AMP / AMG / CloudWatch Log Group）

## 管理の分担

| 管理対象 | 管理主体 | 理由 |
|---|---|---|
| IAM ロール | CDK（IamStack） | インフラ設定 |
| EKS / ノードグループ / アドオン | CDK（EksCdkStack） | インフラ設定 |
| 監視コンポーネント（ADOT / Fluent Bit / AMP / AMG） | CDK（EksCdkStack） | AMP エンドポイント等の動的値（CloudFormation トークン）を注入するため |
| ArgoCD Application（Strimzi / Kafka） | CDK（EksCdkStack） | インフラ設定。バージョン変更は CDK で行う |
| **NLB リスナー / ターゲットグループ** | Git（ArgoCD 経由） | **ACK ELBv2** を使用。ブローカー増設に合わせて YAML で拡張できる |
| **PrivateLink（Endpoint Service 許可）** | Git（ArgoCD 経由） | **ACK EC2** を使用。固定 NLB ARN をマニフェストで参照 |
| **Kafka CR（kafka-cluster.yaml 等）** | Git（ArgoCD 経由） | ブローカー設定・パーティション・スケールを git push で反映 |

## GitOps フロー

```
cdk deploy（初回・インフラ変更時）
  ├─ 共有 NLB / Endpoint Service 作成（ARN 固定）
  ├─ ACK EC2 / ELBv2 Controller インストール
  ├─ Strimzi Operator Application 作成
  └─ kafka-cluster Application 作成（manifests/kafka/ を監視）

git push → ArgoCD が自動検知・反映
  ├─ manifests/kafka/kafka-cluster.yaml  → Kafka ブローカー設定変更
  ├─ manifests/kafka/node-pool-*.yaml    → ノードプール設定変更
  └─ manifests/kafka/privatelink.yaml   → AWS NLB リスナー設定変更（ACK 経由）
```

## デプロイ手順

### 前提条件

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

本構成では、ブローカー増設時も **YAML（GitOps）だけで AWS のリスナー設定を拡張**できる。

1. **デプロイ出力から値を確認する**

   ```
   Outputs:
     EksCdkStack.VpcId             = vpc-xxxxxxxxxxxxxxxxx
     EksCdkStack.KafkaSharedNlbArn = arn:aws:elasticloadbalancing:...
   ```

2. **`manifests/kafka/privatelink.yaml` を編集**

   `vpcID` と `loadBalancerARN` に上記の値を反映する。ブローカー増設時は新しいポートの `Listener` と `TargetGroup` を追記する。

3. **git push**

   ACK ELBv2 Controller が即座に AWS 側の NLB 設定を更新する。

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
| Fluent Bit | コンテナログ | CloudWatch Logs (`/aws/eks/eks-cluster/application`) |

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
