# EKS on EC2 CDK Sample

AWS CDK（Python）で EKS クラスターと Kafka 基盤・監視環境を構築するサンプルです。

## アーキテクチャ概要

```
VPC（3AZ）
└── EKS クラスター（Managed NodeGroup / Node Auto Repair）
    ├── System NodeGroup（m5.large）: ArgoCD、監視コンポーネント等
    └── Kafka NodeGroup（r5.large） : Strimzi が管理する Kafka ブローカー

GitHub
└── ArgoCD
    ├── strimzi-operator  : Strimzi オペレーター（Helm）
    └── kafka-cluster     : Kafka CR（manifests/kafka/）

外部接続
└── Kafka ← NLB（Private / cross-zone） ← PrivateLink（VPC Endpoint Service）

監視
├── ADOT DaemonSet  : メトリクス → AMP + CloudWatch、トレース → X-Ray
├── Fluent Bit DaemonSet : ログ → CloudWatch Logs
├── kminion         : Kafka Consumer Lag → ADOT 経由で AMP へ
├── AMP             : Prometheus メトリクス保存
└── AMG             : Grafana ダッシュボード（AMP をデータソース）
```

## 技術スタック

| 項目 | 採用技術 |
|---|---|
| IaC | AWS CDK v2（Python）/ `aws_eks_v2` |
| EKS | Kubernetes 1.35 / Managed NodeGroup / Node Auto Repair |
| GitOps | ArgoCD / GitHub |
| Kafka | Strimzi 0.45.0 / Kafka 3.9.0（KRaft）/ 3 ブローカー 3AZ |
| 外部接続 | NLB（Internal / cross-zone）+ PrivateLink |
| ストレージ | EBS gp3（EBS CSI Driver） |
| メトリクス | ADOT + AMP + AMG |
| ログ | Fluent Bit + CloudWatch Logs |
| トレース | ADOT + X-Ray |

## CDK スタック構成

### Stack 0 — `IamStack`（IAM ロール）

- EKS cluster-admin ロール（`eks-cluster-admin`）
- `assumed_by` に運用者・CI/CD ロールの ARN を指定する（詳細は `ekscdk/iam_stack.py` のコメント参照）

### Stack 1 — `EksCdkStack`（インフラ + ArgoCD + 監視）

- VPC（3AZ / Public・Private サブネット）
- EKS クラスター（`aws_eks_v2` / Kubernetes 1.35）
- EKS マネージドアドオン（VPC CNI / CoreDNS / kube-proxy / Pod Identity Agent / EBS CSI Driver）
- AWS Load Balancer Controller（`alb_controller` オプションで自動インストール）
- ArgoCD（Helm / System NodeGroup）
- Strimzi Operator・Kafka Cluster ArgoCD Application（CDK 直接管理）
- 監視環境（ADOT / Fluent Bit / kminion / AMP / AMG / CloudWatch Log Group）

### Stack 2 — `PrivateLinkStack`（PrivateLink）

Strimzi が NLB を作成した後にデプロイします。

## 管理の分担

| 管理対象 | 管理主体 | 理由 |
|---|---|---|
| IAM ロール | CDK（IamStack） | インフラ設定 |
| ArgoCD Application（Strimzi、Kafka） | CDK（EksCdkStack） | インフラ設定。バージョン変更は CDK で行う |
| 監視コンポーネント（ADOT / Fluent Bit / kminion） | CDK（EksCdkStack） | AMP エンドポイント等の動的値を注入するため |
| Kafka CR（`manifests/kafka/`） | Git（ArgoCD 経由） | ブローカースケール・バージョンアップ・設定チューニングを git push で反映 |

## GitOps フロー

```
cdk deploy（初回のみ）
  ├─ Strimzi Operator Application 作成
  └─ kafka-cluster Application 作成（manifests/kafka/ を監視）

git push → ArgoCD が自動検知
  └─ manifests/kafka/ の変更 → Kafka クラスター設定に反映
```

`manifests/kafka/` を編集して push するだけで Kafka 設定がクラスターに反映されます。

## デプロイ手順

### 事前準備

```bash
# 依存関係のインストール
uv sync

# AWS 認証設定
export CDK_DEFAULT_ACCOUNT=<AWSアカウントID>
export CDK_DEFAULT_REGION=ap-northeast-1
```

### Stack 0 デプロイ（IAM ロール）

```bash
cdk deploy IamStack
```

`eks-cluster-admin` ロールが作成されます。本番環境では `ekscdk/iam_stack.py` の `assumed_by` を
SSO Permission Set や CI/CD ロールの ARN に変更してください（複数指定は `CompositePrincipal` を使用）。

### Stack 1 デプロイ

```bash
cdk deploy EksCdkStack -c repo-url=https://github.com/<org>/<repo>
```

プライベートリポジトリの場合は、デプロイ後に GitHub PAT を ArgoCD に登録します。

```bash
argocd repo add https://github.com/<org>/<repo> \
  --username x-token --password <GitHub PAT>
```

### Stack 2 デプロイ（Strimzi が NLB を作成した後）

```bash
# NLB ARN を取得
kubectl get svc kafka-cluster-kafka-external-bootstrap -n kafka \
  -o jsonpath='{.metadata.annotations.service\.beta\.kubernetes\.io/aws-load-balancer-arn}'

# PrivateLink をデプロイ
cdk deploy PrivateLinkStack -c kafka-bootstrap-nlb-arn=<NLB ARN>
```

## ディレクトリ構成

```
manifests/
└── kafka/
    ├── kafka-cluster.yaml     # Kafka Cluster CR（git push で自動反映）
    └── kafka-node-pool.yaml   # KafkaNodePool CR（ブローカースケール等）

ekscdk/
├── constructs/
│   ├── network.py             # VPC
│   ├── eks_cluster.py         # EKS クラスター + NodeGroup
│   ├── addons.py              # EKS アドオン + ArgoCD + Kafka Application
│   ├── monitoring.py          # 監視環境（AMP / AMG / ADOT / Fluent Bit / kminion）
│   └── kafka_privatelink.py   # PrivateLink（VPC Endpoint Service）
├── iam_stack.py               # Stack 0: IAM ロール
├── ekscdk_stack.py            # Stack 1
└── privatelink_stack.py       # Stack 2
```
