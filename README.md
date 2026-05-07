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
└── Kafka ← NLB（Shared / Fixed ARN） ← PrivateLink（VPC Endpoint Service）

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
| 外部接続 | NLB（Internal / Shared）+ PrivateLink |
| ストレージ | EBS gp3（EBS CSI Driver） |
| メトリクス | ADOT + AMP + AMG |
| ログ | Fluent Bit + CloudWatch Logs |
| トレース | ADOT + X-Ray |

## CDK スタック構成

### Stack 0 — `IamStack`（IAM ロール）

- EKS cluster-admin ロール（`eks-cluster-admin`）
- `assumed_by` に運用者・CI/CD ロールの ARN を指定する（詳細は `ekscdk/iam_stack.py` のコメント参照）

### Stack 1 — `EksCdkStack`（インフラ + ArgoCD + 監視 + ACK）

- VPC（3AZ / Public・Private サブネット）
- **Kafka 共有 NLB / Endpoint Service**（本体のみ CDK で作成。ARN を固定化）
- EKS クラスター（`aws_eks_v2` / Kubernetes 1.35）
- EKS マネージドアドオン（VPC CNI / CoreDNS / kube-proxy / Pod Identity Agent / EBS CSI Driver）
- AWS Load Balancer Controller（`alb_controller` オプションで自動インストール）
- ArgoCD（Helm / System NodeGroup）
- **ACK EC2 / ELBv2 Controller**（PrivateLink と NLB リスナーをマニフェストで管理するために導入）
- Strimzi Operator・Kafka Cluster ArgoCD Application（CDK 直接管理）
- 監視環境（ADOT / Fluent Bit / kminion / AMP / AMG / CloudWatch Log Group）

## 管理の分担

| 管理対象 | 管理主体 | 理由 |
|---|---|---|
| IAM ロール | CDK（IamStack） | インフラ設定 |
| ArgoCD Application（Strimzi、Kafka） | CDK（EksCdkStack） | インフラ設定。バージョン変更は CDK で行う |
| 監視コンポーネント（ADOT / Fluent Bit / kminion） | CDK（EksCdkStack） | AMP エンドポイント等の動的値を注入するため |
| **NLB リスナー / ターゲットグループ** | Git（ArgoCD 経由） | **ACK (ELBv2) を使用**。ブローカー増設に合わせて YAML を更新 |
| PrivateLink（Endpoint Service） | Git（ArgoCD 経由） | **ACK (EC2) を使用**。マニフェストで固定 NLB ARN を参照 |
| Kafka CR（`manifests/kafka/`） | Git（ArgoCD 経由） | ブローカースケール・バージョンアップ・設定チューニングを git push で反映 |

## GitOps フロー

```
cdk deploy（初回のみ）
  ├─ 共有 NLB / Endpoint Service 作成（ARN 固定）
  ├─ ACK EC2 / ELBv2 Controller インストール
  ├─ Strimzi Operator Application 作成
  └─ kafka-cluster Application 作成（manifests/kafka/ を監視）

git push → ArgoCD が自動検知
  ├─ manifests/kafka/kafka-cluster.yaml の変更 → Kafka 設定反映
  └─ manifests/kafka/privatelink.yaml の変更 → AWS リスナー設定反映
```

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

### Stack 1 デプロイ（インフラ・基盤）

```bash
cdk deploy EksCdkStack -c repo-url=https://github.com/<org>/<repo>
```

### PrivateLink / 接続設定のカスタマイズ

本構成では、ブローカーの増設に合わせて **YAML（GitOps）だけで AWS のリスナー設定を拡張** できます。

1. **必要な値を確認する**
   `EksCdkStack` デプロイ時の出力（Outputs）から、以下の値を確認します。
   - `VpcId`: `vpc-xxxxxx`
   - `KafkaSharedNlbArn`: `arn:aws:elasticloadbalancing:...`

2. **`manifests/kafka/privatelink.yaml` を編集**
   上記で確認した値を、マニフェスト内の `vpcID` および `loadBalancerARN` フィールドに反映させます。
   ブローカー増設時は、同様の手順で新しいポートの `Listener` と `TargetGroup` を追記します。

3. **git push**
   ACK ELBv2 Controller が即座に AWS 側の NLB 設定を更新します。

## ディレクトリ構成

```
manifests/
└── kafka/
    ├── kafka-cluster.yaml     # Kafka Cluster CR（git push で自動反映）
    ├── kafka-node-pool.yaml   # KafkaNodePool CR
    └── privatelink.yaml       # AWS Listener / TargetGroup / VPCEndpointService（ACK 用）

ekscdk/
├── constructs/
│   ├── network.py             # VPC + Shared NLB / Endpoint Service
│   ├── eks_cluster.py         # EKS クラスター + NodeGroup
│   ├── addons.py              # EKS アドオン + ArgoCD + ACK + Kafka Application
│   └── monitoring.py          # 監視環境（AMP / AMG / ADOT / Fluent Bit / kminion）
├── iam_stack.py               # Stack 0: IAM ロール
└── ekscdk_stack.py            # Stack 1
```
