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
│  │    Strimzi Operator / ACK / CoreDNS                 │   │
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
| Kafka | Strimzi 1.0.0 / Kafka 4.2.0（KRaft）/ broker x3 + controller x3 / 3AZ |
| 外部接続 | Shared NLB（Internal）+ PrivateLink（VPC Endpoint Service） |
| ストレージ | EBS gp3（EBS CSI Driver） |
| メトリクス | ADOT（opentelemetry-collector chart 0.153.0）→ AMP + CloudWatch |
| ログ | Fluent Bit → CloudWatch Logs |
| トレース | ADOT → X-Ray |

## CDK スタック構成

### `IamStack`（Stack 0）

EKS cluster-admin ロール（`eks-cluster-admin`）を作成する。`EksCdkStack` より先にデプロイする。

複数の運用者・CI/CD ロールに cluster-admin 権限を付与する場合は `ekscdk/iam_stack.py` の `assumed_by` を `CompositePrincipal` で列挙する。

### `EksCdkStack`（Stack 1）

- VPC（3AZ / Public・Private サブネット）
- **Kafka 共有 NLB / Endpoint Service**（ARN を固定化）
- EKS クラスター（Kubernetes 1.35）
- EKS マネージドアドオン（VPC CNI / CoreDNS / kube-proxy / Pod Identity Agent / EBS CSI Driver）
- **Strimzi Operator**（Helm 直接管理）
- **NLB Listener / TargetGroup**（CDK ELBv2 で直接管理）
- **Kafka Cluster**（manifests/kafka/ の YAML を CDK でロードして apply）
- 監視環境（ADOT / Fluent Bit / AMP / AMG / CloudWatch Log Group）

## GitOps（ArgoCD）を採用しない理由

Kubernetes リソースは ArgoCD ではなく CDK（`cluster.add_manifest()` / `add_helm_chart()`）が直接 apply する設計を採用している。

GitOps の核心的なメリットは「git push だけで変更が完結する」点にある。しかし本構成では **ブローカー数の変更が Kubernetes リソースと AWS リソースを同時に変更する** ため、この前提が成立しない。

具体的には、`kafka-cluster.yaml` の `configuration.brokers` を編集してブローカーを増減すると：

1. `KafkaNodePool` の `replicas`（Kubernetes）が変わる
2. EKS ノードグループの `min_size` / `max_size`（AWS CloudFormation）が変わる

②は GitOps の管轄外であり `cdk deploy` が必ず必要になる。ArgoCD を導入しても git push だけでは完結しないため、GitOps のメリットが得られない。

## 管理の分担

| 管理対象 | 管理主体 | 理由 |
|---|---|---|
| IAM ロール | CDK（IamStack） | インフラ設定 |
| EKS / ノードグループ / アドオン | CDK（EksCdkStack） | インフラ設定 |
| Strimzi Operator | CDK（EksCdkStack） | バージョン管理をコードで一元化 |
| 監視コンポーネント（ADOT / Fluent Bit / AMP / AMG） | CDK（EksCdkStack） | AMP エンドポイント等の動的値（CloudFormation トークン）を注入するため |
| **NLB リスナー / TargetGroup** | CDK（EksCdkStack） | `KafkaConstruct` が CDK ELBv2 で直接管理。ブローカー増設時は `kafka-cluster.yaml` の `configuration.brokers` を編集して `cdk deploy` |
| **Kafka CR（kafka-cluster.yaml 等）** | CDK（EksCdkStack） | `manifests/kafka/` の YAML を CDK がロードして apply。設定変更は YAML 編集 → `cdk deploy` |

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
cdk deploy EksCdkStack
```

| コンテキストキー | デフォルト | 説明 |
|---|---|---|
| `amg-auth-provider` | `AWS_SSO` | AMG 認証方式（IAM Identity Center 未使用の場合は `SAML`） |

### 3. Kafka 設定変更

ブローカー数・ポート設定は `manifests/kafka/kafka-cluster.yaml` が唯一の変更箇所。`node-pool-broker.yaml` の `replicas` と NLB ポートマッピングは CDK が自動で導出する。

```bash
vi manifests/kafka/kafka-cluster.yaml        # ブローカー設定（configuration.brokers を増減）
cdk deploy EksCdkStack
```

### 4. PrivateLink 経由の接続（クライアント側設定）

Kafka ブローカーの advertised host には NLB の DNS 名が自動的に設定される（`cdk deploy` 時に注入）。

クライアント VPC での名前解決は以下の手順で設定する：

1. **VPC Endpoint を作成する**  
   `cdk deploy` の出力から Endpoint Service 名を取得し、クライアント VPC に Interface Endpoint を作成する。

2. **Route53 エイリアスレコードを作成する**  
   クライアント VPC の Route53 Private Hosted Zone に、NLB の DNS 名へのエイリアスレコードを作成する。  
   エイリアス先は `EksCdkStack` の出力 `KafkaNlbDnsName` を参照する。

3. **Kafka クライアントの設定**  
   `bootstrap.servers` には VPC Endpoint の DNS 名（またはエイリアス先）とポート `9094` を指定する。

### 4. Grafana ダッシュボード

AMG は ConfigMap によるダッシュボード自動プロビジョニング非対応のため手動インポートが必要。

`manifests/monitoring/dashboards/` 以下の各 YAML ファイルから `data:` 内の JSON を取り出し、AMG の「Import dashboard」で貼り付ける。

| ファイル | 内容 |
|---|---|
| `grafana-strimzi-kafka-dashboard.yaml` | Kafka JMX メトリクス（スループット・レイテンシ等） |
| `grafana-strimzi-exporter-dashboard.yaml` | Consumer Lag |
| `grafana-strimzi-operators-dashboard.yaml` | Strimzi Operator 健全性 |

## バージョン更新時の参照先

| 項目 | 確認先 |
|---|---|
| Strimzi がサポートする Kafka バージョン一覧 | `https://github.com/strimzi/strimzi-kafka-operator/blob/<strimzi-version>/kafka-versions.yaml` |
| Kafka バージョンごとの `metadataVersion`（IV形式）| `https://github.com/apache/kafka/<kafka-version>/server-common/src/main/java/org/apache/kafka/server/common/MetadataVersion.java` の末尾定数（`LATEST_PRODUCTION` 相当）|
| opentelemetry-collector Helm chart 最新バージョン | `https://github.com/open-telemetry/opentelemetry-helm-charts/releases` |
| fluent-bit Helm chart 最新バージョン | `https://github.com/fluent/helm-charts/releases` |
| AMG でサポートされる Grafana バージョン | `https://docs.aws.amazon.com/grafana/latest/userguide/version-differences.html` |

## Cruise Control によるリバランス

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
│   ├── addons.py         # EKS アドオン / Strimzi / ACK
│   ├── kafka.py          # Kafka Namespace / NodePool / CR / NLB リスナー（YAML ロード）
│   └── monitoring.py     # AMP / AMG / ADOT / Fluent Bit / IRSA
├── iam_stack.py          # IamStack
└── ekscdk_stack.py       # EksCdkStack

manifests/
├── kafka/
│   ├── kafka-cluster.yaml        # Kafka CR（ブローカー設定）
│   ├── node-pool-broker.yaml     # KafkaNodePool（broker）
│   ├── node-pool-controller.yaml # KafkaNodePool（controller）
│   ├── cm.yaml                   # JMX メトリクス設定 ConfigMap
│   ├── kafka-rebalance.yaml      # Cruise Control リバランス定義（手動 kubectl 用）
└── monitoring/
    └── dashboards/               # AMG インポート用 Grafana ダッシュボード JSON
```
