# EKS on EC2 CDK Sample

AWS CDK（Python）で EKS クラスターと Kafka 基盤を構築するサンプルです。

## アーキテクチャ概要

```
VPC（3AZ）
└── EKS クラスター（Managed NodeGroup / Node Auto Repair）
    ├── System NodeGroup  : ArgoCD、CoreDNS 等のクリティカルコンポーネント
    └── Kafka NodeGroup   : Strimzi が管理する Kafka ブローカー

GitHub
└── ArgoCD（App of Apps）
    ├── strimzi-operator  : Strimzi オペレーター（Helm）
    └── kafka-cluster-app : Kafka CR（manifests/kafka/）

外部接続
└── Kafka ← NLB（Private / cross-zone） ← PrivateLink（VPC Endpoint Service）
```

## 技術スタック

| 項目 | 採用技術 |
|---|---|
| IaC | AWS CDK v2（Python）/ `aws_eks_v2` |
| EKS | Kubernetes 1.32 / Managed NodeGroup / Node Auto Repair |
| GitOps | ArgoCD（App of Apps パターン）/ GitHub |
| Kafka | Strimzi 0.45.0 / Kafka 3.9.0 / 3 ブローカー 3AZ |
| 外部接続 | NLB（Internal / cross-zone） + PrivateLink |
| ストレージ | EBS gp3（EBS CSI Driver） |

## CDK スタック構成

### Stack 1 — `EksCdkStack`（インフラ + ArgoCD）

- VPC（3AZ / Public・Private サブネット）
- EKS クラスター（`aws_eks_v2`）
- EKS マネージドアドオン（VPC CNI / CoreDNS / kube-proxy / Pod Identity Agent / EBS CSI Driver）
- ArgoCD（Helm / System NodeGroup に配置）
- Bootstrap Application（`manifests/argocd/` を GitHub から同期）

### Stack 2 — `PrivateLinkStack`（PrivateLink）

Strimzi が NLB を作成した後にデプロイします。

## GitOps フロー

```
git push → ArgoCD が自動検知
         → manifests/argocd/ を同期
              ├── strimzi-operator Application を適用
              └── kafka-cluster-app Application を適用
                    └── manifests/kafka/kafka-cluster.yaml を同期
```

`manifests/kafka/kafka-cluster.yaml` を編集して push するだけで Kafka 設定がクラスターに反映されます。

## デプロイ手順

### 事前準備

```bash
# 依存関係のインストール
uv sync

# AWS 認証設定（CDK_DEFAULT_ACCOUNT / CDK_DEFAULT_REGION）
export CDK_DEFAULT_ACCOUNT=<AWSアカウントID>
export CDK_DEFAULT_REGION=ap-northeast-1
```

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
├── argocd/
│   ├── strimzi-operator.yaml     # Strimzi オペレーター ArgoCD Application
│   └── kafka-cluster-app.yaml    # Kafka CR 管理 ArgoCD Application
└── kafka/
    └── kafka-cluster.yaml        # Kafka Cluster CR（git で変更管理）

ekscdk/
├── constructs/
│   ├── network.py                # VPC
│   ├── eks_cluster.py            # EKS クラスター + NodeGroup
│   ├── addons.py                 # EKS アドオン + ArgoCD + Bootstrap App
│   └── kafka_privatelink.py      # PrivateLink（VPC Endpoint Service）
├── ekscdk_stack.py               # Stack 1
└── privatelink_stack.py          # Stack 2
```
