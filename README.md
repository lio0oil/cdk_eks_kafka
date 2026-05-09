# EKS CDK

AWS CDK（Python）で EKS クラスター上に Kafka 基盤を構築するプロジェクト。

## アーキテクチャ

```
┌────────────────────────────────────────────────────────────────────────┐
│ VPC (10.0.0.0/16 / 3 AZ)                                              │
│                                                                       │
│  ┌──────────────────────────────────────────────────────────────┐    │
│  │ EKS 1.35（Standard support / DeletionProtection 環境別）     │    │
│  │                                                              │    │
│  │  system-nodegroup (dev: t4g.large × 2)                       │    │
│  │    Strimzi Operator (HA × 2) / AWS LBC (× 2)                 │    │
│  │    kube-prometheus-stack (Prometheus / node-exporter /       │    │
│  │      kube-state-metrics / Prometheus Operator)               │    │
│  │    Fluent Bit (DaemonSet)                                    │    │
│  │                                                              │    │
│  │  kafka-nodegroup (dev: t4g.large × 6, broker+controller=6)   │    │
│  │    Kafka broker × 3 + KRaft controller × 3                   │    │
│  │    （cross-pool AntiAffinity で全 6 pod が別ノード配置）     │    │
│  │    Cruise Control / Entity Operator / kafka-exporter         │    │
│  │                                                              │    │
│  │  AWS LBC が Strimzi NodePort Service と NLB TargetGroup を   │    │
│  │    TargetGroupBinding 経由で動的バインド                     │    │
│  └──────────────────────────────────────────────────────────────┘    │
│                            │                                          │
│  Kafka Shared NLB (Internal, ARN 固定) ──▶ VPC Endpoint Service       │
│                            ↓                       (PrivateLink)      │
└────────────────────────────────────────────────────────────────────────┘
         │ メトリクス (Prometheus remote_write SigV4)  │ ログ
         ▼                                            ▼
      AMP + AMG                               CloudWatch Logs
```

## 技術スタック

| 項目 | 採用技術 |
|---|---|
| IaC | AWS CDK v2（Python）/ `aws_eks_v2` |
| Kubernetes | 1.35 / Managed NodeGroup / Node Auto Repair / UpgradePolicy=STANDARD |
| Kafka | Strimzi 1.0.0 / Kafka 4.2.0（KRaft）/ broker × 3 + controller × 3 / 3AZ |
| 外部接続 | Shared NLB（Internal） + PrivateLink (VPC Endpoint Service) + AWS LBC TargetGroupBinding |
| ストレージ | EBS gp3（EBS CSI Driver） |
| メトリクス | kube-prometheus-stack (chart 84.5.0) → AMP（SigV4 remote_write）|
| 可視化 | AMG（Amazon Managed Grafana）/ data source = AMP |
| ログ | Fluent Bit (chart 0.57.3) → CloudWatch Logs |

## CDK スタック構成

### `IamStack`（Stack 0）

EKS cluster-admin ロール（`eks-cluster-admin`）を作成する。`EksCdkStack` より先にデプロイする。

複数の運用者・CI/CD ロールに cluster-admin 権限を付与する場合は `ekscdk/iam_stack.py` の `assumed_by` を `CompositePrincipal` で列挙する。

### `EksCdkStack`（Stack 1）

- VPC（3AZ / Public・Private サブネット）
- **Kafka 共有 NLB / TargetGroup / Listener / Endpoint Service**（NetworkConstruct、ARN を固定化）
- EKS クラスター（Kubernetes 1.35 / UpgradePolicy=STANDARD / 環境別 DeletionProtection）
- EKS マネージドアドオン（VPC CNI / CoreDNS / kube-proxy / EBS CSI Driver）
  - eks-pod-identity-agent は EKS が自動インストール（CDK 管理外）
- **Strimzi Operator** / **AWS Load Balancer Controller**（Helm 直接管理）
- **Kafka Cluster**（manifests/kafka/ の YAML を CDK でロードして apply）
- **TargetGroupBinding**（AWS LBC が Strimzi NodePort Service と NLB TargetGroup を動的バインド）
- 監視環境（kube-prometheus-stack + Fluent Bit + AMP + AMG + CloudWatch Log Group）

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
| Strimzi Operator / AWS Load Balancer Controller | CDK（EksCdkStack） | バージョン管理をコードで一元化 |
| 監視コンポーネント（kube-prometheus-stack / Fluent Bit / AMP / AMG） | CDK（EksCdkStack） | AMP エンドポイント等の動的値（CloudFormation トークン）を注入するため |
| **NLB / TargetGroup / Listener / Endpoint Service** | CDK（EksCdkStack の NetworkConstruct） | NLB ARN を固定化（再作成すると PrivateLink が壊れる）。ブローカー増設時は `kafka-cluster.yaml` の `configuration.brokers` を編集して `cdk deploy` |
| **TargetGroupBinding**（NLB と Pod の動的バインド） | CDK + AWS LBC | `KafkaConstruct` が manifest として apply、ランタイムは AWS LBC が Service Endpoints と TargetGroup を同期 |
| **Kafka CR（kafka-cluster.yaml 等）** | CDK（EksCdkStack） | `manifests/kafka/` の YAML を CDK がロードして apply。設定変更は YAML 編集 → `cdk deploy` |
| Grafana ダッシュボード / AMG 権限 | **手動**（README §5 参照） | AMG は宣言的プロビジョニング非対応。コンソール or HTTP API で手動運用 |

## デプロイ手順

### 前提条件

```bash
uv sync

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
| `env` | `dev` | デプロイ環境（`dev` / `stg` / `prd`）。`ekscdk/config.py` の `ClusterConfig.for_<env>()` が選択される |
| `amg-auth-provider` | `AWS_SSO` | AMG 認証方式（IAM Identity Center 未使用の場合は `SAML`） |

### 3. Kafka 設定変更

ブローカー数・ポート設定は `manifests/kafka/kafka-cluster.yaml` が唯一の変更箇所。`node-pool-broker.yaml` の `replicas` と NLB ポートマッピングは CDK が自動で導出する。

```bash
vi manifests/kafka/kafka-cluster.yaml        # ブローカー設定（configuration.brokers を増減）
cdk deploy EksCdkStack
```

#### ブローカースケールアウト手順

ブローカーを増設する場合は以下の順序で作業する。CDK は Kubernetes リソース（KafkaNodePool の replicas）と AWS リソース（EKS ノードグループの min/max_size、NLB Listener / TargetGroup）を同時に変更するため、`cdk deploy` が必須。

1. `manifests/kafka/kafka-cluster.yaml` の `configuration.brokers` に新ブローカーのエントリを追加する（`broker`・`nodePort`・`advertisedPort` を既存と重複しない値で指定）
2. `cdk deploy EksCdkStack` を実行する（ノードグループ拡張 → NLB Listener 追加 → KafkaNodePool replicas 更新 の順に適用される）
3. Strimzi がブローカー Pod を新ノードにスケジュールし、クラスターに参加させる
4. 必要に応じて Cruise Control でパーティションをリバランスする（[手順](#11-cruise-control-リバランス) 参照）

> **注意**: 手順 2 を省略して `kubectl` や ArgoCD だけで YAML を apply しても EKS ノードグループが拡張されないため、ブローカー Pod が Pending のままになる。

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

### 5. Grafana（AMG）の権限設定とダッシュボードインポート

AMG は ConfigMap や CR ベースの宣言的プロビジョニング**非対応**（self-hosted Grafana の Operator パターンが使えない）。本プロジェクトでは権限・ダッシュボードとも以下のいずれかで設定する。

#### 5-1. 権限設定（IAM Identity Center / SSO ユーザーを AMG にマップ）

`auth_providers=AWS_SSO` で構築している場合、IAM Identity Center の User / Group を AMG の `ADMIN` / `EDITOR` / `VIEWER` ロールにマップする必要がある。

**(A) AMG コンソール（最も簡単）**

1. AMG コンソール → 対象ワークスペース → 「ユーザーとアクセス」タブ
2. 「ユーザーまたはグループを割り当て」→ Identity Center から検索
3. ロール（Admin / Editor / Viewer）を指定して保存

**(B) AWS CLI（再現性・コード管理向き）**

```bash
WORKSPACE_ID=$(aws grafana list-workspaces \
  --query 'workspaces[?name==`eks-cluster-dev-grafana`].id | [0]' --output text)

# Identity Center から User/Group ID を取得
IDC_INSTANCE=$(aws sso-admin list-instances --query 'Instances[0].IdentityStoreId' --output text)
USER_ID=$(aws identitystore list-users --identity-store-id $IDC_INSTANCE \
  --filters "AttributePath=UserName,AttributeValue=alice@example.com" \
  --query 'Users[0].UserId' --output text)
GROUP_ID=$(aws identitystore list-groups --identity-store-id $IDC_INSTANCE \
  --filters "AttributePath=DisplayName,AttributeValue=KafkaOperators" \
  --query 'Groups[0].GroupId' --output text)

# 権限割当
aws grafana update-permissions --workspace-id $WORKSPACE_ID \
  --update-instruction-batch '[
    {
      "action": "ADD",
      "role": "ADMIN",
      "users": [{"id": "'$USER_ID'", "type": "SSO_USER"}]
    },
    {
      "action": "ADD",
      "role": "EDITOR",
      "users": [{"id": "'$GROUP_ID'", "type": "SSO_GROUP"}]
    }
  ]'

# 確認
aws grafana list-permissions --workspace-id $WORKSPACE_ID \
  --query 'permissions[*].[role,user.id,user.type]' --output table
```

#### 5-2. ダッシュボードインポート

`manifests/monitoring/dashboards/` 以下の各 YAML ファイルに dashboard JSON が含まれている。

| ファイル | 内容 |
|---|---|
| `grafana-strimzi-kafka-dashboard.yaml` | Kafka JMX メトリクス（スループット・レイテンシ等）|
| `grafana-strimzi-exporter-dashboard.yaml` | Consumer Lag / Topic / Partitions |
| `grafana-strimzi-operators-dashboard.yaml` | Strimzi Operator 健全性 / Reconciliation 件数 |

**(A) AMG コンソール（最も簡単）**

1. AMG コンソール → ワークスペース URL を開く
2. 左メニュー Dashboards → New → Import
3. 各 YAML から `data.<key>:` 配下の JSON 文字列を取り出し、Import 画面の「Import via dashboard JSON model」に貼り付け
4. データソース選択：`Prometheus`（AMP）→ Import

**(B) Grafana HTTP API（自動化向き）**

ダッシュボード JSON を Grafana の `/api/dashboards/db` に POST する。AMG ではアクセスに **Service Account Token** が必要。

```bash
WORKSPACE_ID=$(aws grafana list-workspaces \
  --query 'workspaces[?name==`eks-cluster-dev-grafana`].id | [0]' --output text)
WORKSPACE_URL=$(aws grafana describe-workspace --workspace-id $WORKSPACE_ID \
  --query 'workspace.endpoint' --output text)

# Service Account を作成（権限 ADMIN：dashboard 作成に必要）
SA_ID=$(aws grafana create-workspace-service-account \
  --workspace-id $WORKSPACE_ID \
  --grafana-role ADMIN --name dashboard-importer \
  --query 'id' --output text)

# トークン発行（最大 30 日有効）
TOKEN=$(aws grafana create-workspace-service-account-token \
  --workspace-id $WORKSPACE_ID --service-account-id $SA_ID \
  --name dashboard-importer-token --seconds-to-live 3600 \
  --query 'serviceAccountToken.key' --output text)

# YAML から dashboard JSON を抽出してインポート
for f in manifests/monitoring/dashboards/*.yaml; do
  python3 -c "
import yaml, json, sys
with open('$f') as fp:
    doc = yaml.safe_load(fp)
for k, v in doc['data'].items():
    print(json.dumps({'dashboard': json.loads(v), 'overwrite': True}))
" | curl -sS -X POST "https://${WORKSPACE_URL}/api/dashboards/db" \
       -H "Authorization: Bearer $TOKEN" \
       -H "Content-Type: application/json" \
       -d @- | python3 -m json.tool | head -5
done

# クリーンアップ：Service Account 削除（トークンも連動失効）
aws grafana delete-workspace-service-account \
  --workspace-id $WORKSPACE_ID --service-account-id $SA_ID
```

> **将来的な CDK 化**: 権限は `AwsCustomResource` で `grafana:UpdatePermissions` を呼べば CFN ベースで完結する（容易）。ダッシュボードは Grafana HTTP API のため独自 Lambda + `Custom::DashboardImport` カスタムリソースが必要（中程度の実装コスト）。本リポジトリでは現状手動運用としている。

#### 5-3. Node / Cluster / Pod ダッシュボード（community 推奨セット）

Strimzi 系 3 ダッシュボードに加え、Node OS / Cluster 全体 / Pod レベルの可視化は kube-prometheus-stack 同梱（grafana.enabled=false なので自動展開なし）の代わりに **grafana.com の community dashboard を ID 指定で import** する。kube-prometheus-stack の標準メトリクス名（`node_*`, `container_*`, `kube_*`）に対応しているため、本構成にそのまま接続できる。

| dashboard ID | 名前 | カバー範囲 |
|---|---|---|
| **1860** | Node Exporter Full | Node OS 全部入り（CPU / Memory / Disk 容量・IO / Network / Load）。**最優先**でインポート推奨 |
| 15757 | Kubernetes / Compute Resources / Cluster | クラスタ全体俯瞰、Namespace 別合計 |
| 15760 | Kubernetes / Compute Resources / Node (Pods) | Node 上の Pod 別 CPU / Memory 配分 |
| 15758 | Kubernetes / Compute Resources / Namespace (Pods) | Namespace 内の Pod 別リソース |
| 15761 | Kubernetes / Compute Resources / Pod | Pod 単体の詳細 |

**インポート手順（AMG コンソール、最も簡単）**

1. AMG コンソール → 左メニュー **Dashboards → New → Import**
2. **「Import via grafana.com」** に上記 ID（例: `1860`）を入力 → **Load**
3. データソース：`Prometheus`（AMP のもの）を選択 → **Import**

**インポート手順（HTTP API 経由、Service Account Token 利用）**

§5-2(B) で発行した `$TOKEN` をそのまま使い、grafana.com から JSON を取得して POST する：

```bash
WORKSPACE_URL=$(aws grafana describe-workspace --workspace-id $WORKSPACE_ID \
  --query 'workspace.endpoint' --output text)

# AMP データソースの UID を取得（dashboard JSON の datasource 置換用）
DS_UID=$(curl -sS "https://${WORKSPACE_URL}/api/datasources/name/prometheus" \
  -H "Authorization: Bearer $TOKEN" | python3 -c "import sys,json;print(json.load(sys.stdin)['uid'])")

# community dashboard ID を一括インポート
for ID in 1860 15757 15760 15758 15761; do
  REV=$(curl -sS "https://grafana.com/api/dashboards/${ID}" | python3 -c "import sys,json;print(json.load(sys.stdin)['revision'])")
  curl -sS "https://grafana.com/api/dashboards/${ID}/revisions/${REV}/download" \
    | python3 -c "
import sys, json
d = json.load(sys.stdin)
# datasource template variable を AMP の UID で固定
for v in d.get('templating', {}).get('list', []):
    if v.get('type') == 'datasource':
        v['current'] = {'text': 'prometheus', 'value': '$DS_UID'}
print(json.dumps({'dashboard': d, 'overwrite': True, 'inputs': [{'name': 'DS_PROMETHEUS', 'type': 'datasource', 'pluginId': 'prometheus', 'value': '$DS_UID'}]}))
" | curl -sS -X POST "https://${WORKSPACE_URL}/api/dashboards/import" \
       -H "Authorization: Bearer $TOKEN" \
       -H "Content-Type: application/json" \
       -d @- | python3 -c "import sys,json;d=json.load(sys.stdin);print(f\"  {d.get('title')}: {d.get('uid')}\")"
done
```

> **community dashboard を Git 管理したい場合**: `https://grafana.com/api/dashboards/<ID>/revisions/<REV>/download` から JSON を保存し、`manifests/monitoring/dashboards/` 以下に既存形式（`apiVersion: v1, kind: ConfigMap, data: { <name>.json: '<json string>' }`）で配置すれば §5-2(B) の一括インポートスクリプトで反映できる。

## バージョン更新時の参照先

| 項目 | 確認先 |
|---|---|
| Strimzi がサポートする Kafka バージョン一覧 | `https://github.com/strimzi/strimzi-kafka-operator/blob/<strimzi-version>/kafka-versions.yaml` |
| Kafka バージョンごとの `metadataVersion`（IV形式）| `https://github.com/apache/kafka/<kafka-version>/server-common/src/main/java/org/apache/kafka/server/common/MetadataVersion.java` の末尾定数（`LATEST_PRODUCTION` 相当）|
| kube-prometheus-stack Helm chart 最新バージョン | `https://github.com/prometheus-community/helm-charts/releases?q=kube-prometheus-stack` |
| aws-load-balancer-controller Helm chart 最新バージョン | `https://github.com/kubernetes-sigs/aws-load-balancer-controller/releases` |
| AWS LBC IAM ポリシー JSON | `https://raw.githubusercontent.com/kubernetes-sigs/aws-load-balancer-controller/v<version>/docs/install/iam_policy.json`（`manifests/addons/aws-lbc-iam-policy.json` を更新）|
| fluent-bit Helm chart 最新バージョン | `https://github.com/fluent/helm-charts/releases` |
| AMG でサポートされる Grafana バージョン | `https://docs.aws.amazon.com/grafana/latest/userguide/version-differences.html` |
| EKS K8s バージョンサポート期限 | `https://docs.aws.amazon.com/eks/latest/userguide/kubernetes-versions.html` |

## デプロイ後の動作確認

### 1. kubeconfig 更新 / EKS API アクセス権付与

EKS のアクセスは 2 層構造（**IAM 認証** + **EKS Access Entry → K8s RBAC マッピング**）。CDK が一部を自動で設定するが、新規ユーザー追加は手動が必要。

#### 1-1. CDK が自動で設定するもの

| 設定 | 対象 | 設定箇所 |
|------|------|---------|
| `cdk deploy` 実行者を cluster-admin に登録 | デプロイ実行 IAM Principal | `bootstrap_cluster_creator_admin_permissions=True`（`eks_cluster.py`）|
| `eks-cluster-admin-<env>` ロールに cluster-admin | IamStack のロール | `AccessEntry "AdminAccessEntry"`（`eks_cluster.py`）|
| context で渡した admin ロールを cluster-admin に登録 | 例：SSO Permission Set Role | `cdk deploy -c console-role-arns=arn1,arn2`（カンマ区切り）|

#### 1-2. kubeconfig 更新

```bash
# 自分のクレデンシャルで接続
aws eks update-kubeconfig --name eks-cluster-dev --region ap-northeast-1

# 特定の IAM ロールを assume して接続（例: 上記 admin ロール）
aws eks update-kubeconfig --name eks-cluster-dev --region ap-northeast-1 \
  --role-arn arn:aws:iam::<AWSアカウントID>:role/eks-cluster-admin-eks-cluster-dev

kubectl get nodes
```

#### 1-3. 後から CLI でユーザー / ロールを追加

`bootstrap_cluster_creator_admin_permissions` でも `console-role-arns` でもカバーされなかった IAM Principal は、後から Access Entry を作って権限を紐付ける。

```bash
CLUSTER=eks-cluster-dev
PRINCIPAL_ARN="arn:aws:iam::<AWSアカウントID>:role/AWSReservedSSO_AdministratorAccess_xxx"

# 1) Access Entry 作成（IAM Principal を EKS に登録）
aws eks create-access-entry \
  --cluster-name $CLUSTER \
  --principal-arn "$PRINCIPAL_ARN" \
  --type STANDARD

# 2) Access Policy を紐付け（K8s 権限を付与）
aws eks associate-access-policy \
  --cluster-name $CLUSTER \
  --principal-arn "$PRINCIPAL_ARN" \
  --policy-arn arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy \
  --access-scope type=cluster

# 3) 確認
aws eks list-associated-access-policies --cluster-name $CLUSTER \
  --principal-arn "$PRINCIPAL_ARN"
```

主要 Access Policy ARN：

| Policy | ARN suffix | 用途 |
|--------|-----------|------|
| `AmazonEKSClusterAdminPolicy` | `cluster-access-policy/AmazonEKSClusterAdminPolicy` | 全権限（cluster scope）|
| `AmazonEKSAdminPolicy` | `cluster-access-policy/AmazonEKSAdminPolicy` | namespace 単位 admin（`--access-scope type=namespace,namespaces=kafka`）|
| `AmazonEKSEditPolicy` | `cluster-access-policy/AmazonEKSEditPolicy` | edit |
| `AmazonEKSViewPolicy` | `cluster-access-policy/AmazonEKSViewPolicy` | 参照のみ |
| `AmazonEKSAdminViewPolicy` | `cluster-access-policy/AmazonEKSAdminViewPolicy` | Secret 含む参照 |

#### 1-4. AWS マネジメントコンソールでの操作

1. EKS Console → 対象クラスター → **「アクセス」タブ**
2. **「アクセスエントリーの作成」**
3. **IAM Principal ARN** を入力（SSO は `AWSReservedSSO_<PermissionSetName>_<id>` ロール）
4. **「次へ」→ ポリシーを追加** → `AmazonEKSClusterAdminPolicy` 等を選択、スコープ指定
5. **「作成」**
6. ローカルで `aws eks update-kubeconfig` を実行

#### 1-5. Access Entry の削除

```bash
# Policy 紐付けを解除
aws eks disassociate-access-policy --cluster-name $CLUSTER \
  --principal-arn "$PRINCIPAL_ARN" \
  --policy-arn arn:aws:eks::aws:cluster-access-policy/AmazonEKSClusterAdminPolicy

# Access Entry 削除
aws eks delete-access-entry --cluster-name $CLUSTER --principal-arn "$PRINCIPAL_ARN"
```

### 2. ノード確認

```bash
kubectl get nodes
# STATUS が Ready になっていれば OK
```

### 3. アドオン Pod 確認

```bash
kubectl get pods -n kube-system
# coredns / kube-proxy / aws-node（VPC CNI）/ ebs-csi-controller 等が Running であること
```

### 4. Strimzi Operator 確認

```bash
kubectl get pods -n strimzi-system
# strimzi-cluster-operator-* が Running であること
```

### 5. Kafka CR の状態確認

```bash
kubectl get kafka -n kafka
# READY が True になるまで数分かかる

kubectl get kafkanodepool -n kafka
```

### 6. 監視コンポーネント確認

```bash
kubectl get pods -n monitoring
# 以下が Running であること:
#   prometheus-* (kube-prometheus-stack の Prometheus pod)
#   *-prometheus-operator-*
#   *-kube-state-metrics-*
#   *-prometheus-node-exporter-* (DaemonSet)
#   *-fluent-bit-* (DaemonSet)
kubectl get pods -n kube-system -l app.kubernetes.io/name=aws-load-balancer-controller
# AWS LBC pod (× 2) が Running であること
```

### 7. NLB 経由の Kafka 接続確認

NLB は internal なため、検証は **VPC 内（クラスタ Pod）から**行う。`system` / `kafka` ノードはそれぞれタイント付きのため、テスト Pod には toleration が必要。

```bash
# NLB DNS と CA cert を取得
NLB_DNS=$(aws cloudformation describe-stacks --stack-name EksCdkStack \
  --query 'Stacks[0].Outputs[?OutputKey==`KafkaNlbDnsName`].OutputValue' --output text)
kubectl get secret -n kafka kafka-cluster-cluster-ca-cert \
  -o jsonpath='{.data.ca\.crt}' | base64 -d > /tmp/ca.crt
kubectl create configmap kafka-test-ca --from-file=/tmp/ca.crt -n default
```

検証用 Pod を起動して、TLS handshake → Kafka メタデータ取得 → topic 一覧取得を一気通貫で確認：

```bash
cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: kafka-client-test
  namespace: default
spec:
  restartPolicy: Never
  tolerations:
    - operator: Exists
  volumes:
    - name: ca
      configMap:
        name: kafka-test-ca
    - name: workdir
      emptyDir: {}
  containers:
    - name: kafka-client
      image: quay.io/strimzi/kafka:1.0.0-kafka-4.2.0
      command:
        - sh
        - -c
        - |
          set -e
          echo "=== TLS handshake ==="
          openssl s_client -connect ${NLB_DNS}:9094 -servername ${NLB_DNS} \
            -CAfile /etc/ca/ca.crt -verify_return_error -brief </dev/null 2>&1 | head -10

          echo "=== truststore 作成 ==="
          keytool -import -trustcacerts -alias ca -file /etc/ca/ca.crt \
            -keystore /work/truststore.jks -storepass changeit -noprompt

          cat > /work/client.properties <<PROPS
          security.protocol=SSL
          ssl.truststore.location=/work/truststore.jks
          ssl.truststore.password=changeit
          PROPS

          echo "=== broker メタデータ取得 ==="
          /opt/kafka/bin/kafka-broker-api-versions.sh \
            --bootstrap-server ${NLB_DNS}:9094 \
            --command-config /work/client.properties | head -1

          echo "=== topic 一覧 ==="
          /opt/kafka/bin/kafka-topics.sh \
            --bootstrap-server ${NLB_DNS}:9094 \
            --command-config /work/client.properties --list
      volumeMounts:
        - name: ca
          mountPath: /etc/ca
        - name: workdir
          mountPath: /work
EOF

# 完了まで待機（30〜60 秒）してログ確認
kubectl wait --for=condition=Ready=False pod/kafka-client-test --timeout=120s
kubectl logs kafka-client-test

# クリーンアップ
kubectl delete pod kafka-client-test
kubectl delete configmap kafka-test-ca
```

期待される出力：

```
=== TLS handshake ===
Protocol version: TLSv1.3
Peer certificate: O=io.strimzi, CN=kafka-cluster-kafka
Verification: OK

=== broker メタデータ取得 ===
kafka-shared-nlb-xxx.elb.ap-northeast-1.amazonaws.com:9095 (id: 0 rack: ap-northeast-1c isFenced: false) -> ...

=== topic 一覧 ===
strimzi.cruisecontrol.metrics
strimzi.cruisecontrol.modeltrainingsamples
strimzi.cruisecontrol.partitionmetricsamples
```

`broker` の advertised host/port が NLB DNS:9095/9096/9097 になっている点が確認できれば、**NLB → NodePort → broker pod** の経路が正しく機能している。

### 8. Consumer Group ダッシュボードの動作確認（オプション）

`Strimzi Kafka Exporter` ダッシュボードの以下のパネルは consumer group が稼働していないと `No data` になる：

- Messages consumed per second
- Lag by Consumer Group
- Consumer Group Offsets
- Consumer Group Lag

> Cruise Control は Kafka を `assign` モードで読むため consumer group には登録されない。実 producer/consumer アプリが動くまで上記パネルは空のままで正常。

ダッシュボード自体の動作確認をしたい場合はテストデータを流す：

```bash
# topic 作成
kubectl exec -n kafka kafka-cluster-kafka-0 -c kafka -- bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 \
  --create --topic test-topic --partitions 3 --replication-factor 3

# 100 件 produce
kubectl exec -n kafka kafka-cluster-kafka-0 -c kafka -- bash -c '
  for i in $(seq 1 100); do echo "msg-$i"; done \
    | bin/kafka-console-producer.sh --bootstrap-server localhost:9092 --topic test-topic'

# 50 件だけ consume（lag を 50 残す）
kubectl exec -n kafka kafka-cluster-kafka-0 -c kafka -- bin/kafka-console-consumer.sh \
  --bootstrap-server localhost:9092 --topic test-topic \
  --group test-group --from-beginning --max-messages 50 --timeout-ms 10000

# consumer group の状態を確認
kubectl exec -n kafka kafka-cluster-kafka-0 -c kafka -- bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --describe --group test-group
```

数十秒後に kafka-exporter がメトリクスを公開し、Grafana ダッシュボードに値が表示される。

クリーンアップ：

```bash
kubectl exec -n kafka kafka-cluster-kafka-0 -c kafka -- bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --delete --topic test-topic
kubectl exec -n kafka kafka-cluster-kafka-0 -c kafka -- bin/kafka-consumer-groups.sh \
  --bootstrap-server localhost:9092 --delete --group test-group
```

> **注意**: consumer group を一度作ると Kafka が `__consumer_offsets` トピック（50 partitions）を自動作成する。これは Kafka 仕様で削除不可、永続的に残る。ダッシュボードの Topics / Partitions の総数がテスト前後で恒久的に増える点に留意。

### 9. Strimzi CR (KafkaTopic / KafkaUser) の動作確認

`Strimzi Operators` ダッシュボードの `Topic CRs` / `User CRs` カウントは Strimzi CRD で宣言したリソース数を表す。Cruise Control の auto-create トピック等は CR を経由せず Kafka API 直叩きで作成されるため、CR が無い状態（`Topic CRs: 0`）でも Kafka 自体は正常に動作する。

CR 経由の管理メリット（GitOps、ドリフト検知、K8s RBAC 連携、宣言的削除）を活かしたい場合は以下の YAML を `kubectl apply` する。

#### KafkaTopic CR サンプル

```yaml
apiVersion: kafka.strimzi.io/v1
kind: KafkaTopic
metadata:
  name: sample-events           # この名前がそのまま Kafka topic 名になる
  namespace: kafka
  labels:
    strimzi.io/cluster: kafka-cluster   # 対象クラスタ名
spec:
  partitions: 6
  replicas: 3
  config:
    retention.ms: 604800000     # 7 日
    cleanup.policy: delete
```

#### KafkaUser CR サンプル（mTLS 認証）

```yaml
apiVersion: kafka.strimzi.io/v1
kind: KafkaUser
metadata:
  name: sample-app              # この名前で Secret も生成される
  namespace: kafka
  labels:
    strimzi.io/cluster: kafka-cluster
spec:
  authentication:
    type: tls
  # 必要なら ACL も宣言できる（cluster 側で authorization 有効化が前提）
  # authorization:
  #   type: simple
  #   acls:
  #     - resource: { type: topic, name: sample-events }
  #       operations: [Read, Write]
```

#### 動作確認手順

```bash
# 1. CR を apply
kubectl apply -f sample-kafka-topic.yaml
kubectl apply -f sample-kafka-user.yaml

# 2. CR が READY になるか確認（数秒〜十数秒）
kubectl get kafkatopic,kafkauser -n kafka
# NAME            CLUSTER         PARTITIONS   REPLICATION FACTOR   READY
# sample-events   kafka-cluster   6            3                    True
# NAME         CLUSTER         AUTHENTICATION   AUTHORIZATION   READY
# sample-app   kafka-cluster   tls                              True

# 3. Topic Operator が Kafka API に同期したか確認
kubectl exec -n kafka kafka-cluster-kafka-0 -c kafka -- bin/kafka-topics.sh \
  --bootstrap-server localhost:9092 --describe --topic sample-events
# Topic: sample-events PartitionCount: 6 ReplicationFactor: 3
# Configs: cleanup.policy=delete,retention.ms=604800000

# 4. User Operator が認証情報の Secret を生成したか確認
kubectl get secret -n kafka sample-app -o jsonpath='{.data}' \
  | python3 -c "import sys,json; print('\n'.join(json.loads(sys.stdin.read()).keys()))"
# ca.crt        ← cluster CA cert
# user.crt      ← クライアント証明書
# user.key      ← クライアント秘密鍵
# user.p12      ← PKCS12 (Java truststore/keystore 用)
# user.password ← p12 のパスワード

# 5. Grafana ダッシュボードで Topic CRs / User CRs が +1 されることを確認
#    （メトリクスは strimzi_resources{kind="KafkaTopic|KafkaUser"}）

# 6. クリーンアップ - CR を消すと Kafka topic も Secret も連動削除される
kubectl delete kafkatopic sample-events -n kafka
kubectl delete kafkauser sample-app -n kafka
```

> **mTLS 認証を実際に使うには**: `kafka-cluster.yaml` の external listener に `authentication: { type: tls }` を追加して再デプロイする必要がある。クライアントは `user.crt` / `user.key` を keystore にロードして接続する（`ssl.keystore.*` プロパティ）。本サンプルは CR 同期動作の確認用途のため、認証なしの listener のままでも CR 自体は READY になり、Secret も生成される（ただし接続には使えない）。

> **ACL を使うには**: `kafka-cluster.yaml` の `spec.kafka` に `authorization: { type: simple }` を追加して再デプロイする必要がある。

### 10. Fluent Bit → CloudWatch Logs

コンテナログが `/aws/eks/<cluster>/application` Log Group に流入するか確認する。

```bash
LG=/aws/eks/eks-cluster-dev/application

# Log Group 存在確認
aws logs describe-log-groups --log-group-name-prefix "$LG" \
  --query 'logGroups[*].[logGroupName,retentionInDays]' --output table

# 最近作成された log stream（pod 単位で 1 stream）
aws logs describe-log-streams --log-group-name "$LG" \
  --order-by LastEventTime --descending --max-items 5 \
  --query 'logStreams[*].[logStreamName,lastEventTimestamp]' --output table

# 直近 5 分のログを確認
aws logs filter-log-events --log-group-name "$LG" \
  --start-time $(($(date +%s%3N) - 300000)) \
  --max-items 5 --query 'events[*].message' --output text
```

各ログには `kubernetes.{namespace,pod_name,labels,annotations}` が付与される（Fluent Bit kube parser が補完）。pod を namespace 別にフィルタしたい場合は `aws logs filter-log-events --filter-pattern '{ $.kubernetes.namespace_name = "kafka" }'` を使う。

### 11. Cruise Control リバランス

`KafkaRebalance` CR でパーティション再配置の提案を生成・承認する。

```bash
# 提案生成（数十秒）
kubectl apply -f manifests/kafka/kafka-rebalance.yaml
kubectl get kafkarebalance -n kafka
# STATUS: PendingProposal -> ProposalReady

# 提案内容を確認（dataToMoveMB / numReplicaMovements 等）
kubectl get kafkarebalance kafka-rebalance -n kafka -o yaml | yq '.status.optimizationResult'

# 承認 → 実行
kubectl annotate kafkarebalance kafka-rebalance \
  strimzi.io/rebalance=approve --overwrite -n kafka
kubectl get kafkarebalance -n kafka -w
# STATUS: Rebalancing -> Ready

# クリーンアップ
kubectl delete kafkarebalance kafka-rebalance -n kafka
```

> 既にバランスが取れたクラスターでは `dataToMoveMB: 0` で `Rebalancing` 後すぐ `Ready` になる（warning が出ても正常）。実データが流れてからリバランスすることが本来の用途。

### 12. Strimzi Cluster Operator HA

`replicas: 2` 構成で Leader Lease による leader election が動いているか検証する。

```bash
# 2 pod が異なるノード（できれば異 AZ）に配置されていること
kubectl get pods -n strimzi-system -l strimzi.io/kind=cluster-operator -o wide

# 現在の leader を確認
kubectl get lease strimzi-cluster-operator -n strimzi-system \
  -o jsonpath='{.spec.holderIdentity}{"\n"}'

# leader を強制終了 → standby が即座に leader 昇格
LEADER=$(kubectl get lease strimzi-cluster-operator -n strimzi-system \
  -o jsonpath='{.spec.holderIdentity}')
kubectl delete pod $LEADER -n strimzi-system --grace-period=0 --force

# 数秒後、新しい leader holder を確認
sleep 5
kubectl get lease strimzi-cluster-operator -n strimzi-system \
  -o jsonpath='{.spec.holderIdentity}{"\n"}'
# → 別の pod 名になっていれば leader 切替成功

# reconciliation が継続しているか
NEW_LEADER=$(kubectl get lease strimzi-cluster-operator -n strimzi-system \
  -o jsonpath='{.spec.holderIdentity}')
kubectl logs -n strimzi-system $NEW_LEADER --tail=5 | grep -i reconcil
```

### 13. Strimzi cluster CA cert 状態 / ローテーション

Strimzi が自動発行する cluster CA / clients CA の有効期限と自動更新ポリシーを確認する。

```bash
# CA Secret 一覧
kubectl get secret -n kafka kafka-cluster-cluster-ca-cert kafka-cluster-clients-ca-cert

# 有効期限を確認
kubectl get secret -n kafka kafka-cluster-cluster-ca-cert \
  -o jsonpath='{.data.ca\.crt}' | base64 -d | openssl x509 -noout -dates -subject
# notBefore=... notAfter=...
# subject=O=io.strimzi, CN=cluster-ca v0
```

**自動更新ポリシー（デフォルト）**

| 項目 | 値 | 備考 |
|------|-----|------|
| `generateCertificateAuthority` | `true` | Strimzi が CA を自動生成 |
| 有効期限 | 365 日 | `clusterCa.validityDays` で変更可 |
| 自動更新トリガー | 期限 30 日前 | `clusterCa.renewalDays` で変更可 |
| 更新方式 | 期限内に新 CA を生成、新旧並行運用、ブローカー rolling restart | クライアントは新 `ca.crt` を取得して truststore を更新する必要あり |

**手動ローテーション**（CA 漏洩等）：

```bash
# 強制更新を要求
kubectl annotate secret kafka-cluster-cluster-ca-cert \
  -n kafka strimzi.io/force-renew=true

# Strimzi が新 CA を生成 → broker rolling restart
# クライアントは新 ca.crt を再取得して truststore を更新
kubectl get secret kafka-cluster-cluster-ca-cert -n kafka \
  -o jsonpath='{.data.ca\.crt}' | base64 -d > new-ca.crt
```

### 14. PrivateLink クロス VPC 接続（クライアント VPC 側手順）

別 VPC や別アカウントから NLB へ接続する手順。実機検証はクライアント側 VPC が必要なため、本リポジトリ側では Endpoint Service の状態確認までを実施する。

```bash
# サービス側（このリポジトリ）の Endpoint Service 名を取得
aws ec2 describe-vpc-endpoint-service-configurations \
  --query "ServiceConfigurations[?starts_with(ServiceName, 'com.amazonaws.vpce') && contains(NetworkLoadBalancerArns[0], 'kafka-shared-nlb')].[ServiceName,ServiceState,AcceptanceRequired]" \
  --output table
# 例: com.amazonaws.vpce.ap-northeast-1.vpce-svc-xxxxxx | Available | False

# NLB DNS（advertised host）取得
aws cloudformation describe-stacks --stack-name EksCdkStack \
  --query 'Stacks[0].Outputs[?OutputKey==`KafkaNlbDnsName`].OutputValue' --output text
```

**クライアント VPC 側の作業**

1. **VPC Endpoint（Interface）作成**：上記 ServiceName を指定して接続。`AcceptanceRequired: false` のため即時接続。サブネットは Kafka が公開している 3 AZ と一致させる。
2. **Route53 Private Hosted Zone**：NLB DNS（`kafka-shared-nlb-xxx.elb.ap-northeast-1.amazonaws.com`）を Endpoint の DNS 名に向けるエイリアスレコードを作成。これでクライアントの `bootstrap.servers=<NLB DNS>:9094` がクライアント VPC 内の Endpoint ENI に解決される。
3. **Kafka client**：§7 の `kafka-client-test` 同様、cluster CA cert（`kafka-cluster-cluster-ca-cert` Secret）を取得した上で `bootstrap.servers=<NLB DNS>:9094` で接続。

> **重要**: NLB の DNS 名は変えない。Strimzi が advertised host として broker 設定に焼き込んでいるため、クライアントは必ずこの DNS 名で接続を試みる。クライアント VPC では Route53 でこの DNS 名を Endpoint ENI に向け直す必要がある。

### 15. Node Auto Repair 設定確認

EKS Managed NodeGroup の `enable_node_auto_repair=True`（CDK）が有効か確認する。

```bash
for ng in system-nodegroup kafka-nodegroup; do
  aws eks describe-nodegroup --cluster-name eks-cluster-dev --nodegroup-name $ng \
    --query 'nodegroup.{NodeRepair:nodeRepairConfig.enabled, Health:health.issues}' --output yaml
done
```

**トリガー条件（EKS が自動修復するケース）**

| 条件 | EKS の対応 |
|------|----------|
| kubelet が `NotReady` 状態を一定時間継続 | ノードを drain → 終了 → ASG が新規ノード起動 |
| 過剰な再起動 / kubelet 通信不能 | 同上 |
| EC2 ヘルスチェック失敗 | ASG の標準動作で自動入れ替え |

実発火の検証は kubelet 強制停止などの破壊的操作が必要なため、本番環境で以下を再現することは非推奨。設定が `enabled: true` であることの確認に留める。

### 16. 災害復旧（DR）

#### EBS Snapshot 取得手順（運用バックアップ）

broker / controller の PV を EBS スナップショットとしてバックアップする。

```bash
# broker-0 PV の EBS Volume ID を取得
PV_HANDLE=$(kubectl get pv -o jsonpath='{range .items[*]}{.spec.csi.volumeHandle},{.spec.claimRef.name}{"\n"}{end}' \
  | grep "data-0-kafka-cluster-kafka-0" | cut -d, -f1)

# Snapshot 取得
aws ec2 create-snapshot --volume-id $PV_HANDLE \
  --description "kafka-broker-0 daily backup $(date -u +%Y-%m-%dT%H:%M:%SZ)" \
  --tag-specifications 'ResourceType=snapshot,Tags=[{Key=Cluster,Value=kafka-cluster},{Key=Pod,Value=kafka-0}]'

# 全 broker / controller を一括取得（運用ではこれを cron / Lambda で日次実行）
for n in 0 1 2; do
  for role in kafka controller; do
    pvc="data-0-kafka-cluster-${role}-${n}"
    [ "$role" = "controller" ] && pvc="data-0-kafka-cluster-controller-$((n+3))"
    vol=$(kubectl get pv -o jsonpath='{range .items[*]}{.spec.csi.volumeHandle},{.spec.claimRef.name}{"\n"}{end}' | grep ",$pvc$" | cut -d, -f1)
    [ -z "$vol" ] && continue
    aws ec2 create-snapshot --volume-id $vol --description "${pvc} backup" --tag-specifications "ResourceType=snapshot,Tags=[{Key=PVC,Value=${pvc}}]"
  done
done
```

#### Snapshot からの Restore（クラスター再構築シナリオ）

1. `cdk deploy EksCdkStack` で空の Kafka cluster を起動（Strimzi が空の PVC を作成）
2. 新しい PVC に対応する EBS Volume を、Snapshot から作成し直す（`aws ec2 create-volume --snapshot-id ...`）
3. `kubectl edit pv` で対象 PV の `spec.csi.volumeHandle` を新 Volume ID に置き換える、または `VolumeSnapshot` CRD を使う（要：snapshot-controller インストール）
4. broker pod を再起動 → 復元データから起動

> 実運用では **AWS Backup** か Velero などのツールを使う方が現実的。手動でやる場合は KRaft メタデータの整合性に注意（全 broker・全 controller を**同じ時点**の snapshot で揃える必要がある）。

#### orphan PV / EBS の掃除

`KafkaNodePool` 削除や pool 名変更で**孤立した PV / EBS Volume** が残る場合がある。`deleteClaim: false`（stg/prd 設定）では特に発生しやすい。

```bash
# Released な PV を一覧
kubectl get pv | awk '$5=="Released"'

# Released PV を削除（→ Retain ポリシーのため EBS は残る）
for pv in $(kubectl get pv -o jsonpath='{range .items[?(@.status.phase=="Released")]}{.metadata.name} {end}'); do
  kubectl delete pv $pv
done

# PV 削除後の Available な EBS を一括削除（cluster タグでフィルタ）
aws ec2 describe-volumes \
  --filters "Name=status,Values=available" "Name=tag:kubernetes.io/cluster/eks-cluster-dev,Values=owned" \
  --query 'Volumes[*].VolumeId' --output text \
  | xargs -n1 aws ec2 delete-volume --volume-id
```

> dev 環境（`delete_claim: true`）では KafkaNodePool 削除時に PVC・PV・EBS まで連動削除される。stg/prd（`delete_claim: false`）はデータ保護優先で残るため、運用上は意図的なオペレーションでのみ削除する。

### よくある問題

| 症状 | 原因 |
|---|---|
| ノードが `NotReady` | VPC エンドポイント / セキュリティグループの設定ミス |
| Pod が `Pending` | ノードグループのキャパシティ不足 / Taint 未設定 |
| Kafka が `READY: False` | Strimzi Operator が起動していない / PVC 未バインド |
| NLB ターゲットが大半 `unhealthy` | `externalTrafficPolicy: Local` のため broker pod 不在ノードは正常に unhealthy（broker pod のあるノードのみ healthy 表示）|
| TLS handshake で `verify error` | `kafka-cluster-cluster-ca-cert` Secret から CA を再取得（cluster CA はローテーションされる）|

## スタック削除手順（クリーンアップ）

dev 環境を完全に破棄する場合、以下の順で実行する。実機検証で発生した詰まり所と対処もまとめている。

### 事前チェック

```bash
# PrivateLink 接続消費者がいないこと（あれば先にクライアント側 Endpoint を消す）
SVC_ID=$(aws ec2 describe-vpc-endpoint-service-configurations \
  --query "ServiceConfigurations[?contains(NetworkLoadBalancerArns[0], 'kafka-shared-nlb')].ServiceId | [0]" --output text)
aws ec2 describe-vpc-endpoint-connections --filters "Name=service-id,Values=$SVC_ID" \
  --query 'VpcEndpointConnections[*].VpcEndpointId' --output table
# → 空ならOK

# DeletionProtection が False であること（dev は False）
aws eks describe-cluster --name eks-cluster-dev --query 'cluster.deletionProtection'
# → false（stg/prd を消す場合は config.py で False にして cdk deploy してから）
```

### Pre-destroy 推奨アクション（CDK 管轄外の事前掃除）

CDK 管理リソース同士の削除順序が原因で詰まる箇所を予め掃除しておくと、`cdk destroy` が一発で通る。

```bash
# 1. AWS LBC の Validating/Mutating Webhook を先に消す
#    (理由: cdk destroy で AWS LBC pod が消えた後も webhook 設定が残って
#           TargetGroupBinding 削除を 1 時間ハングさせる)
kubectl delete validatingwebhookconfiguration aws-load-balancer-webhook 2>/dev/null
kubectl delete mutatingwebhookconfiguration aws-load-balancer-webhook 2>/dev/null

# 2. KafkaNodePool を先に削除して Strimzi に PVC/EBS を片付けさせる
#    (理由: Helm chart 削除が先行すると delete_claim:true でも Strimzi が
#           動けず EBS が orphan 化する)
kubectl delete kafkanodepool --all -n kafka --wait=false 2>/dev/null
```

### Stage 1: cdk destroy

```bash
cdk destroy EksCdkStack --force
# 連動削除: VPC / NAT GW / NLB / TargetGroup / EKS Cluster / NodeGroup /
#          IAM Role / Lambda / AMP / AMG / CloudWatch Log Group など
# 所要時間: 15〜25 分

cdk destroy IamStack --force
# eks-cluster-admin-eks-cluster-dev IAM ロール削除
```

### Stage 2: 削除中エラーの対処

| 症状 | 原因 | 対処 |
|------|------|------|
| `TargetGroupBindingXxx` が DELETE_IN_PROGRESS のまま 1 時間ハングして DELETE_FAILED | AWS LBC pod 削除後も webhook 設定が残り、`kubectl delete tgb` が webhook で永遠に待機 | 上記 Pre-destroy で先に webhook を消すか、発生後は `kubectl delete validatingwebhookconfiguration aws-load-balancer-webhook mutatingwebhookconfiguration aws-load-balancer-webhook && kubectl get tgb -A -o name \| xargs -n1 kubectl patch -n kafka --type=json -p='[{"op":"remove","path":"/metadata/finalizers"}]'` してから `cdk destroy` 再実行 |
| `KafkaNlbSg` SG 削除が `dependent object` で失敗 | NLB 削除のタイミングずれで CFN が依存解消前に SG 削除を試みる（CFN 側の eventual consistency）| 数分待って `cdk destroy` 再実行 / または `aws ec2 delete-security-group --group-id <id>` を直接叩いて再 destroy |
| `cdk destroy` が **DeletionProtection** で拒否 | stg/prd で `deletion_protection=True` のまま | `config.py` で `False` に変更 → `cdk deploy` → `cdk destroy` |
| Namespace `kafka` / `monitoring` が `Terminating` で停止 | CRD finalizer 残存 | `kubectl get ns <ns> -o json \| jq '.spec.finalizers=[]' \| kubectl replace --raw "/api/v1/namespaces/<ns>/finalize" -f -` |
| Helm release が `pending-uninstall` | 前回の helm 操作が中途終了 | `kubectl delete secret -n <ns> -l owner=helm,name=<release-name>` |

### Stage 3: 手動クリーンアップ（CDK 管理外の orphan）

```bash
# (a) Lambda Log Groups（Lambda 関数を消しても Log Group は CDK では削除されない仕様）
aws logs describe-log-groups --log-group-name-prefix '/aws/lambda/EksCdkStack-' \
  --query 'logGroups[*].logGroupName' --output text \
  | tr '\t' '\n' | xargs -r -n1 aws logs delete-log-group --log-group-name

# (b) 旧 ADOT 期に作られた Container Insights Log Group（現 CDK では作成しない）
aws logs delete-log-group --log-group-name /aws/containerinsights/eks-cluster-dev/performance 2>/dev/null

# (c) orphan EBS Volume（Pre-destroy で KafkaNodePool を消し忘れた場合の保険）
aws ec2 describe-volumes \
  --filters "Name=status,Values=available" "Name=tag:kubernetes.io/cluster/eks-cluster-dev,Values=owned" \
  --query 'Volumes[*].VolumeId' --output text \
  | tr '\t' '\n' | xargs -r -n1 aws ec2 delete-volume --volume-id

# (d) ローカル kubeconfig
ACCT=$(aws sts get-caller-identity --query Account --output text)
ARN="arn:aws:eks:ap-northeast-1:${ACCT}:cluster/eks-cluster-dev"
kubectl config delete-cluster $ARN 2>/dev/null
kubectl config delete-context $ARN 2>/dev/null
kubectl config delete-user $ARN 2>/dev/null
```

### 削除完了の検証

```bash
echo "=== CFN Stacks（残るのは CDKToolkit のみ）==="
aws cloudformation list-stacks --query 'StackSummaries[?StackStatus!=`DELETE_COMPLETE` && (StackName==`EksCdkStack` || StackName==`IamStack`)]' --output table

echo "=== EKS Cluster ==="
aws eks list-clusters --query 'clusters' --output table

echo "=== EBS Volume / Snapshot ==="
aws ec2 describe-volumes --filters "Name=tag:kubernetes.io/cluster/eks-cluster-dev,Values=owned" --query 'Volumes[*].VolumeId' --output table
aws ec2 describe-snapshots --owner-ids self --filters 'Name=tag:Cluster,Values=kafka-cluster' --query 'Snapshots[*].SnapshotId' --output table

echo "=== NLB / VPC Endpoint Service ==="
aws elbv2 describe-load-balancers --query 'LoadBalancers[?LoadBalancerName==`kafka-shared-nlb`]' --output table
aws ec2 describe-vpc-endpoint-service-configurations --output table

echo "=== AMP / AMG Workspaces ==="
aws amp list-workspaces --query 'workspaces[?alias==`eks-cluster-dev`]' --output table
aws grafana list-workspaces --query 'workspaces[?name==`eks-cluster-dev-grafana`]' --output table

echo "=== CloudWatch Log Groups / IAM Roles ==="
aws logs describe-log-groups --query 'logGroups[?contains(logGroupName, `EksCdkStack`) || contains(logGroupName, `eks-cluster-dev`)].logGroupName' --output table
aws iam list-roles --query 'Roles[?contains(RoleName, `eks-cluster-admin-eks-cluster-dev`) || starts_with(RoleName, `EksCdkStack`)].RoleName' --output table
```

すべて空であれば `eks-cluster-dev` 関連リソースは完全削除されている。

## 監視対象メトリクス

| 収集元 | メトリクス内容 | 送信先 |
|---|---|---|
| Kafka Exporter（Strimzi 組み込み）| Consumer Lag / Topic Partition Offset | AMP（PodMonitor `kafka-resources-metrics` 経由）|
| JMX Exporter（Strimzi 組み込み）| ブローカー JMX（kafka_*）/ JVM（jvm_memory_bytes_used 等）| AMP（同上）|
| Strimzi Cluster Operator | Reconciliation 件数・エラー率 | AMP（PodMonitor `cluster-operator-metrics`）|
| Strimzi Entity Operator | Topic/User Operator 健全性 | AMP（PodMonitor `entity-operator-metrics`）|
| node-exporter（DaemonSet）| Node OS（CPU / Mem / Disk / Net）| AMP（kube-prometheus-stack 内蔵）|
| kube-state-metrics | K8s リソース状態（Pod / Node / PV 等）| AMP（同上）|
| kubelet / cAdvisor | コンテナリソース使用率・Volume stats | AMP（kubelet ServiceMonitor）|
| Fluent Bit | コンテナログ | CloudWatch Logs (`/aws/eks/<cluster_name>/application`) |
| EC2 Basic monitoring | EC2 ハードウェア状態（CPU / Network / Status）| CloudWatch Metrics（自動、追加設定不要）|

> **CloudWatch Container Insights は採用していない**（data-on-eks 方針に揃え、メトリクスは Prometheus エコシステムに集約）。詳細は data-on-eks リファレンス参照。

## ディレクトリ構成

```
ekscdk/
├── config.py             # 環境別 ClusterConfig (for_dev / for_stg / for_prd)
├── iam_stack.py          # IamStack（cluster-admin ロール）
├── ekscdk_stack.py       # EksCdkStack（メインスタック）
└── constructs/
    ├── _manifest.py      # YAML ロード / kafka-cluster.yaml パース
    ├── network.py        # VPC / Shared NLB / TargetGroup / Listener / Endpoint Service
    ├── eks_cluster.py    # EKS クラスター（CfnCluster override）/ ノードグループ
    ├── addons.py         # EKS マネージドアドオン / Strimzi / AWS Load Balancer Controller
    ├── kafka.py          # Kafka Namespace / NodePool / CR / TargetGroupBinding
    └── monitoring.py     # AMP / AMG / kube-prometheus-stack / Fluent Bit / Pod Identity
                          # / Strimzi 系 PodMonitor

manifests/
├── addons/
│   ├── aws-lbc-iam-policy.json  # AWS LBC 公式 IAM ポリシー
│   └── gp3-storageclass.yaml    # gp3 StorageClass
├── kafka/
│   ├── namespace.yaml            # kafka namespace（labels: name=kafka）
│   ├── kafka-cluster.yaml        # Kafka CR（ブローカー設定の唯一の真実の源）
│   ├── node-pool-broker.yaml     # KafkaNodePool（broker、cross-pool AntiAffinity）
│   ├── node-pool-controller.yaml # KafkaNodePool（controller）
│   ├── cm.yaml                   # JMX exporter ルール ConfigMap（JVM + Kafka）
│   ├── kafka-rebalance.yaml      # Cruise Control リバランス定義
│   ├── kafka-pod-monitor.yaml    # Kafka resources PodMonitor（broker/controller/exporter/cc）
│   ├── cluster-operator-pod-monitor.yaml  # Strimzi Cluster Operator PodMonitor
│   ├── entity-operator-pod-monitor.yaml   # Strimzi Entity Operator PodMonitor
│   └── target-group-binding.yaml # AWS LBC 用 TargetGroupBinding（テンプレート、bootstrap+broker N 適用）
└── monitoring/
    ├── namespace.yaml                          # monitoring namespace
    ├── kube-prometheus-stack-values.yaml       # Helm values（AMP remote_write / SigV4）
    ├── fluent-bit-values.yaml                  # Helm values（CloudWatch Logs 出力）
    └── dashboards/                              # AMG インポート用 Grafana ダッシュボード（手動 import）
```
