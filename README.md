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
4. 必要に応じて Cruise Control でパーティションをリバランスする（[手順](#cruise-control-によるリバランス)参照）

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

### 5. Grafana ダッシュボード

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

## デプロイ後の動作確認

### 1. kubeconfig 更新

```bash
aws eks update-kubeconfig --name <cluster_name> --region ap-northeast-1
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
# adot-collector-* / fluent-bit-* が Running であること
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

### よくある問題

| 症状 | 原因 |
|---|---|
| ノードが `NotReady` | VPC エンドポイント / セキュリティグループの設定ミス |
| Pod が `Pending` | ノードグループのキャパシティ不足 / Taint 未設定 |
| Kafka が `READY: False` | Strimzi Operator が起動していない / PVC 未バインド |
| NLB ターゲットが大半 `unhealthy` | `externalTrafficPolicy: Local` のため broker pod 不在ノードは正常に unhealthy（broker pod のあるノードのみ healthy 表示）|
| TLS handshake で `verify error` | `kafka-cluster-cluster-ca-cert` Secret から CA を再取得（cluster CA はローテーションされる）|

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
│   └── monitoring.py     # AMP / AMG / ADOT / Fluent Bit / Pod Identity
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
