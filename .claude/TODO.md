# TODO

プロジェクトに残っている課題の一覧。優先度が高いものを上に置く。1 項目につき 1 アクション。

## stg / prd 運用前に必ず決める必要がある事項

dev では暫定値で動くが、本番で同じ値を使ってはいけない設定。

- **broker の `resources.requests/limits` を環境別に上書きできるようにする**
  - 現状: [manifests/kafka/node-pool-broker.yaml](manifests/kafka/node-pool-broker.yaml) で memory 2-4Gi / CPU 250-500m 固定（dev 想定）。
- **broker の `storage.size` を環境別に上書きできるようにする**
  - 現状: 20Gi 固定（dev 想定）。
- **broker の `jvmOptions`（`-Xms` / `-Xmx`）を環境別に上書きできるようにする**
  - 現状: 1G / 2G 固定（dev 想定）。
- **controller の `resources.requests/limits` を環境別に上書きできるようにする**
  - 現状: [manifests/kafka/node-pool-controller.yaml](manifests/kafka/node-pool-controller.yaml) で memory 1-2Gi / CPU 250-500m 固定（dev 想定）。
- **controller の `storage.size` を環境別に上書きできるようにする**
  - 現状: 20Gi 固定（dev 想定）。
- **controller の `jvmOptions`（`-Xms` / `-Xmx`）を環境別に上書きできるようにする**
  - 現状: 512M / 1G 固定（dev 想定）。
- **Prometheus の `prometheusSpec.storageSpec` を環境別に上書きできるようにする**
  - 現状: [manifests/monitoring/kube-prometheus-stack-values.yaml](manifests/monitoring/kube-prometheus-stack-values.yaml) で未指定 → emptyDir、Pod 再起動で直近メトリクスが消える。
- **Prometheus の `retention` を環境別に上書きできるようにする**
  - 現状: chart デフォルトのまま。リファレンスは 30d。
- **Prometheus の `scrapeInterval` を環境別に上書きできるようにする**
  - 現状: chart デフォルトのまま。リファレンスは 30s。
- **alertmanager 導入の設計を確定させる**（採用前提、運用要件として確定）
  - 現状: [manifests/monitoring/kube-prometheus-stack-values.yaml:59-60](manifests/monitoring/kube-prometheus-stack-values.yaml#L59-L60) で `enabled: false`。stg/prd 移行前までに以下を決定して有効化する。(1) Strimzi 公式 `examples/metrics/prometheus-rules.yaml` を起点とした PrometheusRule の追加（`KafkaUnderReplicatedPartitions` / `KafkaOfflinePartitions` / `KafkaControllerOffline` / `KafkaConsumerLagHigh` 等。閾値は dev 観測値から決める）。(2) receiver と severity 別 routing の `alertmanager.config` 定義（最低 1 経路は確保）。(3) webhook URL / API key を Secrets Manager + External Secrets Operator 経由で `alertmanager.configSecret` に渡す仕組み（chart の平文 values は使わない）。(4) `alertmanager.alertmanagerSpec.replicas: 3` + AZ 分散 `topologySpreadConstraints` + PDB `maxUnavailable: 1` の HA 構成（gossip cluster 標準サイズ）。PVC は notification log / silence 状態保存用に各 replica へ `gp3` 1Gi 程度。
  - 関連: Prometheus Operator が停止しても既存 PrometheusRule の評価と Alertmanager 配送は継続するため、`prometheusOperator.replicas` の HA 化は本項目より優先度が低い。
- **Grafana の `adminPassword` を環境別に上書きできるようにする**
  - 現状: [manifests/monitoring/kube-prometheus-stack-values.yaml:12](manifests/monitoring/kube-prometheus-stack-values.yaml#L12) で平文 `admin`（dev 用、本番で持ち出してはいけない）。stg/prd は Secrets Manager / `existingSecret` 参照に切り替える。
- **IamStack の `admin_principal` を `CompositePrincipal` で明示ロール列挙に切り替える**
  - 現状: [app.py:35](app.py#L35) で `iam.AccountRootPrincipal()`（同一アカウント全 IAM Principal が AssumeRole 可能）。本番では運用者・CI/CD ロールを ARN で限定する必要がある。
- **`endpoint_access` を env 別に制御する**
  - 現状: [ekscdk/constructs/eks_cluster.py:34](ekscdk/constructs/eks_cluster.py#L34) で全環境 `PUBLIC_AND_PRIVATE` 固定。stg/prd は `PRIVATE` のみが望ましい（kubectl 経由の管理通信が public network から到達可能な現状は本番方針として弱い）。`config.endpoint_access` を追加して env 別に切り替える。
- **`bootstrap_cluster_creator_admin_permissions` を env 別に制御する**
  - 現状: [ekscdk/constructs/eks_cluster.py:35](ekscdk/constructs/eks_cluster.py#L35) で全環境 `True`。`cdk deploy` を実行した IAM identity（CI/CD ロール等）が自動で cluster-admin になる。IamStack の admin_role を AccessEntry で渡している設計と矛盾するため、prd では `False` にして AccessEntry 経由のみに統一する。

## 容量・規模が増えた段階で対処する項目

「いま動かなくはないが、ワークロードが増えると引っかかる」もの。Pod IP 逼迫を検知したら上から順に手を打つ。

- **`vpc-cni` Addon の `configuration_values` に `ENABLE_PREFIX_DELEGATION=true` を入れる**
  - 一次対処。Pod IP 上限を 1 ENI あたり数十倍に拡張。[ekscdk/constructs/addons.py](ekscdk/constructs/addons.py)。
- **VPC に secondary CIDR (`100.64.0.0/x`) を追加する**
  - Prefix Delegation でも足りない場合の二次対処。[ekscdk/constructs/network.py](ekscdk/constructs/network.py)。
- **IPv6 dualstack の採用是非を決定する**
  - 現状: VPC は IPv4 単一スタック。dualstack 化すれば Pod IP 枯渇耐性が上がるが、PrivateLink / クライアント側ネットワーク・NLB の IPv6 対応前提が必要になる。Prefix Delegation / secondary CIDR と並ぶ選択肢として、採用するかどうかを事前に判断しておく。

## 既知の未検証ポイント

リポジトリ自体は synth まで通っているが、運用面で未検証の領域。

- **broker_count 変更時のローリング挙動を実機検証する**
  - [manifests/kafka/kafka-cluster.yaml](manifests/kafka/kafka-cluster.yaml) の `configuration.brokers` を編集 → `cdk deploy` で broker が正しく増減するか。
- **NLB の論理 ID 固定を CDK assertions テストで検証する**
  - NLB を再作成すると ARN が変わって PrivateLink が壊れるが、現状リグレッション検知テストは無し。
- **dev / stg 環境での synth が通ることをテストで固定する**
  - 現状: [tests/unit/test_ekscdk_stack.py](tests/unit/test_ekscdk_stack.py) の fixture は `ClusterConfig.for_prd()` 中心で、env 別 config 差（`deletion_protection` / `nat_gateways` / `enable_interface_endpoints` / `enable_vpc_flow_logs`）が CFN にどう反映されるかは未検証。dev/stg を synth する最小 smoke を 2 件足して env を持ち込んだ瞬間に synth が落ちる事故を防ぐ。

## セキュリティ整理

- **`cdk.context.json` の AWS account id 露出を解消する**
  - 現状: [cdk.context.json](cdk.context.json) に `availability-zones:account=<account-id>:region=...` の形で account id が平文記録されている（memory `security_no_aws_ids_in_git.md` の方針に違反）。対処は (1) `cdk.context.json` を `.gitignore` 化して各環境で個別 lookup させる、または (2) `cdk.json` / `-c` で context 値を外から注入して lookup を回避する、のいずれか。

## 認証・暗号化の本番方針

dev では「external listener (9094) は平文 + 認証なし」で動かす。internal listener の plain/tls と broker 間通信（Strimzi 自動 TLS）はそのまま。

- **本番で external listener に TLS を入れるかどうか決定する**
  - 現状: external listener は dev 検証のために [manifests/kafka/kafka-cluster.yaml](manifests/kafka/kafka-cluster.yaml) で `tls: false`（NLB → broker まで平文）。本番化で TLS を入れる場合、(a) Kafka 終端（broker 側で TLS、Strimzi 自己署名 CA をクライアントに配布。証明書配布の運用負荷あり、mTLS 認証が使える）か (b) NLB 終端（NLB に ACM 証明書を attach、broker 数 + 1 個の TLS listener を NLB に作る、カスタムドメイン要、mTLS は不可で SASL 認証に限定）かを選ぶ必要がある。Strimzi 標準は (a)、SaaS 流は (b)。
- **SASL/SCRAM 認証を導入する**
  - 現状: 全 listener で `authentication` 未指定 = 認証なし（[manifests/kafka/kafka-cluster.yaml](manifests/kafka/kafka-cluster.yaml)）。anonymous Kafka は VPC 境界のみで守られている状態で、複数チーム・複数アプリが使う段階の前に SASL/SCRAM を入れる必要がある。Strimzi の `KafkaUser` CR でユーザー定義、パスワードは Secret に自動生成される。導入時は `listeners[].authentication: type: scram-sha-512` を有効化し、ACL（トピック単位の認可）も同時設計する。SCRAM はチャレンジレスポンスでパスワード自体は通信路に流れないが、偽 broker への接続を防ぐにはサーバー証明書で broker を認証する必要があるため、**TLS 導入とセット**で検討する。

