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

- **本番で application-layer 暗号化（Kafka TLS）が必要かを決定し、必要なら external listener に TLS を入れる**
  - 現状: [manifests/kafka/kafka-cluster.yaml](manifests/kafka/kafka-cluster.yaml) の external listener は `tls: false`、NLB listener も [ekscdk/constructs/network.py](ekscdk/constructs/network.py) で `Protocol.TCP`。つまり**クライアント → NLB → broker の全 hop が Kafka プロトコル / アプリ層で平文**。ただし AWS ネットワーク層では PrivateLink 経由（クライアント → VPC Endpoint）と同一 VPC 内（NLB ↔ broker NodePort）の双方が Nitro / VPC backbone により自動暗号化されており、加えて VPC Endpoint Service + PrivateLink で到達可能なクライアントが限定されているため、**「全経路平文のまま」も一つの正解**である（盗聴・改ざんへの防御を AWS ネットワーク境界に委ねる構成）。
  - 決定事項: 監査・コンプライアンス・契約上の要件として「アプリ層での終端間暗号化」が必須かを先に確定する。
    - **不要なら**: 現状維持（external listener `tls: false` / NLB listener TCP）。ただし SASL/SCRAM を入れる場合は偽 broker 防止のためサーバー証明書が要るので、その時点で本判断は再評価。
    - **必要なら**: (a) Kafka 終端（broker 側で TLS、Strimzi 自己署名 CA をクライアントに配布。証明書配布の運用負荷あり、mTLS 認証が使える）か (b) NLB 終端（NLB に ACM 証明書を attach、broker 数 + 1 個の TLS listener を NLB に作る、カスタムドメイン要、mTLS は不可で SASL 認証に限定）かを選ぶ。Strimzi 標準は (a)、SaaS 流は (b)。
  - 証明書ライフサイクルの運用リスク（採用前に対策を設計に含めること）:
    - (a) を選ぶ場合: Strimzi の cluster CA / clients CA はデフォルト validity 365 日、`renewalDays`（既定 30 日前）で operator が自動更新するが、**(1) operator が renewal タイミングで停止 / reconcile 失敗していると CA 期限切れで全 TLS 通信停止、(2) 外部クライアントは Secret に新しい cert が入っても起動時の keystore を保持するため明示 reload しないと期限切れを迎える、(3) CA rotate はローリング再起動を誘発する**。Reloader / volume watcher 等で Secret 変更検知 → アプリ reload、operator 停止アラート、CA 期限の監視（`x509_cert_expiry` メトリクス相当）をセットで設計する。
    - (b) を選ぶ場合: ACM 証明書は AWS が DNS 検証ベースで自動更新するため、クライアント側 reload も CA 配布も不要。期限切れリスクは ACM の DNS 検証レコードが壊れた場合に限られる（DNS レコード変更時に検証用 CNAME を消さない運用ルールが必要）。**この観点では (a) より運用リスクは小さい**。
- **本番で external listener に必要な認証方式を決定して有効化する**
  - 現状: 全 listener で `authentication` 未指定 = 認証なし（[manifests/kafka/kafka-cluster.yaml](manifests/kafka/kafka-cluster.yaml)）。external listener に到達できるクライアントは VPC Endpoint Service + PrivateLink で経路限定されているのみで、クライアント個別の認証は無い。複数チーム・複数アプリが使う段階の前に「誰が接続しているか」を識別できる手段を入れる必要がある（個別ユーザー単位の audit / 失効 / ACL のため）。
  - 選択肢（Strimzi の `listeners[].authentication.type` がサポート、`KafkaUser` CR 連携可能なもの）:
    - **`tls` (mTLS)**: クライアント証明書による相互認証。`KafkaUser` で証明書発行を自動化、broker 側も Strimzi 自己署名 CA で認証される。**単独でクライアント認証 + broker 認証の両方が成立**するため SASL を別途足さなくて済む。証明書配布 / rotate / 失効の運用コストが主負担。listener 側 `tls: true` 必須。**証明書期限切れ運用リスク**: client cert もデフォルト validity 365 日で Strimzi が Secret を自動更新するが、外部クライアントが Secret 変更を検知して keystore を reload しない限り古い証明書を握ったまま突然認証失敗する。Reloader / volume watcher + アプリ側 reload 機構が前提（詳細は上の TLS 項目の (a) を選んだ場合と同じ運用課題）。
    - **`scram-sha-512` (SASL/SCRAM-SHA-512)**: ユーザー名 + パスワード方式。`KafkaUser` でユーザー定義、パスワードは Secret に自動生成。チャレンジレスポンスでパスワード自体は通信路に流れない。ただし**サーバー (broker) 認証機能は SCRAM 単体には無い**ため、偽 broker 防止には TLS（サーバー証明書）とのセット必須。証明書配布は不要。
    - **`scram-sha-256`**: 上記のハッシュ違い。新規採用は基本 512 で良い。
    - **`oauth` (SASL/OAUTHBEARER)**: OAuth 2.0 / OIDC トークン認証。既存 IdP（Cognito / Auth0 / Keycloak 等）を使いたい場合の選択肢。トークン検証用 JWKS エンドポイント等の追加設定が必要。同じく broker 認証のため TLS 必須。
    - **`plain` (SASL/PLAIN)**: パスワード平文送信。TLS が無いと素抜けで、TLS ありでも broker 側がパスワード平文を保持しないといけないため SCRAM より弱い。**選ばない**。
    - **SASL/AWS_MSK_IAM**: MSK 専用。Strimzi on EKS では不可。
  - 決定軸:
    - クライアント台数が少なく、証明書ライフサイクル管理（rotate / 失効）の運用を許容できるなら **mTLS 単独**が最もシンプル（TLS 化 + 認証が一枚で済む、cert-manager 等で自動化可能）。
    - クライアント台数が多い・人/サービス単位で発行/失効を頻繁にやりたい・既存 IdP 連携が要るなら **SCRAM + TLS** または **OAUTHBEARER + TLS**。
  - 関連: 上の「application-layer 暗号化が必要か」の判断と密結合。mTLS / SCRAM / OAUTHBEARER のいずれを選んでも TLS が前提なので、**「TLS 不要」と決めた場合はこの認証導入も再設計が必要**になる（VPC + PrivateLink の経路限定だけを認可境界として運用し続ける、という方針整理を含む）。
  - 同時に **ACL（`KafkaUser.spec.authorization`）の設計**: トピック単位の read / write / describe 等を `KafkaUser` で宣言。認証だけ入れて全 topic 全権だと意味が薄いため、認証導入時にトピック命名規約 + ACL ポリシーをセットで決める。

