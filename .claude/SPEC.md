### Karpenter / ArgoCD を採用しない（インフラ管理方針）
リファレンス（data-on-eks）は Karpenter で Kafka ノードを動的プロビジョニングし、ArgoCD で manifest を同期する構成だが、本プロジェクトは両方とも採用しない。ノードは ManagedNodeGroup 固定で、broker_count に合わせて CDK が capacity を生成する。Kubernetes リソースは CDK が `cluster.add_manifest()` / `add_helm_chart()` で直接 apply する。トレードオフ: broker_count 増減のたびに `cdk deploy` が必要、GitOps による drift detection は無い。代わりに manifest と AWS リソース（NLB TargetGroup / nodegroup / IAM 等）の依存関係を CDK 内で明示的に順序制御できる。

### EKS Auto Mode は採用しない
Auto Mode は EKS が compute / storage / networking のアドオンと Karpenter ベースのノードプロビジョニングをマネージドで提供するモードだが、本プロジェクトは採用しない。理由は (1) Auto Mode 利用時の管理プレミアム（EC2 時間課金への上乗せ）でコストが見合わない、(2) ManagedNodeGroup + 自前 Addon 構成のほうが broker / controller を別 nodegroup に分けて taint や instance type を細かく制御できる、の 2 点。将来 Karpenter を導入する場合も Helm で自前デプロイする前提で、Auto Mode 経由では入れない。

### VPC は単一 10.0.0.0/16 で /24 分割
`NetworkConstruct` の VPC は `10.0.0.0/16` を public / private で `/24` に切るシンプル構成。Pod IP が逼迫した場合の対処順序は (1) `vpc-cni` Addon の `configuration_values` に `ENABLE_PREFIX_DELEGATION=true` を入れる → (2) 必要なら secondary CIDR (`100.64.0.0/x`) を追加する。

### VPC Flow Logs は stg/prd でのみ S3 に保存
`config.enable_vpc_flow_logs` で env 別に制御する（dev=False、stg/prd=True）。dev では監査要件が薄くデータ転送・保存コストの方が大きいため無効化、stg/prd ではインシデント調査・コンプライアンス用途で必須。送り先は **S3**（CloudWatch Logs に比べ保存コストが約 20 倍安く、Athena でクエリ可能。リアルタイム閲覧は犠牲になるが監査用途では十分）、`traffic_type=ALL`（rejected だけだと不審なドロップを見逃し、accepted だけだと既知通信ノイズに埋もれるため両方記録）。S3 バケットは SSE-S3 暗号化 / public access 全遮断 / TLS 強制、removal_policy は `config.log_removal_policy` に揃える。長期保存のための Glacier 移行 lifecycle は実需が出てから足す。

### monitoring Namespace は CDK が管理する
`MonitoringConstruct` 内の `monitoring` Namespace は `manifests/` ではなく `cluster.add_manifest()` で CDK が直接管理している。IRSA の `add_service_account()` は kubectl apply 時に Namespace が存在していないと失敗するため、ArgoCD に任せると順序が保証できない。

### メトリクス収集は AMP Managed Scraper（in-cluster Prometheus を持たない）
リファレンス（data-on-eks）は kube-prometheus-stack 同梱の in-cluster Prometheus が scrape + AMP に remote_write する構成だが、本プロジェクトは **AMP Managed Scraper（agentless、AWS マネージド）** を採用し、in-cluster Prometheus / Prometheus Operator / Alertmanager は無効化している。AMP Scraper は EKS cluster の API server を使って Pod / Node を discovery し、AWS マネージドで scrape + 長期保存する。HA・パッチ・スケールはすべて AWS 側で吸収され、in-cluster の StatefulSet + PVC を抱えなくて済む。トレードオフ: scrape_interval が 30 秒以上に制限される、PodMonitor / ServiceMonitor CRD は使えない（scrape_configs YAML に統一）。EKS cluster の private endpoint access が必須で、aws_eks_v2.Cluster の AccessEntry ベース認証と組み合わせて `CfnScraper` 作成時に EKS Access Entry policy が自動生成される。

### scrape 設定は `manifests/amp/scrape-config.yaml` に集約
全 scrape job（Kafka resources / Strimzi cluster-operator / entity-operator / kube-state-metrics / node-exporter / kubelet / cAdvisor / apiserver / coredns / kube-proxy / grafana）を 1 ファイルにまとめている。`AmpScraperConstruct` が `<CLUSTER_NAME>` 置換 + base64 化して `CfnScraper.ScrapeConfiguration` に渡す。旧 PodMonitor の relabel ロジック（`labelmap __meta_kubernetes_pod_label_(strimzi_io_.+)` 等）は scrape_configs の relabel_configs に変換済み。**移行時の対象範囲**は (a) 自前 PodMonitor 3 件 (kafka-resources / cluster-operator / entity-operator) と (b) kube-prometheus-stack chart 同梱 ServiceMonitor のうち EKS で実際に scrape 可能なもの。chart 同梱で**意図的に省略**しているもの: `kube-controller-manager` / `kube-scheduler` / `kube-etcd`（EKS マネージドコントロールプレーンは外部 scrape 不可）、`alertmanager` / `prometheus` 自身 / `prometheus-operator` 自身（in-cluster Prometheus 撤去で対象消失）。**chart 同梱から拾い直した**もの: `coredns`（EKS の coredns Service は port 53 のみ公開、pod 直接 scrape で port 9153 を叩く）、`kube-proxy`（EKS Managed addon、`__address__` を `<pod_ip>:10249` に明示書換）、`grafana`（self-monitoring）。**PodMonitor 由来 job には `pod` / `node` / `endpoint` / `container` / `namespace` の標準ラベルを必ず付ける**: Prometheus Operator が PodMonitor を翻訳する際に常時付与するもので、chart 同梱 recording rule の集約キー（`sum by (namespace, pod) ...`）と dashboard ペインがこれに依存する。AMP Scraper の制約: `kubernetes_sd_config` の role に基づく selector しか使えない（PodMonitor の matchExpressions/matchLabels は relabel keep で再現）、authorization は Bearer + serviceaccount token のみ、DNS は `kubernetes.default.svc` 以外 IP 必須。

### Grafana は AMP datasource のみ（in-cluster Prometheus は無い）
kube-prometheus-stack の grafana subchart は残し、Prometheus / Operator / Alertmanager は無効化している。Grafana の datasource は AMP のみ（`isDefault: true`）で、SigV4 で query する。Grafana SA に `AmazonPrometheusQueryAccess` を Pod Identity で付与。Grafana ダッシュボードは `monitoring/dashboards/*.yaml` を ConfigMap ラベル `grafana_dashboard=1` で sidecar に自動取り込み。Grafana へは `kubectl port-forward svc/<release>-grafana -n monitoring 3000:80` でアクセス。

### Kafka 外部接続のポート設計
`kafka-cluster.yaml` の external listener は NodePort 型。共有 NLB → NodePort → Kafka broker の経路で、bootstrap に 30094、broker 0〜2 に 30095〜30097 を使用する。advertised port（9095〜9097）はクライアントがブローカーに繋ぎ直す際のポートで NodePort とは別。`externalTrafficPolicy: Local` でクライアント送信元 IP 保持・余分なホップ排除。

### Kafka 外部接続は TLS のみ（クライアント認証なし）
`kafka-cluster.yaml` の external listener は `tls: true` だが `authentication` フィールドを持たない。これは意図的な設計で、本プロジェクトはブローカーに到達できるクライアントを個別に特定する必要がない（接続経路を VPC Endpoint Service + PrivateLink で限定しているため、ネットワーク到達可能性そのものを認可の境界としている）。mTLS / SCRAM / OAUTHBEARER を導入する必要が出た時点で `authentication` を追加し、対応する `KafkaUser` CR とクライアント証明書配布を設計する。

### Shared NLB の ARN 固定 + AWS LBC TargetGroupBinding
NLB 本体・VPC Endpoint Service・**TargetGroup / Listener** は CDK（`NetworkConstruct`）で作成して ARN を固定する。NLB を再作成すると ARN が変わって PrivateLink が壊れるため、`NetworkConstruct` の変更は慎重に行う。Strimzi の per-broker NodePort Service とのバインドは AWS LBC の `TargetGroupBinding` CRD（`KafkaConstruct` が manifest として apply）が動的に行う。Pod 移動時にも追従。

### broker KafkaNodePool 名は `kafka` 固定
`node-pool-broker.yaml` の `metadata.name` は `kafka`。リファレンス（data-on-eks）は `broker` だが本プロジェクトでは変更している。理由は Strimzi が生成する per-broker Service の命名規則が `<cluster_name>-<pool_name>-<broker_id>`（本プロジェクトでは `kafka-cluster-kafka-<broker_id>`）であり、`KafkaConstruct` が TargetGroupBinding の `serviceRef.name` をこの形で組み立てているため。pool 名を変更すると Service 名解決が壊れて NLB から broker への経路が不通になる。pool 名を変えるなら `KafkaConstruct` 側の Service 名生成ロジックも同時に合わせる必要がある。controller pool は外部 NLB 経路を持たないため `controller` のままで良い。

### Strimzi の apiVersion は `kafka.strimzi.io/v1`
Strimzi 1.0.0 で `v1` が正式 API として昇格し、`v1beta2` / `v1beta1` / `v1alpha1` は廃止された。`kafka-cluster.yaml`・`node-pool-*.yaml`・`kafka-rebalance.yaml` はすべて `apiVersion: kafka.strimzi.io/v1` が正しい。`v1beta2` への変更は誤り。

### Kafka version は Strimzi のサポート最新版に追従
本プロジェクトは Strimzi 1.0.0 + Kafka 4.2.0 / `metadataVersion: 4.2-IV1` を採用。Strimzi が公式サポートする最新 Kafka 版に素直に揃える方針で、4.x 固有の特定機能を要件にしているわけではない。リファレンスは 3.9.0 / 3.9-IV0 だが、これは data-on-eks 側が古い Strimzi に追従しているためで、本プロジェクトでは新しい Strimzi を選んだ結果として Kafka も新しくなっている。Strimzi 自体をアップグレードする際に、サポート対象の最新 Kafka に合わせて `version` / `metadataVersion` も更新する。

### `kafka-cluster.yaml` が唯一の真実の源（broker 設定）
ブローカー数・ポート設定は `kafka-cluster.yaml` の external listener `configuration` が唯一の真実の源。`_manifest.py` の `parse_kafka_nlb_ports()` がこれをパースし、`NetworkConstruct` が NLB Listener / TargetGroup を生成、`BROKER_COUNT` を `node-pool-broker.yaml` の `<BROKER_REPLICAS>` プレースホルダーに注入する。ブローカーを増減する場合は `kafka-cluster.yaml` の `configuration.brokers` リストを編集するだけでよい。

### Controller 数・PVC 削除挙動・インスタンスタイプは config.py
controller の replicas（`kafka_controller_count`、デフォルト 3）と KafkaNodePool 削除時の PVC 削除挙動（`delete_claim`、dev=True / stg/prd=False）、ノードグループのインスタンスタイプ（broker 用 `kafka_broker_instance_type` と controller 用 `kafka_controller_instance_type`）は `ekscdk/config.py` の各 env factory で管理し、CDK 経由で YAML プレースホルダーや nodegroup 定義に注入する。

### EKS 設定は CfnCluster エスケープハッチ
`aws_eks_v2.Cluster` は `UpgradePolicy.SupportType` / `DeletionProtection` / `Logging.ClusterLogging.EnabledTypes` を直接プロパティ化していないため、`eks_cluster.py` で `cfn_cluster.add_property_override()` で設定する。`UpgradePolicy=STANDARD`（追加課金回避）、`DeletionProtection` は env 別（dev=False, stg/prd=True）。Control Plane Logs は全環境共通で `audit` / `api` / `authenticator` を有効化する: data-on-eks リファレンスが採用する `terraform-aws-modules/eks v21` の `enabled_log_types` デフォルト値と揃える方針。`controllerManager` / `scheduler` は採用しない（リファレンスも未有効化、コスト対監査価値が低い）。

### Broker と Controller は別 nodegroup に配置
EKS nodegroup を `kafka-broker-nodegroup` と `kafka-controller-nodegroup` に分離し、`node-pool-broker.yaml` / `node-pool-controller.yaml` の nodeAffinity でそれぞれ `role=kafka-broker` / `role=kafka-controller` ラベルを要求して**物理的に別ノードに配置**する。これにより (1) 1 ノード障害で broker と controller を同時に失うリスクを排除、(2) controller を broker より小型のインスタンス（`kafka_controller_instance_type`）に変更可能、の 2 点を達成する。各 nodegroup の `podAntiAffinity` は同一プール内の HA 確保（broker 同士 / controller 同士の分散）にのみ使う。

### DedicatedKafka taint で Kafka 系ノードを隔離
`kafka-broker-nodegroup` / `kafka-controller-nodegroup` には `DedicatedKafka=true:NoSchedule` taint を打ち、KafkaNodePool（broker / controller）と Strimzi の sidecar（kafka-exporter / cruise-control / entity-operator）にのみ tolerations を持たせる。`system-nodegroup` には taint を打たない: tolerations 未指定の Pod（kube-prometheus-stack / fluent-bit / AWS LBC / Strimzi Cluster Operator 等）は kafka ノードから自然に弾かれ、system 側に schedule されるため、system 側を taint で守る必要がない。

### system-nodegroup の desired_size は VPC の AZ 数と一致させる
`NetworkConstruct` の VPC は `max_azs=3` で 3 AZ 構成。`system_desired_size` は **3 以上**（VPC の AZ 数と一致）にする。EKS Managed Nodegroup は複数 AZ の subnet を渡されると AZ にバランスよく Node を配置するため、desired<3 だと 3 AZ のうち一部 AZ に system Node が立たない。Grafana など EBS PVC を持つ Pod が「Node がいない AZ」で PV を作ると、EBS は AZ をまたげない制約から、その PV の node affinity を満たす Node が存在せず `FailedScheduling`（`didn't match PersistentVolume's node affinity`）が発生する。`system_min_size` は env 別（dev=2 で停止/縮退時のコスト節約優先、stg/prd=3 で常時 3 AZ 配置を保証）、`system_max_size = system_desired_size + 1`（既存流儀、ローリング更新時に新 Node を立てる余裕）。kafka-broker / kafka-controller nodegroup も同様に 3 AZ 整合が前提。

### 以下をリファレンスにする
/home/vscode/workspace/data-on-eks/data-stacks/
https://awslabs.github.io/data-on-eks/docs/datastacks/streaming/kafka-on-eks/infra
https://github.com/awslabs/data-on-eks
