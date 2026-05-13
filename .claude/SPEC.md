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

### メトリクス収集は in-cluster Prometheus（AMP は採用しない）
kube-prometheus-stack 同梱の **in-cluster Prometheus + Prometheus Operator** を有効化し、scrape と長期保存を cluster 内で完結させる。当初は AMP Managed Scraper を使っていたが、active series 数 × scrape 回数で効く ingestion 課金が月数百ドル級まで膨らむため self-host に切り戻した（コミット履歴: `feat(monitoring): in-cluster Prometheus + Operator + PodMonitor を復活させる` → `refactor(monitoring)!: AMP Workspace / Scraper / Rule Manager と関連リソースを削除する`）。代わりに StatefulSet + PVC の運用負担と、AZ 単一障害でメトリクスが数分欠ける可能性を許容する。

### Prometheus は 2 replica HA / retention 15 日 / gp3 PVC 20Gi（各 replica）
`prometheus.prometheusSpec` で `replicas: 2`、`retention: 15d`、`storageSpec.volumeClaimTemplate` に `storageClassName: gp3` の PVC 20Gi を割り当てる。2 replica は同じ scrape を 2 本独立に走らせる active-active 方式で、片方の Pod や AZ が落ちてもメトリクスが完全には欠けない。Service が 2 Pod 間でロードバランスするため Grafana 側の datasource 設定変更は不要。データの重複排除（Thanos / Mimir 等）は導入しない: 短期保存のダッシュボード用途では 2 系列の値が同等で実害が無く、追加コンポーネントの運用コストを払う動機が薄い。`topologySpreadConstraints` で `topology.kubernetes.io/zone` を `maxSkew: 1` / `whenUnsatisfiable: ScheduleAnyway` 指定し、2 replica が別 AZ に乗るよう促す（DoNotSchedule にすると Node 不足時に Pod Pending → 監視欠損に直結するため避ける）。retention 15 日は Prometheus 公式デフォルトに揃え、SLO 月次レポートが要れば段階的に伸ばす（PVC 容量は容量見積りに連動して引き上げる）。容量試算: 60s scrape × 約 70K series × 15 日 = 圧縮後 ~8GB。20Gi は約 2.5 倍のヘッドルームで、broker 増設や JMX 追加で series が伸びても対応できる余地。各 replica が独立に保管するため EBS 代は replicas 数に比例（20Gi × 2 = 40Gi）。Alertmanager は通知先要件が出るまで `enabled: false` のまま。`scrape_interval` は chart デフォルトの 30s（PodMonitor 個別に上書き可）。

### Prometheus Operator が scrape を管理（PodMonitor / ServiceMonitor CRD ベース）
scrape は Prometheus Operator が PodMonitor / ServiceMonitor CRD を読み取って Prometheus 内部の scrape config に翻訳する方式。chart 同梱の ServiceMonitor（kube-state-metrics / node-exporter / kubelet / cAdvisor / kube-apiserver / coredns / kube-proxy / grafana 等）はそのまま動く。Strimzi 系は自前の PodMonitor 3 件 (`manifests/kafka/{kafka,cluster-operator,entity-operator}-pod-monitor.yaml`) を CDK が apply する。CRD ベースなので `pod` / `node` / `endpoint` / `container` / `namespace` 等の標準ラベルは Operator 翻訳時に自動付与され、chart 同梱の recording rule（`sum by (namespace, pod) ...`）と dashboard が無改造で動く。chart 同梱で EKS では scrape 不可なため対象外: `kube-controller-manager` / `kube-scheduler` / `kube-etcd`（EKS マネージドコントロールプレーンは外部 scrape 不可、ServiceMonitor は残るが endpoint が無いため scrape は失敗扱い）。

### Grafana は in-cluster Prometheus を datasource として直接 query
chart デフォルトの datasource（`name: Prometheus`、`isDefault: true`、`url: http://<release>-kube-prometheus-stack-prometheus:9090`）をそのまま使う。Strimzi 公式 dashboard を含む既存 dashboard が `Prometheus` 名でハードコード参照するため命名は維持。Grafana の Pod Identity は付与するが managed policy は何も attach しない（in-cluster 通信のみで AWS API を叩かないため IAM 不要）。Grafana ダッシュボードは `monitoring/dashboards/*.yaml` を ConfigMap ラベル `grafana_dashboard=1` で sidecar に自動取り込み。アクセスは `kubectl port-forward svc/<release>-grafana -n monitoring 3000:80`。

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
