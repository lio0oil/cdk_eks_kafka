### Karpenter / ArgoCD を採用しない（インフラ管理方針）
リファレンス（data-on-eks）は Karpenter で Kafka ノードを動的プロビジョニングし、ArgoCD で manifest を同期する構成だが、本プロジェクトは両方とも採用しない。ノードは ManagedNodeGroup 固定で、broker_count に合わせて CDK が capacity を生成する。Kubernetes リソースは CDK が `cluster.add_manifest()` / `add_helm_chart()` で直接 apply する。トレードオフ: broker_count 増減のたびに `cdk deploy` が必要、GitOps による drift detection は無い。代わりに manifest と AWS リソース（NLB TargetGroup / nodegroup / IAM 等）の依存関係を CDK 内で明示的に順序制御できる。

### EKS Auto Mode は採用しない
Auto Mode は EKS が compute / storage / networking のアドオンと Karpenter ベースのノードプロビジョニングをマネージドで提供するモードだが、本プロジェクトは採用しない。理由は (1) Auto Mode 利用時の管理プレミアム（EC2 時間課金への上乗せ）でコストが見合わない、(2) ManagedNodeGroup + 自前 Addon 構成のほうが broker / controller を別 nodegroup に分けて taint や instance type を細かく制御できる、の 2 点。将来 Karpenter を導入する場合も Helm で自前デプロイする前提で、Auto Mode 経由では入れない。

### VPC は単一 10.0.0.0/16 で暫定
`NetworkConstruct` の VPC は `10.0.0.0/16` を public / private で `/24` に切るシンプル構成。リファレンスの secondary CIDR (`100.64/x`) + IPv6 dualstack + vpc-cni `ENABLE_PREFIX_DELEGATION=true` は未検討で、Pod IP の上限を意図的に評価したわけではない。broker_count / workload 増加で Pod IP が逼迫した時点で、`vpc-cni` Addon の `configuration_values` に `ENABLE_PREFIX_DELEGATION=true` を入れる → 必要なら secondary CIDR を追加する、の順で対処する。

### monitoring Namespace は CDK が管理する
`MonitoringConstruct` 内の `monitoring` Namespace は `manifests/` ではなく `cluster.add_manifest()` で CDK が直接管理している。IRSA の `add_service_account()` は kubectl apply 時に Namespace が存在していないと失敗するため、ArgoCD に任せると順序が保証できない。

### メトリクススタックは kube-prometheus-stack
当初 ADOT (opentelemetry-collector) を採用していたが、Strimzi 標準ダッシュボードが kube-prometheus-stack の標準ラベル（`strimzi_io_*` / `kubelet_volume_stats_*` 等）に依存していたため kube-prometheus-stack に置き換えた。Prometheus が AMP に SigV4 remote_write、self-hosted Grafana（kube-prometheus-stack 同梱）が AMP を SigV4 で query する構成。Grafana へは `kubectl port-forward svc/<release>-grafana -n monitoring 3000:80` でアクセス。

### Prometheus ローカルストレージは未検討（現状 emptyDir）
`kube-prometheus-stack-values.yaml` で `prometheus.prometheusSpec.storageSpec` を指定していないため、in-cluster Prometheus は emptyDir で動作し、Pod 再起動で直近メトリクスが消える。長期保存は AMP への remote_write に寄せているが、Prometheus 自体の永続化（gp3 PVC で直近 query 用にローカル保持するか、emptyDir のままで割り切るか）は未決定。stg/prd 運用前に方針を確定する。`retention` / `scrapeInterval` も chart デフォルトに任せている（リファレンスは 30d / 30s を明示）ため、同タイミングで合わせて見直す。

### Kafka メトリクスは PodMonitor 一つに集約
Strimzi の broker / controller / cruise-control / kafka-exporter は `kafka-pod-monitor.yaml` 一つの PodMonitor で scrape する。ServiceMonitor は持たない（同じ Pod を Service 経由でも scrape すると二重計上が発生するため）。Strimzi Cluster Operator / Entity Operator は別 namespace（strimzi-system / kafka）に居るので別 PodMonitor。data-on-eks リファレンスの構成と同方針。Grafana ダッシュボードは `monitoring/dashboards/*.yaml` を ConfigMap ラベル `grafana_dashboard=1` で sidecar に自動取り込みさせる。

### Kafka 外部接続のポート設計
`kafka-cluster.yaml` の external listener は NodePort 型。共有 NLB → NodePort → Kafka broker の経路で、bootstrap に 30094、broker 0〜2 に 30095〜30097 を使用する。advertised port（9095〜9097）はクライアントがブローカーに繋ぎ直す際のポートで NodePort とは別。`externalTrafficPolicy: Local` でクライアント送信元 IP 保持・余分なホップ排除。

### Shared NLB の ARN 固定 + AWS LBC TargetGroupBinding
NLB 本体・VPC Endpoint Service・**TargetGroup / Listener** は CDK（`NetworkConstruct`）で作成して ARN を固定する。NLB を再作成すると ARN が変わって PrivateLink が壊れるため、`NetworkConstruct` の変更は慎重に行う。Strimzi の per-broker NodePort Service とのバインドは AWS LBC の `TargetGroupBinding` CRD（`KafkaConstruct` が manifest として apply）が動的に行う。Pod 移動時にも追従。

### Strimzi の apiVersion は `kafka.strimzi.io/v1`
Strimzi 1.0.0 で `v1` が正式 API として昇格し、`v1beta2` / `v1beta1` / `v1alpha1` は廃止された。`kafka-cluster.yaml`・`node-pool-*.yaml`・`kafka-rebalance.yaml` はすべて `apiVersion: kafka.strimzi.io/v1` が正しい。`v1beta2` への変更は誤り。

### Kafka version は Strimzi のサポート最新版に追従
本プロジェクトは Strimzi 1.0.0 + Kafka 4.2.0 / `metadataVersion: 4.2-IV1` を採用。Strimzi が公式サポートする最新 Kafka 版に素直に揃える方針で、4.x 固有の特定機能を要件にしているわけではない。リファレンスは 3.9.0 / 3.9-IV0 だが、これは data-on-eks 側が古い Strimzi に追従しているためで、本プロジェクトでは新しい Strimzi を選んだ結果として Kafka も新しくなっている。Strimzi 自体をアップグレードする際に、サポート対象の最新 Kafka に合わせて `version` / `metadataVersion` も更新する。

### `kafka-cluster.yaml` が唯一の真実の源（broker 設定）
ブローカー数・ポート設定は `kafka-cluster.yaml` の external listener `configuration` が唯一の真実の源。`_manifest.py` の `parse_kafka_nlb_ports()` がこれをパースし、`NetworkConstruct` が NLB Listener / TargetGroup を生成、`BROKER_COUNT` を `node-pool-broker.yaml` の `<BROKER_REPLICAS>` プレースホルダーに注入する。ブローカーを増減する場合は `kafka-cluster.yaml` の `configuration.brokers` リストを編集するだけでよい。

### Controller 数・PVC 削除挙動・インスタンスタイプは config.py
controller の replicas（`kafka_controller_count`、デフォルト 3）と KafkaNodePool 削除時の PVC 削除挙動（`delete_claim`、dev=True / stg/prd=False）、ノードグループのインスタンスタイプ（broker 用 `kafka_broker_instance_type` と controller 用 `kafka_controller_instance_type`）は `ekscdk/config.py` の各 env factory で管理し、CDK 経由で YAML プレースホルダーや nodegroup 定義に注入する。

### KafkaNodePool のリソース指定は dev 前提のテストサイズ
`node-pool-broker.yaml` / `node-pool-controller.yaml` の `resources` / `storage` / `jvmOptions` は dev 想定で最小化されている（broker: 20Gi / 2-4Gi mem / 250-500m CPU / JVM Xms-Xmx 1-2G、controller: 20Gi / 1-2Gi mem / 250-500m CPU / JVM Xms-Xmx 512M-1G）。stg/prd 運用前に、environment-specific に上書きできる仕組み（config.py 経由のプレースホルダー注入か、env 別 YAML に分割か）を追加する予定。現状の値のまま stg/prd を運用してはいけない。

### EKS 設定は CfnCluster エスケープハッチ
`aws_eks_v2.Cluster` は `UpgradePolicy.SupportType` / `DeletionProtection` / `Logging.ClusterLogging.EnabledTypes` を直接プロパティ化していないため、`eks_cluster.py` で `cfn_cluster.add_property_override()` で設定する。`UpgradePolicy=STANDARD`（追加課金回避）、`DeletionProtection` は env 別（dev=False, stg/prd=True）。Control Plane Logs は全環境共通で `audit` / `api` / `authenticator` を有効化する: data-on-eks リファレンスが採用する `terraform-aws-modules/eks v21` の `enabled_log_types` デフォルト値と揃える方針。`controllerManager` / `scheduler` は採用しない（リファレンスも未有効化、コスト対監査価値が低い）。

### Broker と Controller は別 nodegroup に配置
EKS nodegroup を `kafka-broker-nodegroup` と `kafka-controller-nodegroup` に分離し、`node-pool-broker.yaml` / `node-pool-controller.yaml` の nodeAffinity でそれぞれ `role=kafka-broker` / `role=kafka-controller` ラベルを要求して**物理的に別ノードに配置**する。これにより (1) 1 ノード障害で broker と controller を同時に失うリスクを排除、(2) controller を broker より小型のインスタンス（`kafka_controller_instance_type`）に変更可能、の 2 点を達成する。各 nodegroup の `podAntiAffinity` は同一プール内の HA 確保（broker 同士 / controller 同士の分散）にのみ使う。

### DedicatedKafka taint で Kafka 系ノードを隔離
`kafka-broker-nodegroup` / `kafka-controller-nodegroup` には `DedicatedKafka=true:NoSchedule` taint を打ち、KafkaNodePool（broker / controller）と Strimzi の sidecar（kafka-exporter / cruise-control / entity-operator）にのみ tolerations を持たせる。`system-nodegroup` には taint を打たない: tolerations 未指定の Pod（kube-prometheus-stack / fluent-bit / AWS LBC / Strimzi Cluster Operator 等）は kafka ノードから自然に弾かれ、system 側に schedule されるため、system 側を taint で守る必要がない。

### cdk deploy はユーザーが行う
`cdk deploy` は本リポジトリの自動化対象外（ユーザーが手動で実行）。リポジトリで管理するのは synth 可能な状態までで、デプロイの判断・タイミングはユーザー側。

### 以下をリファレンスにする
/home/vscode/workspace/data-on-eks/data-stacks/
https://awslabs.github.io/data-on-eks/docs/datastacks/streaming/kafka-on-eks/infra
https://github.com/awslabs/data-on-eks
