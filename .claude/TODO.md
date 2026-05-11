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
- **Prometheus の `prometheusSpec.storageSpec` を決定する**
  - 現状: [manifests/monitoring/kube-prometheus-stack-values.yaml](manifests/monitoring/kube-prometheus-stack-values.yaml) で未指定 → emptyDir、Pod 再起動で直近メトリクスが消える。
- **Prometheus の `retention` を明示する**
  - 現状: chart デフォルトのまま。リファレンスは 30d。
- **Prometheus の `scrapeInterval` を明示する**
  - 現状: chart デフォルトのまま。リファレンスは 30s。
- **Grafana の `adminPassword` を Secrets Manager / `existingSecret` 参照に切り替える**
  - 現状: [manifests/monitoring/kube-prometheus-stack-values.yaml:12](manifests/monitoring/kube-prometheus-stack-values.yaml#L12) で平文 `admin`（dev 用、本番で持ち出してはいけない）。
- **IamStack の `admin_principal` を `CompositePrincipal` で明示ロール列挙に切り替える**
  - 現状: [app.py:35](app.py#L35) で `iam.AccountRootPrincipal()`（同一アカウント全 IAM Principal が AssumeRole 可能）。本番では運用者・CI/CD ロールを ARN で限定する必要がある。

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