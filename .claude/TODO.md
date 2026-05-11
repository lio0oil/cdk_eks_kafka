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

## コード整理（低優先）

- **`eks-pod-identity-agent` Addon の `AddonVersion` を pin する**
  - 現状: `aws_eks_v2.Cluster` が `IdentityType.POD_IDENTITY` 用に自動追加する Addon で、`AddonVersion` 未指定 → latest 追従。他 6 addon は [ekscdk/config.py:17-24](ekscdk/config.py#L17-L24) の `_ADDON_VERSIONS_K8S_135` で pin しているのに不揃い。`config.addon_versions` に項目を追加し、`eks.Addon` で明示作成して上書きする。
- **`# type: ignore[union-attr]` を `cast(eks.CfnCluster, ...)` で整理する**
  - 現状: [ekscdk/constructs/eks_cluster.py:48-57](ekscdk/constructs/eks_cluster.py#L48-L57) に 3 箇所、[ekscdk/constructs/addons.py:62](ekscdk/constructs/addons.py#L62) に 1 箇所。pyright が実コードを見るようになった今、`cast(eks.CfnCluster, self._cluster.node.default_child)` で型を絞れば ignore コメントを消せる。