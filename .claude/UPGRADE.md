# UPGRADE

バージョン更新のリファレンス。**コンポーネントごとに「どこで管理し・どう上げ・何が止まるか」**を恒久的にまとめる。

**現状のピン値・最新版・どれを上げるべきか（要否）は書かない**。陳腐化するため、現状値は各 `config.py` / `manifests/*.yaml`（真実の源）を、最新版は実施時に各自で確認する。表中のバージョン番号はすべて手順を示す例。

業務影響があるのは **Kafka データプレーン（クライアント → NLB/PrivateLink → broker）だけ**。broker が再起動する契機は「Kafka/Strimzi の設定変更」か「ノード入れ替え」のみで、下表の更新は末尾「事故シナリオ」を踏まない限りそれを起こさない。監視・ログの一時断は「可視性の低下」であって業務停止ではない。

## 更新対象一覧

| 種別 | 対象 | 役割（何のため） | 管理場所 | 更新方法 | CRD | 停止・影響 | 主な注意 |
|---|---|---|---|---|---|---|---|
| A. Kubernetes バージョン | クラスター（control plane + node） | クラスタ全体の土台（K8s API / スケジューラ等の版） | `eks_cluster.py` の `KubernetesVersion` 定数 | 定数変更 → `cdk deploy` | — | 全ノード置換 + **broker ローリング再起動**（要 PDB/minISR 確認）。**最も影響大・別格** | kubectl layer / 全アドオン / 全 chart が連動。単独で計画。dev はノード固定で headroom 薄い |
| B. Amazon EKS アドオン | vpc-cni | Pod に VPC の IP を割り当てる（CNI） | `config.py` の `_ADDON_VERSIONS_K8S_1xx` | `describe-addon-versions` で版確認 → 値変更 → `cdk deploy` | — | in-place 更新・原則無停止。Pod ネットワークに直結 | 本番は 1 つずつ・営業時間外 |
| B. Amazon EKS アドオン | coredns | クラスタ内の DNS 名前解決 | `config.py` の `_ADDON_VERSIONS_K8S_1xx` | `describe-addon-versions` で版確認 → 値変更 → `cdk deploy` | — | in-place 更新・原則無停止。DNS 直結（replica 2 の HA） | 本番は 1 つずつ・営業時間外 |
| B. Amazon EKS アドオン | kube-proxy | Service の仮想 IP → Pod のルーティング | `config.py` の `_ADDON_VERSIONS_K8S_1xx` | `describe-addon-versions` で版確認 → 値変更 → `cdk deploy` | — | in-place 更新・原則無停止。再起動中も iptables/ipvs ルールは残る | 本番は 1 つずつ・営業時間外 |
| B. Amazon EKS アドオン | aws-ebs-csi-driver | PVC を EBS で払い出す（broker のデータ永続化） | `config.py` の `_ADDON_VERSIONS_K8S_1xx` | `describe-addon-versions` で版確認 → 値変更 → `cdk deploy` | — | in-place 更新・原則無停止。**マウント済みボリュームに無影響** | — |
| B. Amazon EKS アドオン | metrics-server | リソースメトリクス提供（kubectl top / HPA） | `config.py` の `_ADDON_VERSIONS_K8S_1xx` | `describe-addon-versions` で版確認 → 値変更 → `cdk deploy` | — | in-place 更新・原則無停止。観測系で影響軽微 | — |
| B. Amazon EKS アドオン | eks-node-monitoring-agent | ノード障害の検知 | `config.py` の `_ADDON_VERSIONS_K8S_1xx` | `describe-addon-versions` で版確認 → 値変更 → `cdk deploy` | — | in-place 更新・原則無停止。観測系で影響軽微 | — |
| B. Amazon EKS アドオン | eks-pod-identity-agent | Pod に IAM ロールを渡す（Pod Identity） | `config.py` の `_ADDON_VERSIONS_K8S_1xx` | `describe-addon-versions` で版確認 → 値変更 → `cdk deploy` | — | in-place 更新・原則無停止。キャッシュ済み認証情報が残り影響軽微 | CFN override で版注入（`addons.py`） |
| C. Helm chart | Strimzi | Kafka クラスタを運用する operator（Kafka 本体） | `config.py` の `strimzi_version` | 値変更 → `cdk deploy` | **あり（大）→ chart 更新前に手動 SSA 必須**（付録）: `Kafka`/`KafkaNodePool`/`KafkaTopic`/`KafkaUser`/`KafkaRebalance` 等。helm upgrade で更新されず、`Kafka` CRD は 256KB 超で client-side 不可 | 設定不変の版上げは broker 無停止。operator/exporter 等が Pod ローリング | 上げる時は `strimzi-crds-<version>.yaml` を server-side apply してから deploy |
| C. Helm chart | kube-prometheus-stack | メトリクス監視（Prometheus / Grafana / Alertmanager） | `config.py` の `kube_prometheus_stack_chart_version` | 値変更 → `cdk deploy` | **あり（大）→ chart 更新前に手動 SSA 必須**（付録） | Kafka 無停止 / 監視 Pod 再起動（数分のメトリクス欠損） | operator の版が上がる更新は CRD スキーマ更新を伴う。release notes で CRD 破壊的変更・`kube-prometheus-stack-values.yaml` のキー互換を確認。単独 PR |
| C. Helm chart | AWS Load Balancer Controller | NLB/ALB を k8s から管理（Kafka 公開用 NLB） | `config.py` の `aws_lbc_chart_version` | 値変更 → `cdk deploy` | あり（小）: `TargetGroupBinding`/`IngressClassParams`。変更時のみ apply（client-side 可） | 原則無停止（controller 再起動中も NLB は稼働継続） | 唯一の停止筋は新版が NLB を作り直すケース → PrivateLink 全断。**`cdk diff` で NLB の Replace 無しを確認**。IAM ポリシー差分も照合 |
| C. Helm chart | Fluent Bit | Pod ログの収集・転送 | `config.py` の `fluent_bit_chart_version` | 値変更 → `cdk deploy` | なし | 無停止 / ログ転送が数秒〜数十秒途切れる | 低リスク。新チャート（collector/aggregator 系）移行は別テーマ |
| D. Kubernetes manifest | external-snapshotter（CRD のみ） | EBS CSI Driver の csi-snapshotter サイドカーが要求する VolumeSnapshot CRD を提供（CRD のみ。実バックアップは AWS Backup） | `config.py` の `external_snapshotter_version` + `manifests/snapshotter/crds.yaml` | `config.py` コメントの `kubectl kustomize` で `crds.yaml` 再生成 → タグ変更 → `cdk deploy` | `crds.yaml` が CRD 単体（小）。client-side で当たるため SSA 不要 | CRD apply のみ。稼働ワークロードに無影響、Kafka 無停止 | snapshot-controller / VolumeSnapshotClass は未導入（実需が出たら追加） |
| E. ツールチェーン | aws-cdk-lib | インフラ定義の CDK ライブラリ | `pyproject.toml` の版制約 | `uv lock --upgrade-package aws-cdk-lib` → `uv run pytest` → `cdk diff` | — | 更新自体は無停止（コード生成のみ）。停止は diff 次第 | `aws_eks_v2` 使用で EKS 関連 CFN に差分が出やすい。**`cdk diff` で Cluster/Nodegroup の Replace 無しを確認** |
| E. ツールチェーン | kubectl layer | CDK が manifest/helm を適用する kubectl ランタイム | `pyproject.toml`（`aws-cdk-lambda-layer-kubectl-vNN`） | K8s 本体の版に合わせて差し替え | — | — | 単独で上げない（A に連動） |

補足:
- **B と C はどちらも広義の「アドオン」**で、EKS は公式に **B = Amazon EKS アドオン（マネージド）/ C = self-managed アドオン** と区別する。違いは配り方だけ——B は AWS が EKS の機構で版を管理、C は自分で Helm の版を `config.py` に書く（C の正式な配り方が Helm chart）。
- **CRD は独立した更新対象ではない**。chart（C）か manifest（D）の中身で、版は親に従う。Helm は upgrade で CRD を更新しないため、**大きい CRD を持つ kube-prometheus-stack と Strimzi は手動 SSA が要る**（付録）。LBC は CRD が小さく client-side で可、Fluent Bit は CRD なし、snapshotter（D）も小さく SSA 不要。
- **アドオン（B）更新は nodegroup の AMI 更新と一緒にやらない**（AMI 更新はノード置換 = broker drain。別 PR に分離）。

## 事故シナリオ（broker が止まり得る 3 つ）

計画停止を要する更新は Kubernetes バージョン更新（A）以外に無い。A 以外で broker が止まるのは次の 3 つだけで、いずれも **deploy 前の `cdk diff` 確認で回避可能**。

1. AWS LBC が NLB を置換 → PrivateLink 全断（C）
2. アドオン更新を nodegroup AMI 更新と混在 → ノード置換で broker drain（B）
3. aws-cdk-lib の diff がリソース置換を誘発（E）

リスクの目安（低 → 高）: ツールチェーン / Fluent Bit / 観測系アドオン ＜ AWS LBC / net・DNS 系アドオン ＜ kube-prometheus-stack・Strimzi のメジャー（CRD SSA 伴う）＜ Kubernetes バージョン更新（A・別格）。

## 付録: CRD 手動更新の段取り（kube-prometheus-stack / Strimzi）

operator の版が上がる更新では、chart を上げる前にこの手順で CRD を server-side apply する。kube-prometheus-stack と Strimzi の両方が対象（どちらも CRD が 256KB 超で client-side 不可）。

### kube-prometheus-stack

```bash
# ターゲット chart 同梱の CRD だけを server-side apply で先に当てる（タグは実施時の版に置換）
KPS_TAG=kube-prometheus-stack-<version>
BASE=https://raw.githubusercontent.com/prometheus-community/helm-charts/${KPS_TAG}/charts/kube-prometheus-stack/charts/crds/crds

for crd in alertmanagerconfigs alertmanagers podmonitors probes prometheusagents \
           prometheuses prometheusrules scrapeconfigs servicemonitors thanosrulers; do
  kubectl apply --server-side --force-conflicts -f ${BASE}/crd-${crd}.yaml
done
# この後に config.py のバージョンを上げて cdk deploy
```

### Strimzi

```bash
# Strimzi はリリースページの統合 CRD ファイルを server-side apply（タグは実施時の版に置換）
STRIMZI_VER=<version>
kubectl apply --server-side --force-conflicts \
  -f https://github.com/strimzi/strimzi-kafka-operator/releases/download/${STRIMZI_VER}/strimzi-crds-${STRIMZI_VER}.yaml
# この後に config.py の strimzi_version を上げて cdk deploy
```

### なぜ手動 SSA か

- helm upgrade（CDK の `add_helm_chart` 含む）は chart の `crds/` 配下を **初回 install のみ適用・upgrade では更新しない**（Helm エンジンの仕様）。
- client-side `kubectl apply` は適用内容を `last-applied-configuration` アノテーションに全文格納するため metadata の **256KB 上限**に当たる。prometheus CRD・Strimzi の `Kafka` CRD はいずれもこれを突破するので **server-side apply（SSA）必須**。SSA はマージを API サーバが `managedFields` で行い巨大アノテーションを使わない。`--force-conflicts` は既存の所有権を引き取る指定。
- CDK の `KubernetesManifest`/`add_manifest` は client-side 固定で SSA 不可（2.253.1 で確認）。よって CDK では当てきれず、メジャー更新時のみ手動 SSA が現実解。

### CRD を先に当てる影響

- 「新 CRD + 旧 operator」の共存は基本無害（新 CRD は上位互換、旧 operator は知らない新フィールドを無視）。**CRD apply 自体は operator も Prometheus も再起動しない**（API サーバ上のスキーマ変更のみ）。
- ゼロではない例外: バリデーション厳格化・フィールド削除/リネーム（pruning）・default 変更。これらは「その CR が次に reconcile される時」に出る。**release notes 確認で潰す**。共存ウィンドウは短く（CRD apply 後すぐ chart upgrade）。

### 参考: ツール横断の CRD 取り扱い

| | upgrade で CRD 追従 | server-side apply | 巨大 CRD | 順序制御 |
|---|---|---|---|---|
| helm CLI / CDK `add_helm_chart` | しない（`crds/`） | CDK 標準は不可 | 手動 SSA か作り込み | 手動（`add_dependency`） |
| Terraform `helm_release` | しない | `kubectl_manifest` で可 | kubectl provider で可 | `depends_on` |
| ArgoCD（Helm source） | する（毎 sync で apply） | `ServerSideApply=true` | 標準で可 | 自動（CRD 先行） |

ArgoCD は CRD 取り扱いが最も素直だが、導入は GitOps への運用転換であり CRD 1 件のためには過剰。
