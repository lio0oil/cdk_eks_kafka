# ディスク容量アラート対応手順

KubePersistentVolumeFillingUp 等のディスク空き容量アラートが SNS 通知された後の対応手順。
容量の自動拡張（volume autoscaler）は導入していない。gp3 StorageClass の `allowVolumeExpansion: true` と
EBS CSI Driver（AmazonEBSCSIDriverPolicy に `ec2:ModifyVolume` を含む）による**手動オンライン拡張**が前提。

## 対象アラートと発火条件（kube-prometheus-stack 同梱ルール）

| アラート | severity | 発火条件の概要 |
|---|---|---|
| KubePersistentVolumeFillingUp | warning | PVC 残量 15% 未満、かつ直近 6h の傾向で 4 日以内に枯渇予測 |
| KubePersistentVolumeFillingUp | critical | PVC 残量 3% 未満 |
| KubePersistentVolumeInodesFillingUp | warning / critical | 上記の inode 版 |
| KubePersistentVolumeErrors | critical | PV が Failed / Pending |
| NodeFilesystemSpaceFillingUp | warning / critical | ノード FS が 24h / 4h 以内に枯渇予測 |
| NodeFilesystemAlmostOutOfSpace | warning / critical | ノード FS 残量 5% / 3% 未満 |

## 共通の制約（拡張前に必ず確認）

- 拡張は**オンライン**（Pod 停止不要）。PVC の要求サイズを増やすと EBS CSI が ModifyVolume と
  ext4 の online resize まで自動で行う
- **縮小は不可**。一度増やしたサイズは戻せない
- **同一 EBS ボリュームの変更は約 6 時間に 1 回**。小刻みに増やすと次の拡張まで待たされるため、
  増分は現サイズの 50〜100% を目安に一度で確保する
- gp3 の IOPS 3000 / throughput 125 は StorageClass 固定値で、サイズ拡張では変わらない
- 拡張後は**リポジトリの manifest 宣言値と実サイズを必ず一致させる**
  （クラスタ再構築や broker 追加時に旧サイズで作られる事故を防ぐ）

## 1. 初動（共通）

1. 通知の `persistentvolumeclaim` / `namespace` ラベル（ノード系は `instance` / `device`）で対象を特定する
2. 現状と増加傾向を確認する

   ```bash
   # Helm release 名は CDK が自動生成するため、release 名に依存しない
   # operator 生成の Service（prometheus-operated / alertmanager-operated）を使う
   kubectl -n monitoring port-forward svc/prometheus-operated 9090:9090
   ```

   ```promql
   kubelet_volume_stats_available_bytes{persistentvolumeclaim="<PVC>"}
     / kubelet_volume_stats_capacity_bytes
   predict_linear(kubelet_volume_stats_available_bytes{persistentvolumeclaim="<PVC>"}[6h], 4*24*3600)
   ```

3. 作業中の再通知（`repeat_interval: 4h`）を抑えたい場合は Alertmanager UI で silence を作成する
   （`kubectl -n monitoring port-forward svc/alertmanager-operated 9093:9093`）。
   期限は作業見込み時間に限定する
4. **増加の正当性を判断する**。拡張の前に、原因除去（retention 見直し・cardinality 修正等）で
   解消しないかを先に検討する

## 2. Kafka broker / controller の PVC

PVC 名は `data-0-kafka-cluster-kafka-<node_id>`（controller は `data-0-kafka-cluster-controller-<node_id>`）。

**増加要因の確認**

- 不要トピック、過大な `retention.ms` / `retention.bytes` → トピック設定の見直しで解消するなら拡張不要
- 正当なデータ増 → 拡張へ

**拡張手順（必ず Strimzi 経由。PVC を直接 patch しない）**

1. `cdk/manifests/kafka/node-pool-broker.yaml`（または `node-pool-controller.yaml`）の
   `size: 20Gi` を新サイズへ変更し、PR を作成・merge する
2. ユーザーが `cdk deploy` を実行する
3. Strimzi Cluster Operator が pool 内の全 PVC を resize する
   （pool 単位の一括変更。broker 3 台なら EBS 費用も 3 本分増える）
4. 確認:

   ```bash
   kubectl -n kafka get pvc
   kubectl -n kafka get kafka kafka-cluster -o jsonpath='{.status.conditions}'
   ```

PVC を直接 `kubectl patch` で拡張すると KafkaNodePool の宣言と乖離し、後から追加した broker だけ
旧サイズで作られてディスクサイズが不揃いになる。manifest 経由を必須とする。

## 3. Prometheus / Alertmanager の PVC

**増加要因の確認**

- cardinality 急増（新規 ServiceMonitor、ラベル爆発）が疑われる場合は scrape 側の修正が先:

  ```promql
  topk(10, count by (__name__)({__name__=~".+"}))
  ```

- retention 15d 設計内の自然増（見積もりは values のコメント参照: 約 8GB / 20Gi）→ 拡張へ

**拡張手順（prometheus-operator は volumeClaimTemplate の変更を既存 StatefulSet に適用できないため 3 段階）**

1. 既存 PVC を直接拡張する（オンラインで即効性があるため最初に行う）:

   ```bash
   kubectl -n monitoring get pvc -l app.kubernetes.io/name=prometheus
   kubectl -n monitoring patch pvc <PVC名> \
     -p '{"spec":{"resources":{"requests":{"storage":"40Gi"}}}}'
   # replica 数分（2 本）繰り返す
   ```

2. `kube-prometheus-stack-values.yaml` の `storageSpec` を同じサイズへ更新し、PR → `cdk deploy`
3. StatefulSet を orphan 削除して operator に新テンプレートで再作成させる
   （Pod は残るため無停止。volumeClaimTemplate が immutable なため必要）:

   ```bash
   kubectl -n monitoring delete statefulset -l app.kubernetes.io/name=prometheus --cascade=orphan
   ```

4. 確認: `kubectl -n monitoring get pvc,sts` と手順 1 の usage クエリ

Alertmanager（1Gi）は notification log と silence のみでほぼ増えない設計。発火したらまず異常
（silence の大量作成等）を疑う。拡張手順は同じ（label は `app.kubernetes.io/name=alertmanager`）。

## 4. ノードの root volume（NodeFilesystem 系）

nodegroup は `disk_size` 未指定のため AMI デフォルト（20GiB）。

1. 消費元を特定する（コンテナイメージ / ログ / emptyDir）:

   ```bash
   kubectl debug node/<node名> -it --image=busybox -- df -h /host
   ```

   Prometheus からは `node_filesystem_avail_bytes` を mountpoint 別に確認する
2. **短期対応**: 対象ノードを drain して EC2 を terminate → managed nodegroup が新ノードを補充
   （root volume は初期化される）。Prometheus / Alertmanager の PDB（minAvailable: 1）があるため
   drain は 1 台ずつ行う
3. **恒久対応**: `eks_cluster.py` の launch template に block device mapping を追加して
   root volume サイズを増やす（deploy 時に nodegroup のローリング入れ替えが発生する）

## 5. 事後

- アラートが resolved になり、SNS に resolved 通知が届いたことを確認する
- 容量見積もりが設計時の前提（values コメントの計算根拠等）から乖離していた場合は、
  コメント・`SPEC.md` の見積もりを実態に合わせて更新する
- Kafka の PVC サイズを変えた場合、AWS Backup のスナップショット費用も増える点を認識しておく
