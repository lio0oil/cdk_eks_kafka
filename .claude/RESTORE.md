# Kafka リストア手順（AWS Backup の EBS スナップショットからの復旧）

AWS Backup で取得した broker / controller の EBS スナップショットから、Strimzi Kafka クラスターをデータ入りで復旧する手順。
Strimzi 公式の「Recovering a deleted Kafka cluster」（PV からのクラスタ復旧）を本プロジェクトの構成（KRaft / KafkaNodePool / CDK 管理）に合わせて具体化したもの。

## 適用範囲

**この手順を使う場面**

- broker / controller の EBS ボリューム群を失った（誤削除・アカウント侵害等）
- Kafka のデータを過去のスナップショット時点へ巻き戻したい（論理破壊）
- namespace 誤削除や EKS クラスタ全損からの復旧（後述のシナリオ B として、CDK 再構築後に本手順へ合流する）

**この手順を使わない場面**

- **単一ボリュームの喪失**: RF=3 のため対象 broker の PVC/PV を消して空ボリュームで起動すれば再レプリケーションで自動復旧する。スナップショットからの復元は不要
- **未 consume データの救出**: スナップショットは取得時点までしか含まない（日次なら RPO は最大 24 時間）。スナップショット以降に produce されたデータは戻らない

**整合性の前提**

EBS スナップショットはボリューム単位の crash-consistent で、broker 間で取得時刻が数秒〜数分ずれる。Kafka はクラッシュ復旧を前提に設計されており、起動後にレプリケーションの truncate / 再同期で broker 間の差分は収束する。スナップショット時刻の差分に相当するデータは末尾で失われ得る。

## このクラスタ固有の事実（手順中で使う値）

| 項目 | 値 |
|---|---|
| namespace | `kafka` |
| Kafka CR 名 | `kafka-cluster` |
| broker pool / node ID | pool 名 `kafka`、node ID は 0 起点（`next-node-ids: [0-999999]`） |
| controller pool / node ID | pool 名 `controller`、node ID は 1000000 起点（`next-node-ids: [1000000-1999999]`） |
| broker PVC 名 | `data-0-kafka-cluster-kafka-<node_id>`（例: `data-0-kafka-cluster-kafka-0`） |
| controller PVC 名 | `data-0-kafka-cluster-controller-<node_id>`（例: `data-0-kafka-cluster-controller-1000000`） |
| StorageClass | `gp3`（encrypted / iops 3000 / throughput 125、`reclaimPolicy: Retain`、`WaitForFirstConsumer`） |
| ファイルシステム | ext4（EBS CSI Driver デフォルト。StorageClass で fsType 未指定） |
| deleteClaim | dev=true / stg/prd=false |

node ID は Strimzi が動的に採番するため、**実環境の実際の PVC 名は平時記録（下記）を正とする**。

## 0. 平時にやっておくこと（バックアップ取得側の前提）

リストアはここで記録した情報に依存する。バックアップ運用開始時と broker 増減時に更新する。

1. **PVC / PV / EBS ボリューム / AZ のマッピングを記録する**

   ```bash
   kubectl get pvc -n kafka -o custom-columns='PVC:.metadata.name,PV:.spec.volumeName'
   kubectl get pv -o custom-columns='PV:.metadata.name,VOLID:.spec.csi.volumeHandle,ZONE:.spec.nodeAffinity.required.nodeSelectorTerms[0].matchExpressions[0].values[0],CLAIM:.spec.claimRef.name'
   ```

2. **KRaft cluster ID を記録する**（復旧時に必須。ボリュームからも回収可能だが控えておくと速い）

   ```bash
   kubectl get kafka kafka-cluster -n kafka -o jsonpath='{.status.clusterId}'
   ```

3. **Kafka CR / KafkaNodePool の生きた定義を保存する**（CDK が brokers[] を注入するため、リポジトリの YAML 単体では完全な CR にならない）

   ```bash
   kubectl get kafka kafka-cluster -n kafka -o yaml > kafka-cr-live.yaml
   kubectl get kafkanodepool -n kafka -o yaml > kafka-pools-live.yaml
   ```

4. **dev でこの手順のリハーサルを一度実施する**。リストア未検証のバックアップは無いのと同じ。

## 1. 停止と現状保全

1. producer を停止する（外部システム側）。
2. Spark consumer（Structured Streaming job）を停止する。
3. 現状の定義・cluster ID・マッピングを取得する（手順 0 と同じコマンド。クラスタが応答する場合のみ）。
4. クライアント CA を退避する（namespace ごと失われるシナリオへの保険）:

   ```bash
   kubectl get secret kafka-cluster-cluster-ca-cert kafka-cluster-cluster-ca -n kafka -o yaml > kafka-ca-backup.yaml
   ```

## 2. AWS Backup からの EBS ボリューム復元

broker 3 本 + controller 3 本 = 6 ボリュームを、それぞれ**元と同じ AZ** に復元する（元 AZ 不明の場合は broker 3 本・controller 3 本を 3 AZ に 1 本ずつ割り当てれば良い。rack は起動時にノードの AZ から再計算される）。

1. 復旧したい時点の recovery point を特定する。同一バックアップ実行分（≒ 同時刻帯）の 6 本を揃えること:

   ```bash
   aws backup list-recovery-points-by-backup-vault \
     --backup-vault-name <vault_name> \
     --query 'RecoveryPoints[].{Arn:RecoveryPointArn,Resource:ResourceArn,Created:CreationDate}'
   ```

2. recovery point（= EBS スナップショット）がどの PVC のものかをタグで照合する。EBS CSI Driver がボリュームに付与した `kubernetes.io/created-for/pvc/name` タグがスナップショットに引き継がれている:

   ```bash
   aws ec2 describe-snapshots --snapshot-ids <snap_id> \
     --query 'Snapshots[0].Tags[?Key==`kubernetes.io/created-for/pvc/name`].Value'
   ```

3. 6 本それぞれ復元ジョブを実行する（AWS Backup の監査証跡に乗せるため `start-restore-job` を使う。`aws ec2 create-volume --snapshot-id` でも結果は同じ）:

   ```bash
   aws backup start-restore-job \
     --recovery-point-arn <recovery_point_arn> \
     --iam-role-arn <backup_service_role_arn> \
     --metadata availabilityZone=<az>,volumeType=gp3,encrypted=true
   ```

4. 復元された `vol-xxxx` を PVC 名・AZ と対応付けて記録する（手順 4 で使う）。

## 3. 旧 Kubernetes リソースの撤去

**シナリオ A（EKS クラスタと Strimzi operator は稼働中）**: そのまま以下を実行。

**シナリオ B（namespace / EKS クラスタごと喪失）**: 先に `cdk deploy` で基盤を再構築する。CDK は Kafka CR まで一括 apply するため、**空の新ボリュームで新しい cluster ID の Kafka が一度立ち上がる**。それを以下の手順で撤去してシナリオ A に合流する（CDK デプロイ中に作られた空 PVC / PV / EBS は混同しないようタグ・作成日時で識別して破棄する）。

1. Kafka CR を削除する（operator が Pod / StrimziPodSet / Service を片付ける。KafkaTopic CR は削除しない）:

   ```bash
   kubectl delete kafka kafka-cluster -n kafka
   kubectl delete kafkanodepool kafka controller -n kafka
   ```

2. 旧 PVC を削除する（stg/prd は `deleteClaim: false` のため残存している）。PV は `Retain` なので Released になる:

   ```bash
   kubectl get pvc -n kafka
   kubectl delete pvc -n kafka -l strimzi.io/cluster=kafka-cluster
   ```

3. 旧 PV を削除する（Kubernetes オブジェクトのみ。背後の EBS は消えない）:

   ```bash
   kubectl get pv | grep kafka-cluster   # CLAIM 列で対象確認
   kubectl delete pv <pv_name> ...
   ```

旧 EBS ボリューム自体の削除は、復旧完了を確認した後の後始末（手順 8）で行う。

## 4. PV / PVC の静的再作成

復元した 6 ボリュームについて、PV → PVC の順に作成する。**PVC 名と volumeName / claimRef の対応を間違えるとデータ破損相当になる**ため、手順 2 のマッピング表と突き合わせて 1 本ずつ確認する。

PV（例: broker 0、AZ は実際の復元先に合わせる）:

```yaml
apiVersion: v1
kind: PersistentVolume
metadata:
  name: restored-data-0-kafka-cluster-kafka-0
spec:
  capacity:
    storage: 20Gi
  accessModes:
    - ReadWriteOnce
  persistentVolumeReclaimPolicy: Retain
  storageClassName: gp3
  volumeMode: Filesystem
  csi:
    driver: ebs.csi.aws.com
    volumeHandle: vol-XXXXXXXXXXXXXXXXX
    fsType: ext4
  nodeAffinity:
    required:
      nodeSelectorTerms:
        - matchExpressions:
            - key: topology.ebs.csi.aws.com/zone
              operator: In
              values:
                - ap-northeast-1a
```

PVC（PV と 1:1。`volumeName` で明示的に紐付ける）:

```yaml
apiVersion: v1
kind: PersistentVolumeClaim
metadata:
  name: data-0-kafka-cluster-kafka-0
  namespace: kafka
spec:
  accessModes:
    - ReadWriteOnce
  resources:
    requests:
      storage: 20Gi
  storageClassName: gp3
  volumeMode: Filesystem
  volumeName: restored-data-0-kafka-cluster-kafka-0
```

全 PVC が `Bound` になることを確認する:

```bash
kubectl get pvc -n kafka
```

## 5. Kafka CR の復旧（pause → cluster ID 設定 → unpause）

KRaft では、ボリューム上の `meta.properties` の `cluster.id` と operator が認識する cluster ID が一致しないと broker が `InconsistentClusterIdException` で起動しない。pause したまま CR を作成し、status に元の cluster ID を書き込んでから reconcile を開始する。

1. 保全しておいた CR（手順 0/1 の `kafka-cr-live.yaml`）から `status:` セクションと `metadata` の `uid` / `resourceVersion` / `creationTimestamp` / `generation` を除去し、pause annotation を追加して apply する:

   ```yaml
   metadata:
     name: kafka-cluster
     namespace: kafka
     annotations:
       strimzi.io/pause-reconciliation: "true"
   ```

   ```bash
   kubectl apply -f kafka-cr-recovered.yaml
   kubectl apply -f kafka-pools-recovered.yaml   # KafkaNodePool 2 件（同様にクリーン化）
   ```

   CR の保全が無い場合は、リポジトリの manifest（`node-pool-*.yaml` のプレースホルダーを config.py の値で置換、`kafka-cluster.yaml` に external listener の `brokers[]` を補完）から組み立てる。`cdk synth` の出力と一致していることを確認する。

2. cluster ID を status に設定する（手順 0 で記録した値）:

   ```bash
   kubectl patch kafka kafka-cluster -n kafka --subresource=status --type=merge \
     -p '{"status":{"clusterId":"<CLUSTER_ID>"}}'
   ```

   記録が無い場合は復元ボリュームから回収する:

   ```bash
   PVC_NAME="data-0-kafka-cluster-kafka-0"
   COMMAND="grep cluster.id /disk/kafka-log*/meta.properties | awk -F'=' '{print \$2}'"
   kubectl run tmp -itq --rm --restart "Never" --image "busybox" --overrides "{\"spec\":
     {\"containers\":[{\"name\":\"busybox\",\"image\":\"busybox\",\"command\":[\"/bin/sh\",
     \"-c\",\"$COMMAND\"],\"volumeMounts\":[{\"name\":\"disk\",\"mountPath\":\"/disk\"}]}],
     \"volumes\":[{\"name\":\"disk\",\"persistentVolumeClaim\":{\"claimName\":
     \"$PVC_NAME\"}}]}}" -n kafka
   ```

3. reconciliation を再開する:

   ```bash
   kubectl annotate kafka kafka-cluster -n kafka strimzi.io/pause-reconciliation=false --overwrite
   ```

   operator が StrimziPodSet を再作成し、Pod が手順 4 の PVC にバインドされて起動する。

KafkaTopic CR は CDK 管理で同一設定のまま残っているため再作成不要（Topic Operator が既存トピックをそのまま採用する。設定が異なる CR を入れると operator が Kafka 側を CR に合わせて変更するので注意）。KafkaUser は本プロジェクトでは未使用。

## 6. 検証

```bash
kubectl get pods -n kafka                                  # broker x3 / controller x3 / entity-operator が Ready
kubectl get kafka kafka-cluster -n kafka -o jsonpath='{.status.conditions[?(@.type=="Ready")].status}'
kubectl logs kafka-cluster-kafka-0 -n kafka | grep -i "InconsistentClusterId"   # 出力が無いこと
kubectl get kafkatopics -n kafka -o wide                   # READY=True
```

- NLB のターゲットヘルス（bootstrap + broker 0-2 の TargetGroup）が healthy であること
- `test-topic` への produce / consume スモークテスト
- Prometheus で `KafkaUnderReplicatedPartitions` / `KafkaOfflinePartitions` が発火していないこと

## 7. consumer（Spark）の再開と checkpoint 整合

Kafka をスナップショット時点へ巻き戻したため、checkpoint が記録している offset が Kafka の log end offset より**先**になっている可能性がある。その状態で再開すると offset out of range / data loss 検出で失敗する。

- **巻き戻しを伴う復旧（論理破壊からのロールバック）の場合**: checkpoint（`s3a://<CHECKPOINT_BUCKET>/envelope/`）を退避してから削除し、`startingOffsets: earliest` で再取り込みする。Iceberg 側に重複が入るため、原則として **S3 Tables 側も同時点の Iceberg スナップショットへ rollback してから** 再取り込みする
- **ボリューム喪失からの復旧で、復元時点が checkpoint の offset より先（または同等）の場合**: そのまま再開できる

## 8. 後始末

1. 旧 EBS ボリューム（壊れた・巻き戻し前のもの）を、復旧成立を確認した上で削除する（事前にユーザー確認必須の破壊的操作）。
2. シナリオ B で CDK が作った空ボリュームも同様に削除する。
3. `cdk diff` で手動 apply した CR と CDK manifest に乖離が無いことを確認する（pause annotation が `false` で残るのは無害）。
4. 平時記録（手順 0 のマッピング・cluster ID）を復旧後の値で更新する。

## 参考

- Strimzi 公式: Deploying and Managing Strimzi - "Cluster recovery from persistent volumes"（https://strimzi.io/docs/operators/latest/deploying）
- AWS Backup: Restore an Amazon EBS volume（https://docs.aws.amazon.com/aws-backup/latest/devguide/restoring-ebs.html）
