# kafka/consumer

Kafka を PySpark Structured Streaming + `Trigger.AvailableNow` で取得し、S3 Tables (Iceberg) に書き込む consumer。

**案 R-1: テーブル分割 + 並列 streaming query** を採用。1 つの EMR ジョブ内で SCHEMAS の各 ProtoBuf 型に対して独立した StreamingQuery を起動し、それぞれが別 topic を subscribe → 別 Iceberg テーブルに書き込む。Spark の同一 Executor プールを共有して並列実行する。

EMR Serverless 7.13.0 / EMR on EKS 7.13.0 / ローカル PySpark 3.5.6 で動作。

## 処理フロー

```
1 つの EMR ジョブ (spark-submit 1 プロセス)
├ SparkSession (1 つ。Executor プールを保持)
│   ├ StreamingQuery 1  topic=sample-events-event   → table=sample_events_event
│   ├ StreamingQuery 2  (将来 .proto を追加すれば自動増殖)
│   └ ...
└ Executor プール (全 query で共有)

各 StreamingQuery:
  Kafka 1 topic → from_protobuf (1 種類) → foreachBatch → append 専用テーブル

DLQ: 全 query 共通の sample_events_dlq に append (失敗データ用)
```

各 query は **start() の non-blocking 起動** で並列に走り、`awaitTermination()` をループの最後にまとめて呼んで完了待ち。Iceberg テーブルがそれぞれ別なので snapshot 履歴も独立し、commit serialize の影響を受けません。

## なぜ案 R-1 か

100 種類規模の ProtoBuf を 1 トピックで扱う場合と比較した利点:

| 観点 | 1 topic + 共通テーブル (案 A 改良) | 案 R-1 (テーブル分割 + 並列 query) |
|---|---|---|
| プラン compilation | 重い (N=100 で 400 ノード) | 軽い (各 query 1 schema 分のみ) |
| 64KB メソッドサイズ問題 | リスクあり | リスクなし |
| commit/batch | 1 (同テーブル) | 各テーブル 1 (並列) |
| commit serialize | なし | なし (テーブル別の linear history) |
| 横断クエリ | ◎ (1 テーブル SELECT) | △ (UNION ALL が必要) |
| 新型追加 | .proto 追加のみ | .proto 追加 + CDK にテーブル定義追加 |

## ProtoBuf スキーマと descriptor file

メッセージ定義は [kafka/proto/](../proto/) 配下の `.proto` ファイル群。`from_protobuf` には全 message を含む統合 `events.desc` を渡す。生成手順は [kafka/proto/README.md](../proto/README.md) を参照。

EMR ジョブでは `events.desc` を S3 にアップロードして [constants.py](constants.py) の `DESCRIPTOR_FILE` で参照する。

```bash
aws s3 cp kafka/proto/events.desc s3://<アーティファクトバケット>/proto/events.desc
```

## SCHEMAS の指定方法

`SCHEMAS` リストは [constants.py](constants.py) に **固定値で直接書き並べる**。動的派生はしない (関数で名前を組み立てると CDK 側のテーブル名と乖離した時に気付きにくいため)。

```python
SCHEMAS: list[SchemaConfig] = [
    SchemaConfig(
        schema_name="Event",
        protobuf_full_name="ekscdk.kafka.Event",
        topic="sample-events-event",
        target_table="s3tablesbucket.events.sample_events_event",   # CDK の CfnTable と一致
        checkpoint_location="file:///tmp/sample-events-checkpoint/event/",
    ),
    # 新型を追加する場合は SchemaConfig を 1 件追加するだけで並列 query が増える
]
```

`target_table` は CDK の `S3TablesStack` で作成したテーブル名と必ず一致させる。新型追加時は本リストへの追加と CDK 側の `CfnTable` 定義追加を **両方** 行う必要がある (片方だけ忘れると実行時に「テーブルが存在しない」エラーになる)。

## 書き込み先テーブル

CDK の `S3TablesStack` ([cdk/ekscdk/s3tables_stack.py](../../cdk/ekscdk/s3tables_stack.py)) で事前作成された 2 つの Iceberg テーブル:

- TableBucket: 環境別 (dev: `kafka-events-dev` / stg: `kafka-events-stg` / prd: `kafka-events`)
- Namespace: `events`
- 本テーブル: `sample_events_event` (パーティション `day(datetime)`)
- DLQ テーブル: `sample_events_dlq` (パーティション `day(failed_at)`)

### `sample_events_event` (本テーブル)

| カラム | Iceberg 型 | required | 由来 |
|---|---|---|---|
| id | long | true | ProtoBuf `Event.id` (int64) |
| datetime | timestamp | true | ProtoBuf `Event.datetime` (ISO8601 string) を `to_timestamp` でキャスト |
| name | string | true | ProtoBuf `Event.name` |
| rawdata | binary | true | Kafka `value` (ProtoBuf bytes) をそのまま保持 |

`rawdata` を残すことで、スキーマ進化や障害調査時に元 ProtoBuf を再デシリアライズできる。

### `sample_events_dlq` (Dead Letter Queue)

ProtoBuf デシリアライズに失敗した Kafka メッセージの退避先 (全 query 共通):

| カラム | Iceberg 型 | required | 由来 |
|---|---|---|---|
| failed_at | timestamp | true | consumer が失敗を検知した時刻 |
| rawdata | binary | true | 失敗した Kafka `value` (ProtoBuf bytes) をそのまま保持 |

## ランタイムバージョン

EMR 7.13.0 同梱の以下のバージョンを前提とする。spark-submit `--packages` も同じバージョンに揃える。

| コンポーネント | バージョン |
|---|---|
| Apache Spark | 3.5.6 |
| Apache Iceberg | 1.10.0 |
| Python | 3.11 |
| Scala (Spark) | 2.12 |

## ローカル開発: .env で環境変数を渡す

consumer.py / launch.json は AWS アカウント依存の値 (`TABLE_BUCKET_ARN`, `CHECKPOINT_BUCKET`) を環境変数で受け取る。リポジトリには placeholder を置かず、各開発者が `.env` を作って埋める。

```bash
cp .env.example .env
# .env を編集して <アカウントID> 等を実値に置き換える
```

`.vscode/launch.json` の `envFile` 設定が `.env` を読み込むので、VS Code から debugpy で `consumer.py` を起動すれば自動で適用される。`.env` はルート `.gitignore` で除外済み。

ターミナルから直接 `python consumer.py` を叩く場合は事前に `set -a; source .env; set +a` で読み込む。

## ローカル開発: PySpark の Hadoop バンドル差し替え

PySpark 3.5.6 wheel は Hadoop 3.3.4 を同梱するが、本リポジトリは `hadoop-aws:3.4.1` (AWS SDK v2 採用版) とクラスパスを揃える必要があるため、`.venv` 生成後に Hadoop client jar を 3.4.1 系に差し替える。本番 EMR 7.13.0 では Hadoop 3.4 系が同梱済みのため不要。

```bash
uv sync --all-groups
bash scripts/patch-pyspark-hadoop.sh
```

`scripts/patch-pyspark-hadoop.sh` は冪等。`.venv` を再生成したら毎回走らせる。

## 実行例

### 事前準備: constants.py を実行環境に合わせて編集する

接続先・descriptor 置き場・SCHEMAS 内の各 `checkpoint_location` は [constants.py](constants.py) の固定値として持つ。実行前に環境に合わせて書き換えること。

| 定数 | ローカル開発 | EMR 本番 |
|---|---|---|
| `BOOTSTRAP_SERVERS` | `localhost:9094` | `<NLB DNS>:9094` |
| `DESCRIPTOR_FILE` | `kafka/proto/events.desc` を相対解決 | `s3://<アーティファクトバケット>/proto/events.desc` |
| `SCHEMAS[*].checkpoint_location` | `file:///tmp/sample-events-checkpoint/event/` | `s3://kafka-consumer-checkpoint-<アカウントID>-prd/event/` |
| `SCHEMAS[*].topic` / `target_table` | 既定値 (CDK と一致) | 同左 |
| `DLQ_TARGET_TABLE` | `s3tablesbucket.events.sample_events_dlq` | 同左 |

### EMR Serverless 7.13.0 / EMR on EKS 7.13.0 で S3 Tables に書き込む

```bash
TABLE_BUCKET_ARN=arn:aws:s3tables:ap-northeast-1:<アカウントID>:bucket/kafka-events

spark-submit \
  --conf spark.jars.packages=org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.0,software.amazon.s3tables:s3-tables-catalog-for-iceberg-runtime:0.1.5,org.apache.spark:spark-protobuf_2.12:3.5.6,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6 \
  --conf spark.sql.extensions=org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions \
  --conf spark.sql.catalog.s3tablesbucket=org.apache.iceberg.spark.SparkCatalog \
  --conf spark.sql.catalog.s3tablesbucket.catalog-impl=software.amazon.s3tables.iceberg.S3TablesCatalog \
  --conf spark.sql.catalog.s3tablesbucket.warehouse=${TABLE_BUCKET_ARN} \
  consumer.py
```

EMR Serverless では上記コマンドを Job Run のエントリポイントとして登録する。EMR on EKS の場合は SparkApplication CRD の `mainApplicationFile` で `consumer.py` を、`sparkConf` で同じ catalog 設定を渡す。

## EMR ジョブに必要な IAM 権限 (最小限)

EMR Serverless Job Role / EMR on EKS Execution Role に付与する。

```json
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "S3TablesReadWrite",
      "Effect": "Allow",
      "Action": [
        "s3tables:GetTableBucket",
        "s3tables:GetNamespace",
        "s3tables:GetTable",
        "s3tables:GetTableMetadataLocation",
        "s3tables:GetTableData",
        "s3tables:PutTableData",
        "s3tables:UpdateTableMetadataLocation"
      ],
      "Resource": [
        "arn:aws:s3tables:ap-northeast-1:<アカウントID>:bucket/kafka-events",
        "arn:aws:s3tables:ap-northeast-1:<アカウントID>:bucket/kafka-events/*"
      ]
    },
    {
      "Sid": "CheckpointBucket",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:DeleteObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::<チェックポイントバケット>",
        "arn:aws:s3:::<チェックポイントバケット>/*"
      ]
    },
    {
      "Sid": "DescriptorAndArtifacts",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:ListBucket"],
      "Resource": [
        "arn:aws:s3:::<アーティファクトバケット>",
        "arn:aws:s3:::<アーティファクトバケット>/*"
      ]
    }
  ]
}
```

## 設計判断

- **Structured Streaming + `Trigger.AvailableNow`**: ジョブ起動のたびに「Kafka 上の現在地までを 1 回で処理して終了」する。`checkpointLocation` で次回起動時は続きから自動再開し、自前オフセット管理が不要。
- **テーブル分割 + 並列 streaming query (案 R-1)**: 1 ジョブで 100 種類対応する場合、共通テーブル + union 方式だとクエリプランが 400+ ノードに膨らんで compilation cost / 64KB 問題に当たる。テーブルを分けて各 query を独立した小さなプランにすることで、両問題を回避しつつ並列度を確保。
- **producer 側で topic 分割**: schema_name から派生する topic 名 (`sample-events-<schema_lower>`) で書き分け。consumer 側は subscribe オプションで該当 topic を 1 つだけ読むので、ヘッダー識別が不要。
- **`SCHEMAS` は固定値で書き並べる**: 動的派生 (関数で topic / table 名を組み立てる) は採用しない。CDK 側のテーブル名と consumer 側の名前は人の目で一致確認できる方が運用ミスを防ぎやすいため。代わりに新型追加時は SCHEMAS への追加と CDK の CfnTable 追加を 2 箇所セットで行う。
- **テーブル定義を CDK で持つ**: スキーマと partition spec を IaC で表現することで、レビューと環境間差分管理が CFN 経由で一元化される。新型追加時は CDK に CfnTable を 1 件足す必要あり。
- **DLQ は全 query 共通の 1 テーブル**: 失敗データの頻度は低い前提で、100 query が並列書き込みしても Iceberg 楽観ロックの retry は許容範囲。

## 書き込みと DLQ

各 StreamingQuery の foreachBatch で以下を実行する:

1. `from_protobuf(..., mode="PERMISSIVE")` でデシリアライズ。失敗した行は `payload` が `null` (例外で停止しない)
2. `payload != null` の行 (デシリアライズ成功):
   - `payload.*` で ProtoBuf の全フィールドを展開
   - `datetime` 列を `to_timestamp` でキャスト
   - `sample_events_<schema>` に append (`writeTo(...).append()`)
3. `payload == null` の行 (デシリアライズ失敗):
   - `(failed_at, rawdata)` を全 query 共通 DLQ `sample_events_dlq` に append
   - 件数を `logger.warning` で運用通知

これにより:

- **形式不正データ**: 全件破棄ではなく DLQ に蓄積され、後から原因解析・再処理が可能
- **並列書き込み**: 各 query が別テーブルに書くので Iceberg snapshot 履歴が独立、commit が並列で完了

> at-least-once 配信なので、ジョブ再実行やリトライで同一レコードが複数回 append される可能性がある。重複が問題になる場合は下流 (集計クエリ・後段ジョブ) で排除する想定。

### DLQ 行の確認 (Athena)

```sql
SELECT failed_at, length(rawdata) AS bytes
FROM s3tablesbucket.events.sample_events_dlq
WHERE failed_at > current_timestamp - interval '1' day
ORDER BY failed_at DESC
LIMIT 100;
```

### 型ごとのカウント (Athena)

```sql
SELECT 'Event' AS schema_name, count(*) AS cnt
FROM s3tablesbucket.events.sample_events_event
WHERE datetime > current_timestamp - interval '1' day
-- 新型を追加したら UNION ALL で連結
```

## 既知の未対応事項

- **Glue Data Catalog 統合**: Athena から `sample_events_event` を SELECT したい場合は `s3tablescatalog` (アカウント・リージョン単位で 1 つ) の作成が必要。EMR Spark からの書き込みだけなら不要のため、本リポジトリの CDK には含めていない。
- **横断クエリの煩雑さ**: 全型を 1 クエリで集計するには UNION ALL が必要。VIEW を作る運用で吸収する余地あり。
- **新型追加時の CDK 変更**: `sample_events_<schema>` の CfnTable 定義を 1 件追加する必要がある (consumer 側は SCHEMAS 自動展開で変更不要)。
