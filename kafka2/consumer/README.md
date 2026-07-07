# kafka2/consumer

Kafka を PySpark Structured Streaming + `Trigger.AvailableNow` で取得し、classification 毎に分解した parquet として S3 に書き込むバッチ consumer。

EMR Serverless 7.13.0 / EMR on EKS 7.13.0 / ローカル PySpark 3.5.6 で動作する。

## 処理フロー

```
1 つの EMR ジョブ (spark-submit 1 プロセス)
└ SparkSession
    └ StreamingQuery (topic: sample-events-event)
        Kafka 読み込み
          → zlib 展開 (producer が zlib 圧縮して送っているため)
          → from_protobuf (PERMISSIVE, デコード失敗時は payload=null)
          → foreachBatch
              ├ payload IS NULL            → DLQ (sample-events-dlq/)
              └ 上記以外 (デコード成功)      → sample-events-event/ へ
```

`Trigger.AvailableNow` により「起動のたびに Kafka 上の現在地までを 1 回で処理して終了」する。`checkpointLocation` で次回起動時は続きから自動再開する (at-least-once)。

## メッセージ構造と Envelope → 出力行への変換

`Envelope` は `common` / `user` / `content` を持ち、`content.historyrecords` (repeated `HistoryRecord`) には `classification` が異なる複数件が混在し得る。

```
Envelope
├─ common  (id, datetime)
├─ user    (userid, username)
└─ content
    └─ historyrecords[]  (historyid, datetime, memo, classification)
```

consumer は `content.historyrecords` を 1 レコードずつ `explode` し、各行の `envelope.content.historyrecords` を「その行の HistoryRecord 1 件だけ」に絞り込んで書き込む (`withField` で `payload` 全体から `content.historyrecords` だけを差し替えるため、`Envelope` に新しいフィールドが増えても手動で列挙し直す必要がない)。

出力行のスキーマ:

| 列 | 由来 |
|---|---|
| `envelope` | 元の `Envelope` (`content.historyrecords` は選択された 1 件のみ) |
| `classification` | 選択された `HistoryRecord.classification` (partition 列) |
| `year` / `month` / `day` | **処理時刻** (`current_timestamp()`) 由来 (partition 列) |

`year`/`month`/`day` を `Envelope.common.datetime` ではなく処理時刻にしているのは、再実行・バックフィル時に古い日付 partition へ書き込みが散らばらず、実行日の partition に集約されるようにするため。

## 書き込み先と partition

- 正常データ: `constants.OUTPUT_PATH` (`s3a://<OUTPUT_BUCKET>/sample-events-event/`) に `classification / year / month / day` で partition した parquet (zstd 圧縮) を追記
- DLQ: `constants.DLQ_OUTPUT_PATH` (`s3a://<OUTPUT_BUCKET>/sample-events-dlq/`) に `reason / year / month / day` で partition した parquet (zstd 圧縮) を追記。現状 `reason` は `deserialize_error` (from_protobuf がデコードできなかった行) のみ

書き込み直前に `classification` で `repartition` している。これをしないと 1 タスクが複数 classification の行を持ち得るため、そのタスクが担当する classification の種類数だけファイルを書いてしまい、読み込み側の並列数 (`minPartitions`) を上げるとファイル数がそれに比例して増えてしまう。`repartition("classification")` により同じ classification の行が同じタスクに集約され、書き込みファイル数は classification の種類数 (現状 60〜100 程度) 前後に収まる。

## ローカル開発: .env で環境変数を渡す

```bash
cp .env.example .env
# .env を編集して <アカウントID> 等を実値に置き換える
```

| 環境変数 | 内容 |
|---|---|
| `CHECKPOINT_BUCKET` | `checkpointLocation` 用 S3 バケット名 (`s3a://` プレフィックス除く) |
| `OUTPUT_BUCKET` | 出力 parquet (`sample-events-event/`, `sample-events-dlq/`) の書き込み先 S3 バケット名 |
| `PYSPARK_SUBMIT_ARGS` | ローカル PySpark 起動時の `--packages` / `--conf`。parquet 書き込みのみのため Iceberg / S3 Tables catalog 関連の設定は不要 (`spark-protobuf` / `spark-sql-kafka` / `hadoop-aws` のみ) |

`.vscode/launch.json` の `envFile` 設定が `.env` を読み込むので、VS Code から debugpy で `consumer.py` を起動すれば自動で適用される。ターミナルから直接 `python consumer.py` を叩く場合は事前に `set -a; source .env; set +a` で読み込む。`.env` はルート `.gitignore` で除外済み。

## ローカル開発: PySpark の Hadoop バンドル差し替え

PySpark 3.5.6 wheel は Hadoop 3.3.4 を同梱するが、本リポジトリは `hadoop-aws:3.4.1` (AWS SDK v2 採用版) とクラスパスを揃える必要があるため、`.venv` 生成後に Hadoop client jar を 3.4.1 系に差し替える。本番 EMR 7.13.0 では Hadoop 3.4 系が同梱済みのため不要。

```bash
uv sync --all-groups
bash scripts/patch-pyspark-hadoop.sh
```

`scripts/patch-pyspark-hadoop.sh` は冪等。`.venv` を再生成したら毎回走らせる。

## ProtoBuf schema と descriptor file

メッセージ定義は [kafka2/proto/event.proto](../proto/event.proto)。`from_protobuf` に渡す `events.desc` (FileDescriptorSet) は `.proto` から protoc で再生成する。

```bash
protoc -I ../proto --include_imports --descriptor_set_out=events.desc ../proto/event.proto
```

生成に使う protoc は、`pyproject.toml` の `protobuf>=6,<7` ランタイムと同じメジャーバージョン系列のものを使うこと。

## 実行例 (EMR Serverless / EMR on EKS)

```bash
spark-submit \
  --conf spark.jars.packages=org.apache.spark:spark-protobuf_2.12:3.5.6,org.apache.spark:spark-sql-kafka-0-10_2.12:3.5.6,org.apache.hadoop:hadoop-aws:3.4.1 \
  consumer.py
```

## 既知の制約

- **at-least-once**: ジョブ再実行やリトライで同一レコードが複数回書き込まれる可能性がある。重複が問題になる場合は下流 (集計クエリ・後段ジョブ) で排除する想定。現状、重複排除用のキー (Kafka の `partition`/`offset` など) は出力に含めていない。
- **複数 partition をまたぐ atomic commit は無い**: parquet + `mode("append")` は、S3 書き込みコミット (EMRFS / EMR S3A) の仕様上、タスク単位でしかコミットが保証されない。ジョブ内の一部タスクが成功した状態で他のタスクが失敗すると、成功済みタスクの分は取り消されずに残る。全体で真にトランザクショナルな書き込みが必要な場合は Iceberg 等のテーブルフォーマットへの移行が必要。
- **DLQ の `reason` は `deserialize_error` のみ**: producer が Kafka header を付与しない設計のため、schema/version の不一致検知は行っていない。
