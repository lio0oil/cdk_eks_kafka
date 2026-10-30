# Parquet 出力 + 定期 MERGE サンプル設計

作成日: 2026-10-30

[DEDUP-OPTIONS.md](DEDUP-OPTIONS.md) の推奨案（5.2 節「Parquet 出力 + 定期 MERGE」、9 章）を、動くサンプルとして実装するための設計。

## 1. 方針

- **既存ソースは編集しない**。`kafka2/`、`scripts/s3tables/`、既存の Terraform モジュール・env には手を入れず、新規の `parquet-merge/` ディレクトリ 1 つで完結させる。既存の Terraform モジュールは呼ばず、既存リソースの値は変数で受け取る（3 章、7 章）。
- **Lambda は Iceberg に commit しない**。加工した行を 1 バッチ 1 ファイルの Parquet として通常の S3 バケットに書くだけにする。
- **MERGE ジョブは 1 本だけ**。PySpark で、書き込みの終わった Parquet を読み、本テーブルにないメッセージだけを `MERGE INTO` で入れる。
- **テスト先行**。重複排除のロジックはローカルの pytest（PySpark + ローカル Iceberg catalog）で red から green まで固める。`apply` は行わず、Terraform は `validate` / `plan` / `terraform test` まで。

## 2. 全体像

```
Kafka（既存の topic。今回の範囲外）
  → Lambda（ESM）  zlib 解凍 → protobuf デコード → 1 メッセージ N 行に展開 → バッチ内重複排除
       → s3://<staging>/incoming/t=<PUT 時刻の分>/<batch_id>.parquet   （1 バッチ 1 ファイル）
       → 壊れたデータは s3://<staging>/dlq/ に Parquet で退避
  → [間隔ごと] EventBridge Scheduler → EMR Serverless（maxConcurrentRuns=1）で MERGE ジョブ
       1. 前回より後に書き終わった分の Parquet を読む
       2. 入力の中でメッセージごとに 1 ファイルの行だけ残す
       3. 本テーブルへ MERGE INTO（キーが無い行だけ INSERT）、merge_run_id と merged_at を付ける
       4. 処理済みの時刻（watermark）を S3 に記録
  → S3 Tables（Iceberg）  sample_readings
```

## 3. 新規ディレクトリ構成

リポジトリ直下に、`kafka/`、`kafka2/` と並ぶ新しいトップレベルディレクトリ `parquet-merge/` を 1 つ作り、コード・テスト・テーブル定義・Terraform をすべてその中に置く。`samples/` や既存の `terraform/` 配下には置かない。

| パス | 内容 |
|---|---|
| `parquet-merge/proto/readings.proto` | 新しい proto |
| `parquet-merge/writer/` | Lambda（Parquet 書き込み側）。独立した uv プロジェクト（`pyproject.toml`、`uv.lock`、`tests/`）。Python 3.13（Lambda ランタイムに合わせる）。依存は pyarrow、protobuf。生成した `readings_pb2.py` もここに置く（`kafka2/producer/event_pb2.py` と同じ扱い） |
| `parquet-merge/merger/` | PySpark の MERGE ジョブ。独立した uv プロジェクト（同上）。Python 3.11（EMR 7.14.0 に合わせる）。PySpark 3.5.8 は dev グループ（EMR に同梱されるため） |
| `parquet-merge/contract/staging_schema.json` | staging の Parquet の列定義（列名と型）。writer の出力と merger の入力が一致していることを、両方のテストが同じこのファイルで検査する |
| `parquet-merge/tables/sample_readings.json` | S3 Tables のテーブル定義 |
| `parquet-merge/terraform/` | 既存の `terraform/` と同じ構成（`modules/`、`env/dev/`、`scripts/`）を、この中に再現する。詳細は下 |

```
parquet-merge/terraform/
  modules/
    merge-infra/    main.tf outputs.tf variables.tf versions.tf tests/（*.tftest.hcl、ordering.sh）
    merge-app/      main.tf outputs.tf variables.tf versions.tf tests/（同上）
  env/dev/
    infra/          backend.tf main.tf outputs.tf providers.tf terraform.tfvars variables.tf versions.tf tests/（wiring.tftest.hcl）
    app/            （同上）
  scripts/
    check.sh graph.sh
```

- **uv プロジェクトを writer と merger で分ける理由**: 実行環境が違う（Lambda の Python 3.13 と EMR の Python 3.11）、依存が重ならない（pyarrow / protobuf と PySpark）。既存の `kafka2/consumer`、`kafka2/producer`、`cdk` も、コンポーネントごとに独立した uv プロジェクトを持つ。
- **層の分け方は既存の `consumer-infra` / `consumer-app` と同じ**。コード（`function.zip`）の置き場は先に要り、`VersionId` はアップロード後にしか分からないので、`infra`（置き場などの土台）→ コードのアップロード → `app`（Lambda、ESM、実行ロール、Scheduler）の順に適用する。
- **backend** は既存と同じ S3 バケットで、key だけを `dev/parquet-merge-infra.tfstate`、`dev/parquet-merge-app.tfstate` にする。provider、`versions.tf`（`>= 1.10`、aws `>= 6.0`）、`default_tags` も既存と同じ。
- **既存インフラは参照しない**。既存の `terraform/` の state（`terraform_remote_state`）は読まず、既存リソースの値は、`terraform.tfvars` に書くパラメータ（変数）で受け取る。変数の一覧は 7 章。`parquet-merge/terraform/` は、既存の state に依存せず単独で `plan` できる。
- **自分の層どうしの受け渡し**: `app` が `infra`（どちらも `parquet-merge/terraform/` の層）の出力を受け取る部分は、既存の `consumer-app` と同じく `terraform_remote_state`（`dev/parquet-merge-infra.tfstate`）を使う。
- **バケット**: 既存の `terraform/modules/secure-bucket` は呼ばず、`merge-infra` の中に同等の定義（暗号化、public access 遮断、TLS 必須ポリシー、バージョニング）を持つ。`parquet-merge/` を単体で完結させ、消せば元に戻る状態を保つため。
- **検査スクリプト**: 既存の `terraform/scripts/check.sh` は `terraform/modules/*/` と `terraform/env/*/*/` だけを見るので、この構成は対象外になる。同じ内容を `parquet-merge/terraform/scripts/check.sh` として新規に置く。pre-commit への登録は 9 章。
- **独立させる理由**: 既存の `terraform/` の構成と `scripts/s3tables/` の挙動を変えず、`parquet-merge/` を消せば元に戻る状態にする。`scripts/s3tables/create-tables.sh` は `tables/*.json` を全部作るので、そこにテーブル定義を足すと既存の手順の挙動が変わる。テーブル作成は、このディレクトリの JSON を `aws s3tables create-table` に渡す 1 コマンドで足りる（手順は 9 章）。
- **Lambda のデプロイ**: 既存と同じ、artifact バケット + `function.zip` + 明示 `VersionId` の方式。artifact バケットは既存のものを使わず、`merge-infra` で新規に作る。

## 4. データモデル（新規）

### 4.1 proto

```proto
message ReadingsMessage {
  string device_id = 1;
  string device_code = 2;
  string message_id = 3;       // UUID
  int64 sent_at = 4;           // デバイスの送信時刻
  repeated Reading readings = 5;
}
message Reading {
  string metric = 1;
  double value = 2;
  int64 measured_at = 3;       // 計測時刻
}
```

正本は [parquet-merge/proto/readings.proto](../parquet-merge/proto/readings.proto)。

- Kafka の value は `zlib.compress(ReadingsMessage.SerializeToString())`。
- **時間はすべて Unix time（UTC、マイクロ秒）の int64**。Iceberg の timestamptz と同じ精度なので、変換で丸めは起きず、Parquet のタイムゾーン付き timestamp（UTC、マイクロ秒）にそのまま書ける。
- **partition に使う `device_code`（メッセージ単位）と `Reading.measured_at`（行単位）は、デバイスが再送しても変わらない値**であることが前提（同一テーブル partition 条件、DEDUP-OPTIONS 2.4 節）。取り込み時刻や Kafka の timestamp は使わない。
- **1 メッセージの N 行は、`measured_at` によって複数の時間 partition に分かれ得る**。同じメッセージのコピーは、同じ行が同じ partition に入るので、重複の照合は成り立つ。
- 行識別子（`row_index` 等）は持たせない。upsert ではなく「キーが無ければ INSERT」の MERGE なので不要（DEDUP-OPTIONS 2.5 節）。

### 4.2 Iceberg テーブル `sample_readings`

| 列 | 型 | 内容 |
|---|---|---|
| `device_id` | string | 重複キー |
| `message_id` | string | 重複キー |
| `device_code` | string | message の `device_code`。partition 元 |
| `sent_at` | timestamptz | message の `sent_at`（Unix time を変換）。デバイスの送信時刻 |
| `measured_at` | timestamptz | Reading の `measured_at`（Unix time を変換）。partition 元 |
| `metric` | string | Reading の `metric` |
| `value` | double | Reading の `value` |
| `written_at` | timestamptz | Lambda が staging にバッチを書いた時刻。同じバッチの全行で同じ値 |
| `merge_run_id` | string | MERGE の実行 ID。時刻順に並ぶ文字列（例: `20261030T120500Z-1a2b3c4d`）。読む側の差分抽出に使う |
| `merged_at` | timestamptz | MERGE が本テーブルに INSERT した時刻（登録日時）。1 回の実行の全行で同じ値 |

- **時刻列の型は `timestamptz`**（Spark の `TimestampType`、staging の `timestamp_us_utc` に対応）。値は UTC の瞬間（Unix time）で、Spark の `TimestampType` が瞬間を表す型であり、Iceberg では `timestamptz` に対応するため。
- **partition**: `device_code`（identity）と `hour(measured_at)`。MERGE を数分間隔で回しても 1 回に読む量を抑えるため、時間は日単位ではなく時間単位（DEDUP-OPTIONS 5.2 節の表）。
  - **どちらも再送で変わらない値**が前提。この前提が崩れると、コピーが別の partition に入って照合されず、重複が漏れる（データは欠落しない。DEDUP-OPTIONS 2.6 節）。
  - **取り込み時刻（`written_at`）は使わない**。再送されたコピーは届いた時点の値になるため。
  - **`device_id` と `message_id` は使わない**。DEDUP-OPTIONS 2.2 節の条件で、重複キーはテーブルの partition にしない。
  - **`device_code` の取り得る値の数が分かっていない**。値の種類が多いと、時間 partition ごとのファイル数が `device_code` の種類数だけ増え、小さいファイルが増える（MERGE 1 回で書くファイルは partition ごとに最低 1 つ）。種類数を確認する（10 章）。
  - **照合の条件**: `device_code` は等値、`measured_at` は入力に含まれる時間ごとの範囲で、本テーブル側に直接書く（6.2 節）。
- **フォーマット**: Iceberg v2 に固定する。v3 にすると Athena で読めなくなり、v3 から v2 には戻せないため、今回は上げない。
  - テーブル定義 JSON の `properties` に `"format-version": "2"` を明示する（指定しなくても v2 が既定だが、`ALTER TABLE ... SET TBLPROPERTIES ('format-version'='3')` による片道の切り替えが、誰かの操作で起きても気づけるよう、意図を残す）。
  - S3 Tables の `create-table` がこのプロパティをそのまま受け付けるかは、実環境でのテーブル作成時に確認する（9 章の手順の後に、`get-table-metadata-location` で得た metadata の `format-version` が 2 であることを確かめる）。
  - merger のローカルテストでも、テーブルを `format-version = 2` で作り、MERGE 後も v2 のままであることを検査する。
- **sort order**: テーブル定義の JSON では指定しない。差分抽出を重視する場合は `merge_run_id` を先頭にするが、S3 Tables の sort compaction 後にしか効かないため、サンプルでは扱わない。

## 5. Lambda（writer）の設計

### 5.1 入力と処理

- 入力: self-managed Kafka の ESM イベント。`records` は `"<topic>-<partition>"` をキーにしたリストで、各要素の `value` は base64、`offset` を持つ。
- 1 レコードごとの処理: base64 → zlib 解凍（上限付き）→ protobuf デコード → 行に展開。

| ステップ | 失敗時 |
|---|---|
| zlib 解凍 | DLQ（`reason=zlib_error`）。解凍後サイズが上限を超えた場合は `reason=too_large` |
| protobuf デコード | DLQ（`reason=decode_error`） |
| `device_id`、`message_id`、`device_code` のいずれかが空、または `sent_at` / `Reading.measured_at` が範囲外（西暦 1 年〜9999 年に収まらない） | DLQ（`reason=invalid_field`） |

- **バッチ内重複排除**: 同じ `(device_id, message_id)` は最初の 1 件だけ残す（DEDUP-OPTIONS 7 章）。DLQ に出したレコードは対象に入れない（壊れたレコードが、同じキーの正常なレコードを落とさないため）。
- **解凍上限**: `zlib.decompressobj().decompress(data, max_length)` で解凍後サイズを制限する（zlib 爆弾対策）。

### 5.2 出力

- **ファイル配置**: `incoming/t=<分>/<batch_id>.parquet`。
  - `<分>` は PUT 時刻を 1 分で切り下げた UTC の値（例: `t=20261030T1205`）。固定で、設定値にはしない。MERGE の間隔を変えても、この配置は変わらない。
  - `<batch_id>` は、イベントに含まれる各 `(topic, partition)` の最初と最後の offset から決める（決定的）。同じバッチの Lambda 再試行は同じオブジェクトキーの上書きになる。
- **Parquet**: pyarrow、zstd 圧縮。全行に同じ `written_at` を入れる。
- **ReportBatchItemFailures は使わない**。DEDUP-OPTIONS 7 章は有効化を求めているが、これは Iceberg へ commit する案で「一部だけ commit 済み」になるのを避けるためのもの。この設計では出力が 1 オブジェクトの PUT で、成功か失敗かの 2 通りしかない。S3 への PUT が失敗したら例外でバッチ全体を失敗させて再試行し、途中までの状態は残らない。壊れたデータは DLQ に書いて成功扱いにする。
- **DLQ**: `dlq/t=<分>/<batch_id>.parquet`。列は `topic`、`partition`、`offset`、`reason`、`payload`（元のバイト列）。DLQ の書き込みに失敗した場合も例外でバッチ全体を失敗させる。
- **パッケージング**: pyarrow を含めた `function.zip` を作る。サイズが上限（展開後 250MB）に収まらなければ、AWS 提供の SDK for pandas レイヤーに切り替える（実装時に確認）。

## 6. MERGE ジョブ（merger）の設計

### 6.1 処理対象の決め方と処理済みの管理

- **締め時刻（cutoff）**: `now - margin` を 1 分に切り下げた時刻。`margin >= Lambda のタイムアウト + 時計のずれ`（例: 120 秒）。
  - **処理対象は「watermark より後で、cutoff より前の分」**。cutoff の分は含めない。writer の分ラベルは Lambda の呼び出し開始時刻で決まり、PUT はその後タイムアウトまでの間に起こり得る。そのため、cutoff の分にはまだ新しいファイルが現れ得る。cutoff より前の分だけが、読み取り済みとして扱える。
- **watermark**: `state/watermark.json` に「処理を終えた最後の分」を記録する。値は cutoff の 1 分前（処理対象に入った最後の分）。ジョブは `watermark より後 〜 cutoff の手前まで` の分の prefix を処理する。
  - 対象が 0 件でも watermark は進む（閉じた分が空だと分かった状態を残すため）。ただし戻らない（現在の値と新しい値の大きい方を書く）。
  - 初回（watermark なし）は、`incoming/` にある最も古い分から（cutoff の分は除く）。
  - **処理 → 本テーブルへの commit → watermark の更新**の順。commit 後、watermark 更新前に落ちると同じ範囲を再処理するが、MERGE は本テーブルにあるキーを入れないので重複は入らない。
  - Parquet は消さない。S3 のライフサイクルで一定日数後に削除する。
- **対象ファイルの列挙**: staging は通常の S3 バケットなので、prefix を列挙できる。分の一覧は、watermark と cutoff から決める。
- **間隔との関係**: ジョブの実行間隔は Scheduler の `rate(...)` だけで決まり、自由に変えられる。間隔が長ければ 1 回に処理する分の数が増え、短ければ減る。watermark が時刻なので、間隔を変えても整合は崩れない。

### 6.2 処理

1. 対象の分の Parquet を読む。`input_file_name()` で元ファイルのパスを列 `_source_file` に持つ。`datetimeRebaseMode=CORRECTED` で読む。writer は西暦 1 年〜9999 年を受け入れるが、Spark の既定（`EXCEPTION`）では 1900 年より前などの古い値を読めず、1 件で全実行が失敗するため。writer の pyarrow も Spark 3 も先発グレゴリオ暦で、マイクロ秒の INT64 で持つので、CORRECTED で値は変わらない。
2. **入力内の重複排除**: `(device_id, message_id)` ごとに、1 つの `_source_file` の行だけを残す。選ぶファイルは `(written_at, _source_file)` の最小。コピーを丸ごと残すので、別のコピーの行が混ざらない（DEDUP-OPTIONS 2.5 節）。
3. **照合範囲の限定**: 入力に含まれる `(device_code, 1 時間の範囲)` の組を求め、本テーブル側の条件に値で直接書く（DEDUP-OPTIONS 2.6 節）。
   - 時間の切り出しは UTC の整数演算（エポックからのマイクロ秒を 1 時間で割る）。セッションの TZ に依存しない。
   - 時間範囲は `TIMESTAMP '...'` のリテラルではなく `timestamp_micros(<epoch マイクロ秒>)` で書く。9999 年 12 月 31 日 23 時台の次の時間は西暦 10000 年になり、リテラルも Python の `datetime` も表せないため。
   - 照合条件は、`(device_code, 時間範囲)` の組ごとの OR。`device_code` と時間範囲を別々に集めた直積にはしない（入力にない組の partition まで読むのを避ける）。
4. `merge_run_id` を採番し、入力に付ける。あわせて `merged_at` も入力に付ける。`merge_run_id` と `merged_at` は、ジョブ起動時に 1 回だけ取る同じ現在時刻（`now`）から作る。`merge_run_id` の時刻部分は秒まで、`merged_at` はマイクロ秒まで持つ。
5. 実行:

```sql
MERGE INTO target t
USING src s
  ON  t.device_code = s.device_code
  AND t.device_id = s.device_id
  AND t.message_id = s.message_id
  AND (
       (t.device_code = <code1> AND t.measured_at >= timestamp_micros(<時間の始まり>) AND t.measured_at < timestamp_micros(<時間の終わり>))
    OR (t.device_code = <code2> AND ...)
    OR ...
  )
WHEN NOT MATCHED THEN INSERT *
```

6. watermark を更新する。

### 6.3 実行エンジンと 1 本ずつの制御

- **EMR Serverless 7.14.0**（Spark 3.5.8、Iceberg 1.10.1、Python 3.11、Java 17）。`kafka2/consumer` は 7.13.0 を対象にしているが、独立したプロジェクトなので揃えない。アプリケーションの `maxConcurrentRuns = 1` で直列化する（DEDUP-OPTIONS 5.2 節）。起動は EventBridge Scheduler の `StartJobRun` 直接呼び出しで、`clientToken` に Scheduler の実行 ID を渡す。
- **起動と終了**: ジョブ（job run）は処理が終わると終了し、ワーカーは解放される。課金はジョブの実行中のリソースだけ。アプリケーションは、事前初期化済みのキャパシティを持たず、auto start / auto stop（アイドルで停止）を有効にする。停止中の費用はかからない。代わりに、ジョブごとにワーカーの起動時間（目安は 1 分前後、実測が必要）がかかるので、MERGE の間隔はこれより十分長くする。
- **カタログ**: S3 Tables の Iceberg catalog（`s3-tables-catalog-for-iceberg-runtime`）。EMR Serverless 7.14.0 でこの jar をどう渡すか（同梱か `--packages` か）は実装時に確認する。
- **ローカルテスト**: 同じ SQL を、ローカルの Hadoop catalog（`iceberg-spark-runtime-3.5_2.12`）で動かす。catalog 名と warehouse だけをテストの fixture で差し替える。
- **環境**: Python 3.11、PySpark 3.5.8、JDK 17（memory: JDK 25 では失敗する）。

## 7. Terraform の設計

`parquet-merge/terraform/`（構成は 3 章）。

| 層 | リソース | 内容 |
|---|---|---|
| `merge-infra` | staging バケット | `parquet-merge/` 内に同等の定義を持つ（`secure-bucket` は呼ばない）。`incoming/`、`dlq/` は 14 日、`state/` は削除しない |
| `merge-infra` | artifact バケット | `parquet-merge/` 内に同等の定義を持つ（バージョニング有効）。`function.zip` と MERGE ジョブの PySpark スクリプトを置く |
| `merge-infra` | EMR Serverless アプリ | `maxConcurrentRuns = 1`、`queueTimeoutMinutes` を設定 |
| `merge-app` | Lambda + ESM | 既存の topic を購読（topic 名は変数）。batch size と batching window は変数。`ReportBatchItemFailures` は無効 |
| `merge-app` | Lambda の実行ロール | staging の `incoming/*` と `dlq/*` への PutObject のみ |
| `merge-app` | ジョブの実行ロール | staging の読み取りと `state/*` の書き込み、S3 Tables の対象テーブルへの読み書き |
| `merge-app` | EventBridge Scheduler | `rate(5 minutes)`（変数）。`StartJobRun` を直接呼ぶ |

**既存リソースを受け取る変数**（`env/dev/app/terraform.tfvars` に実値を書く。アカウント ID などを含む値は、リポジトリに持たせない運用との兼ね合いで、必要なら `.tfvars` を `.gitignore` に入れて `terraform.tfvars.example` を置く形にする）

| 変数 | 内容 |
|---|---|
| `subnet_ids` | ESM のポーラー ENI を置く subnet |
| `esm_security_group_id` | ESM のポーラー ENI に付ける SG（Kafka への到達性を持つもの） |
| `bootstrap_dns_name`、`bootstrap_port` | Kafka の bootstrap の接続先 |
| `topic_name` | 購読する topic（既定値 `sample-readings`） |
| `table_bucket_arn`、`namespace_name` | 本テーブルを置く S3 Tables のテーブルバケットと namespace |
| `table_name` | 本テーブルの名前（既定値 `sample_readings`） |

- **Glue の外部テーブルは作らない**。DEDUP-OPTIONS 5.2 節は Athena から読む場合に必要としているが、この設計は Spark が S3 のパスを直接読むので不要。
- **テストは既存と同じ形式**。モジュールごとに `tests/*.tftest.hcl`、env ごとに配線を確かめる `wiring.tftest.hcl`、適用順序は `ordering.sh` で検査する。既存の state を読まないので、`terraform test` は変数に仮の値を渡すだけで動き、既存の実環境に依存しない。IAM の許可範囲（Lambda が本テーブルに書けないこと、ジョブの実行ロールが staging を削除できないこと）を invariant として検査する。

## 8. テスト計画（テスト先行）

**モックの方針**

- **`unittest.mock` は使わない**。`pytest-mock` の `mocker` フィクスチャ（`mocker.Mock(spec=...)`、`mocker.patch(...)`）だけを使う。守られているかは、各プロジェクトの ruff の設定（`flake8-tidy-imports` の `banned-api` で `unittest.mock` を禁止）で機械的に検査する。
- **S3 は moto で再現する**（`mock_aws`）。AWS の状態を再現して結果を検査する場合に使う。
- **使い分け**: 書かれた結果を読み戻して検査するなら moto、失敗の注入（例外を起こす）や呼び出しの検査なら `mocker`。
- **依存**: writer、merger の両方の dev 依存に `pytest-mock` と `moto` を入れる。moto のバージョンは、Python 3.13 と 3.11 の両方で動くものを、実装時に確認して pin する。
- **S3 Tables はモックしない**。merger のコードは S3 Tables の API（boto3 の `s3tables`）を呼ばず、Spark が catalog 経由で書き込むだけなので、モックする対象がない。moto の S3 Tables は、テーブルバケット、namespace、テーブルの作成・取得・一覧・削除などの管理 API の一部だけに対応し、`get_table_metadata_location` や Iceberg の REST catalog は対応していない（Spark からの書き込みは再現できない）。
- **EMR Serverless もモックしない**。起動は EventBridge Scheduler の `StartJobRun` で、アプリケーションのコードからは呼ばない。
- **MERGE の検査**: モックせず、ローカルの Spark と Iceberg（ローカルのファイルシステムと Hadoop catalog）で実際に動かす。S3 Tables の catalog との差は実環境で確認する。Spark の `s3a` を moto に向ける設定は、検査したいロジックと無関係なので行わない。

**merger のローカル Iceberg 環境**

- **catalog は Hadoop catalog**（`type=hadoop`）。warehouse は pytest の一時ディレクトリで、S3 は使わない。サーバーも Docker も要らない。
- **選んだ理由**: 検査するのは重複排除、MERGE、冪等性、watermark のロジックで、catalog の種類に依存しない。テストは単一プロセスで 1 本ずつ動き、MERGE は本番でも 1 本に直列化するため、Hadoop catalog の弱点（オブジェクトストア上の同時書き込みの排他）に当たらない。S3 Tables の catalog との差は、実環境で確認する。
- **構成**: PySpark 3.5.8（dev 依存）に、`spark.jars.packages` で `org.apache.iceberg:iceberg-spark-runtime-3.5_2.12:1.10.1`（EMR 7.14.0 の Iceberg と同じ）を指定する。jar は初回に Maven Central から取得するのでネットワークが要る。JDK は 17。
- **SparkSession は session スコープのフィクスチャで 1 つだけ作る**（起動に数十秒かかるため）。テストごとに、`format-version = 2`、partition は `device_code`（identity）と `hour(measured_at)` のテーブルを新しく作る。
- `kafka2/consumer` にある Hadoop の jar の差し替えスクリプトは不要（`s3a` を使わないため）。

**writer（pytest、Lambda イベントの dict を直接渡す）**

- 契約: 出力した Parquet の列名と型が `contract/staging_schema.json` と一致する（merger 側のテストも、同じファイルで入力を作る）。
- 正常: 1 メッセージ N 行が、全行同じ `written_at`、`device_code` で 1 ファイルに出る。`measured_at` は Reading ごとの値になり、Unix time から正しく timestamp に変換される。
- バッチ内の重複: 同じ `(device_id, message_id)` が 2 回あっても 1 メッセージ分しか出ない。
- 再試行: 同じイベントを 2 回処理しても、同じオブジェクトキーを上書きする（ファイルが増えない）。
- 壊れたデータ: zlib 不正、protobuf 不正、解凍後サイズ超過、`device_id` / `message_id` / `device_code` のいずれかが空、または時間の範囲外（`reason=invalid_field`）がそれぞれ DLQ に出て、他のレコードは正常に出る。DLQ に出したレコードは、バッチ内重複排除の対象にならない（同じ `(device_id, message_id)` の正常なレコードが落ちない）。
- S3 に書かれた結果（moto）: 書かれた Parquet を読み戻して、列、行数、`written_at` を検査する。同じイベントの再処理で、オブジェクトが 1 つのまま上書きされることも、オブジェクトの数で検査する。
- S3 の PUT 失敗: 例外が伝播する。`mocker.patch` でクライアントの `put_object` を失敗させる。

**merger（pytest、ローカル PySpark + ローカル Iceberg）**

- 1 回の入力の中に同じメッセージのコピーが 2 つ（別ファイル）あると、1 つのファイルの行だけが入る。別のコピーの行が混ざらない。
- 本テーブルに既にあるメッセージは入らない（別の実行で届いた重複）。
- 同じ範囲を 2 回処理しても、行数が変わらない（冪等）。
- 対象ファイルの列挙と watermark の読み書き（boto3 で実装し、moto の S3 で検査する）: `incoming/t=<分>/` の prefix から、watermark より後で cutoff より前の分だけを列挙する（cutoff の分は含めない）。`state/watermark.json` がなければ、cutoff の分を除いて最も古い分から始める。
- watermark: cutoff の分と、それより後の分（まだ閉じていない分）は処理しない。処理後に watermark が cutoff の 1 分前まで進む。対象が 0 件でも進み、戻らない。commit 後・watermark 更新前に落ちた状態から再実行しても重複しない。
- 照合範囲: 古い `measured_at` で遅れて届いたデータが、該当する `(device_code, 時間)` の partition だけ照合して入る。
- 1 メッセージの `readings` が複数の時間にまたがる場合（時間 partition をまたぐ場合）も、同じメッセージの別コピーが全行重複として落ちる。
- `merge_run_id`: 1 回の実行で入った全行に同じ値が入り、実行ごとに時刻順に増える。
- `merged_at`: 1 回の実行で入った全行が、呼び出し側から渡した時刻と同じ値になる。実行ごとに渡した値がそのまま入り、後の実行は前の実行の行を変えない。本テーブルに既にあるメッセージの重複コピーが落ちたときも、既存行の `merged_at` は書き換わらない。ジョブの起動処理では、同じ時刻から `merge_run_id` と `merged_at` が作られる。
- 新しい snapshot の operation が `append` で、既存のデータファイルを書き直さないこと（insert のみの MERGE の挙動をテストで固定する）。

**テーブル定義 JSON（`tables/sample_readings.json`、merger のプロジェクトのテストで静的に検査。モック不要）**

- 列が `contract/staging_schema.json` と矛盾しない（MERGE が付ける `merge_run_id` と `merged_at` を加えた形）。`sent_at`、`measured_at`、`written_at` の型が `timestamptz`（staging の `timestamp_us_utc` に対応）。
- partition の `sourceId` が、`device_code`（identity）と `measured_at`（hour）の列 ID を指している。
- `format-version` が `"2"`。

**Terraform**: `terraform validate`、`terraform test`、`plan`。ジョブの実行ロールが、変数で渡した対象のテーブルバケットだけに読み書きできることも、IAM ポリシー文書で検査する。

## 9. 実装の順序

1. proto と、`parquet-merge/writer/`、`parquet-merge/merger/` の 2 つの uv プロジェクト（ruff、pyright、pytest、pytest-mock）の雛形。`contract/staging_schema.json` もここで置く。
2. writer のテストを書き、red を確認して実装。
3. merger のテストを書き、red を確認して実装（SQL → cutoff と watermark の順）。
4. テーブル定義の JSON と、作成コマンドの手順。
5. Terraform を `merge-infra` → `merge-app` の順に、各層で module → env の順に、`terraform test` を先に書いて実装。`scripts/check.sh` もここで置く。
6. ユーザーによる `apply` と実環境での確認（MERGE が間隔内に終わるか、など）。

**pre-commit への登録（自動検査）**: 既存の `.pre-commit-config.yaml` に、`parquet-merge/` 用の hook を追加する。既存の hook は変更しない。対象のコードやテストがまだ無い段階で入れるとコミットが失敗するため、Python の hook は手順 2 の後、Terraform の hook は手順 5 の後に入れる。

| hook | 対象 | 実行内容 |
|---|---|---|
| ruff check / ruff format | `^parquet-merge/(writer\|merger)/.*\.py$` | プロジェクトごとに `uv run --project parquet-merge/<writer または merger> ruff ...`（既存の `cdk` 用と同じ形） |
| pyright | writer、merger それぞれの Python | プロジェクトごとに `uv run --directory parquet-merge/<...> pyright` |
| pytest | `^parquet-merge/(writer\|merger\|contract)/` | プロジェクトごとに `uv run --directory parquet-merge/<...> pytest` |
| terraform fmt | `^parquet-merge/terraform/.*\.(tf\|tfvars\|tftest\.hcl)$` | `terraform fmt -check -recursive -diff parquet-merge/terraform` |
| terraform test | `^parquet-merge/terraform/` | `bash parquet-merge/terraform/scripts/check.sh` |
| tflint | `^parquet-merge/terraform/.*\.(tf\|tfvars)$` | `tflint --recursive --chdir=parquet-merge/terraform` |

テーブル作成（apply 後にユーザーが実行）:

```bash
aws s3tables create-table \
  --table-bucket-arn "<テーブルバケットの ARN>" \
  --namespace "<namespace>" \
  --name sample_readings --format ICEBERG \
  --metadata file://parquet-merge/tables/sample_readings.json
```

## 10. 決めたこと・残る確認事項

**このサンプルで割り切ること**

- 遅れて届くデータが多くの時間帯に散らばる場合の、MERGE の経路分け（DEDUP-OPTIONS 5.2 節）は実装しない。照合は時間ごとの範囲の OR にとどめる。
- 反映方法は MERGE の `INSERT` のみ。`add_files` は扱わない。
- Athena での読み取りと、読む側の差分抽出の実装は扱わない。`merge_run_id` 列を付けるところまで。
- 流量の検証はしない。ロジックの正しさが対象で、性能は実環境で測る。

**実装時に確認すること**

- EMR Serverless 7.14.0 で S3 Tables の catalog を使うための jar の渡し方。
- 7.14.0 からは、アプリケーション側の Spark 設定とジョブ側の設定が置き換えではなく**マージ**される（同じ項目はジョブ側が優先）。アプリケーションとジョブのどちらに設定を置くかを決めるときに前提にする。
- Spark の insert のみの MERGE が、本当に既存ファイルを書き直さない append になるか（8 章のテストで確認）。
- pyarrow を含めた `function.zip` のサイズ。

**決めたこと**

- バケットは、既存の `secure-bucket` モジュールを呼ばず、`parquet-merge/` の中に持つ（ユーザー決定）。3 章・7 章はこの決定に合わせて書き換え済み。
- topic 名は `merge-app` の変数 `topic_name` で変更できるようにする。既定値は `sample-readings`。

**ユーザーに確認したいこと**

- `device_code` の取り得る値の数は未確認（partition の細かさ、小さいファイルの数に影響する）。`device_code` は文字列。テストでは数種類の値を使う。

**範囲外にしたこと**

- Kafka の topic の作成（Terraform、manifest とも）。Lambda 以降だけを作る。
- producer 側のサンプル。Lambda の end-to-end 確認は、イベントの dict を手元で組み立てた invoke のみになる。ただし、自分で作る `ReadingsMessage` の proto は、既存 topic に実際に流れている形式とは別物なので、実際に topic を購読させる場合は、その topic に `ReadingsMessage` を送る側を別途用意する必要がある。
- MERGE の間隔は Scheduler の変数（既定 5 分）。変更はいつでもできる。
