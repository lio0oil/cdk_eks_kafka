region      = "ap-northeast-1"
environment = "dev"

# バケット名は <name_prefix>-staging- / <name_prefix>-artifact- に乱数が付く。EMR Serverless のアプリ名にもなる
name_prefix = "parquet-merge-dev"

# dev は環境破棄を容易にするため中身ごと削除可能にする（stg/prd は false でデータを保護する）
force_destroy = true

# incoming/ と dlq/ の保持日数。watermark を戻して再処理できる期間の上限になる
staging_retention_days = 14

# merger の PySpark 3.5.8 / Iceberg 1.10.1 は EMR 7.14.0 に同梱されるものに合わせてある
emr_release_label = "emr-7.14.0"

# 最小値。MERGE は冪等で、次回の実行が同じ範囲を処理し直すため、長く待たせる意味がない
emr_queue_timeout_minutes = 15
