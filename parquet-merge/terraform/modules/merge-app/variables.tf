variable "name_prefix" {
  type        = string
  description = "リソース名の接頭辞。Lambda・IAM ロール・Scheduler・consumer group の名前に使う。同じアカウントの複数環境では環境ごとに変える"
}

variable "subnet_ids" {
  type        = list(string)
  description = "ESM のポーラー ENI を置く subnet。Kafka と同じ subnet を渡す"
}

variable "esm_security_group_id" {
  type        = string
  description = "ESM のポーラー ENI に付ける SG（Kafka への到達性を持つもの）"
}

variable "bootstrap_dns_name" {
  type        = string
  description = "Kafka の bootstrap に使うホスト名"
}

variable "bootstrap_port" {
  type        = number
  description = "Kafka の bootstrap port"
}

variable "topic_name" {
  type        = string
  description = "購読する Kafka topic 名"
  default     = "sample-readings"
}

variable "table_bucket_arn" {
  type        = string
  description = "本テーブルを置く S3 Tables のテーブルバケットの ARN。ジョブ実行ロールが読み書きできるのはこのバケットだけ"
}

variable "namespace_name" {
  type        = string
  description = "本テーブルの namespace"
}

variable "table_name" {
  type        = string
  description = "本テーブルの名前"
  default     = "sample_readings"
}

variable "catalog_name" {
  type        = string
  description = "ジョブの Spark セッションで S3 Tables の catalog に付ける名前。MERGE の対象は <catalog_name>.<namespace_name>.<table_name> になる"
  default     = "s3tablesbucket"
}

variable "staging_bucket_name" {
  type        = string
  description = "staging バケット名。Lambda の出力先で、ジョブの入力と状態の置き場"
}

variable "staging_bucket_arn" {
  type        = string
  description = "staging バケットの ARN"
}

variable "artifact_bucket_name" {
  type        = string
  description = "function.zip とジョブのスクリプトを置いたバケット名"
}

variable "artifact_bucket_arn" {
  type        = string
  description = "artifact バケットの ARN"
}

variable "emr_application_id" {
  type        = string
  description = "MERGE ジョブを実行する EMR Serverless アプリの ID"
}

variable "emr_application_arn" {
  type        = string
  description = "EMR Serverless アプリの ARN。Scheduler 実行ロールが StartJobRun できる対象"
}

variable "s3_object_version" {
  type        = string
  description = "function.zip の S3 VersionId。S3Key だけではコードの変更を検知できないため明示する"
}

variable "lambda_timeout_seconds" {
  type        = number
  description = "Lambda の timeout（秒）。merge_margin_seconds はこれより 30 秒以上長くする"
  default     = 60
}

variable "lambda_memory_mb" {
  type        = number
  description = "Lambda のメモリ（MB）。pyarrow の読み込みとバッチの変換に足りる大きさにする"
  default     = 512
}

variable "log_retention_days" {
  type        = number
  description = "Lambda の CloudWatch Logs の保持日数"
}

variable "esm_starting_position" {
  type        = string
  description = "ESM の開始位置（LATEST / TRIM_HORIZON）"
  default     = "LATEST"
}

variable "esm_batch_size" {
  type        = number
  description = "ESM が 1 回の呼び出しに渡すレコード数の上限"
}

variable "esm_max_batching_window_seconds" {
  type        = number
  description = "ESM がバッチを溜める最大時間（秒）"
}

variable "schedule_expression" {
  type        = string
  description = "MERGE ジョブを起動する間隔。ワーカーの起動時間（1 分前後）より十分長くする"
  default     = "rate(5 minutes)"
}

variable "merge_margin_seconds" {
  type        = number
  description = "MERGE ジョブの --margin-seconds。now からこの秒数より新しい分は処理しない"
  default     = 120

  # writer が付ける分のラベルは呼び出し開始時刻で決まり、PUT はその後 timeout までの間に起こり得る。
  # 30 秒は Lambda と EMR の時計のずれの余裕。
  validation {
    condition     = var.merge_margin_seconds >= var.lambda_timeout_seconds + 30
    error_message = "merge_margin_seconds は lambda_timeout_seconds + 30 以上にする（まだ書き込み中の分を処理しないため）"
  }
}

variable "spark_submit_parameters" {
  type        = string
  description = "StartJobRun の sparkSubmitParameters に追加する部分。S3 Tables の catalog の jar と、spark.sql.extensions・spark.sql.catalog.<catalog_name> の設定を渡す。--py-files・--files・warehouse はこの module が付ける。EMR Serverless 7.14.0 での jar の渡し方は未確認のため既定値を持たない"
}
