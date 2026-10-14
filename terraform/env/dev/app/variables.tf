variable "region" {
  type        = string
  description = "デプロイ先の AWS リージョン。infra と同じ値にする"
}

variable "environment" {
  type        = string
  description = "環境名（dev / stg / prd）。default_tags に付与する"
}

variable "kafka_consumer_code_object_version" {
  type        = string
  description = "Lambda コンシューマの function.zip の S3 VersionId。zip のアップロード（cdk/scripts/upload-kafka-consumer-lambda.sh）は Terraform の外で行うため、アップロードして VersionId を得てから terraform.tfvars に書いて適用する。ロールバックは、書き戻して適用する"
}

variable "log_retention_days" {
  type        = number
  description = "CloudWatch Logs の保持日数"
}

variable "log_force_destroy" {
  type        = bool
  description = "DLQ バケットを中身ごと、ロググループを destroy 時に削除可能にするか。環境を破棄する dev のみ true"
}

variable "lambda_runtime" {
  type        = string
  description = "Lambda コンシューマの runtime"
}

variable "lambda_timeout_seconds" {
  type        = number
  description = "Lambda コンシューマの timeout（秒）"
}

variable "lambda_application_log_level" {
  type        = string
  description = "Lambda の Advanced Logging Controls のアプリケーションログレベル（JSON 形式でのみ指定できる）"
}

variable "esm_starting_position" {
  type        = string
  description = "ESM の開始位置（LATEST / TRIM_HORIZON）"
}

variable "esm_batch_size" {
  type        = number
  description = "ESM が 1 回の呼び出しに渡すレコード数の上限"
}

variable "esm_max_batching_window_seconds" {
  type        = number
  description = "ESM がバッチを溜める最大時間（秒）。一度変更すると既定（500ms）には ESM の再作成でしか戻せない"
}
