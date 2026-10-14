variable "cluster_name" {
  type        = string
  description = "Lambda・実行ロール・consumer group 名の接頭辞に使うクラスター名"
}

variable "subnet_ids" {
  type        = list(string)
  description = "ESM のポーラー ENI を置く subnet。AZ 跨ぎのデータ転送料を避けるため、NLB・nodegroup と同じ subnet を渡す"
}

variable "esm_security_group_id" {
  type        = string
  description = "ESM のポーラー ENI に付ける SG"
}

variable "data_bucket_arn" {
  type        = string
  description = "Lambda が読み取るデータ用バケットの ARN"
}

variable "artifact_bucket_name" {
  type        = string
  description = "Lambda デプロイパッケージ（function.zip）を置いたバケット名"
}

variable "code_object_version" {
  type        = string
  description = "function.zip の S3 VersionId。S3Key だけではコードの変更を検知できないため明示する"
}

variable "bootstrap_dns_name" {
  type        = string
  description = "Kafka の bootstrap に使うホスト名（Private Hosted Zone の名前）"
}

variable "bootstrap_port" {
  type        = number
  description = "Kafka の bootstrap port（external listener の port）"
}

variable "topic_name" {
  type        = string
  description = "購読する Kafka topic 名"
}

variable "table_bucket_arn" {
  type        = string
  description = "書き込み先の S3 Tables の table bucket の ARN"
}

variable "log_retention_days" {
  type        = number
  description = "CloudWatch Logs の保持日数"
}

variable "force_destroy" {
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
