variable "region" {
  type        = string
  description = "デプロイ先の AWS リージョン。infra と同じ値にする"
}

variable "environment" {
  type        = string
  description = "環境名（dev / stg / prd）。default_tags に付与する"
}

variable "name_prefix" {
  type        = string
  description = "リソース名の接頭辞。Lambda・IAM ロール・Scheduler の名前に使う。同じアカウントの複数環境では環境ごとに変える"
}

variable "subnet_ids" {
  type        = list(string)
  description = "ESM のポーラー ENI を置く subnet（既存の Kafka と同じ subnet）"
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
  description = "本テーブルを置く S3 Tables のテーブルバケットの ARN。アカウント ID を含む"
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

variable "function_object_version" {
  type        = string
  description = "function.zip の S3 VersionId。scripts/build-artifacts.sh が出力する手順でアップロードして得る。ロールバックは、前の値に書き戻して適用する"
}

variable "lambda_timeout_seconds" {
  type        = number
  description = "Lambda の timeout（秒）。merge_margin_seconds はこれより 30 秒以上長くする"
}

variable "lambda_memory_mb" {
  type        = number
  description = "Lambda のメモリ（MB）"
}

variable "log_retention_days" {
  type        = number
  description = "Lambda の CloudWatch Logs の保持日数"
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
  description = "ESM がバッチを溜める最大時間（秒）"
}

variable "schedule_expression" {
  type        = string
  description = "MERGE ジョブを起動する間隔"
}

variable "merge_margin_seconds" {
  type        = number
  description = "MERGE ジョブの --margin-seconds。lambda_timeout_seconds + 30 以上にする"
}

variable "spark_submit_parameters" {
  type        = string
  description = "StartJobRun の sparkSubmitParameters に追加する部分（S3 Tables の catalog の jar と catalog の設定）。EMR Serverless 7.14.0 での渡し方は未確認"
}
