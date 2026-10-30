variable "name_prefix" {
  type        = string
  description = "リソース名の接頭辞。バケット名（<name_prefix>-staging- など）と EMR Serverless アプリ名に使う"
}

variable "force_destroy" {
  type        = bool
  description = "バケットを中身ごと削除可能にするか。環境を破棄する dev のみ true"
}

variable "staging_retention_days" {
  type        = number
  description = "staging の incoming/ と dlq/ を削除するまでの日数。MERGE が watermark を戻して再処理できる期間の上限になる"
  default     = 14
}

variable "release_label" {
  type        = string
  description = "EMR Serverless のリリースラベル。merger の PySpark / Iceberg のバージョン（EMR に同梱されるもの）と揃える"
  default     = "emr-7.14.0"
}

variable "queue_timeout_minutes" {
  type        = number
  description = "実行中のジョブがあるとき、後続のジョブが待機キューに留まる時間（分）。この時間を過ぎると失敗として取り消される"
  default     = 15

  validation {
    condition     = var.queue_timeout_minutes >= 15 && var.queue_timeout_minutes <= 720
    error_message = "queue_timeout_minutes は 15 以上 720 以下（EMR Serverless API の範囲）"
  }
}
