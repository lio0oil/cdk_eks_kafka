variable "region" {
  type        = string
  description = "デプロイ先の AWS リージョン"
}

variable "environment" {
  type        = string
  description = "環境名（dev / stg / prd）。default_tags に付与する"
}

variable "name_prefix" {
  type        = string
  description = "リソース名の接頭辞。バケット名と EMR Serverless アプリ名に使う。同じアカウントの複数環境では環境ごとに変える"
}

variable "force_destroy" {
  type        = bool
  description = "バケットを中身ごと削除可能にするか。環境を破棄する dev のみ true、stg/prd は false（データ保護）"
}

variable "staging_retention_days" {
  type        = number
  description = "staging の incoming/ と dlq/ を削除するまでの日数"
}

variable "emr_release_label" {
  type        = string
  description = "EMR Serverless のリリースラベル。merger の PySpark / Iceberg のバージョンと揃える"
}

variable "emr_queue_timeout_minutes" {
  type        = number
  description = "実行中のジョブがあるとき、後続のジョブが待機キューに留まる時間（分）"
}
