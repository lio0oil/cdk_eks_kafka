variable "region" {
  type        = string
  description = "デプロイ先の AWS リージョン"
}

variable "environment" {
  type        = string
  description = "環境名（dev / stg / prd）。default_tags に付与する"
}

variable "table_bucket_name" {
  type        = string
  description = "S3 Tables の table bucket 名"
}

variable "checkpoint_bucket_suffix" {
  type        = string
  description = "consumer の checkpointLocation 用バケット名の接尾辞"
}

variable "force_destroy" {
  type        = bool
  description = "バケットを中身ごと削除可能にするか。環境を破棄する dev のみ true"
}

variable "kms_key_alias" {
  type        = string
  description = "KMS キーのエイリアス（alias/ から始まる）。アカウント + リージョン内で一意が必要なため、同じアカウントの複数環境では環境ごとに変える"
}
