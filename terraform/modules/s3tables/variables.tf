variable "table_bucket_name" {
  type        = string
  description = "S3 Tables の table bucket 名（アカウント内で一意、3-63 文字の小文字・数字・ハイフン）"
}

variable "checkpoint_bucket_suffix" {
  type        = string
  description = "consumer の checkpointLocation 用バケット名の接尾辞。実名は kafka-consumer-checkpoint-<account>-<suffix>（アカウントと環境でグローバル衝突を避ける）"
}

variable "force_destroy" {
  type        = bool
  description = "table bucket と checkpoint バケットを中身ごと削除可能にするか。環境を破棄する dev のみ true、stg/prd は false（データ保護）"
}

variable "kms_key_alias" {
  type        = string
  description = "KMS キーのエイリアス（alias/ から始まる）。アカウント + リージョン内で一意が必要なため、同じアカウントの複数環境では環境ごとに変える"
}
