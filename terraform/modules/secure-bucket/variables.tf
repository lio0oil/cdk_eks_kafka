variable "bucket_name" {
  type        = string
  description = "バケット名。null なら bucket_prefix から自動採番する"
  default     = null
}

variable "bucket_prefix" {
  type        = string
  description = "bucket_name が null のときの接頭辞（末尾にランダムな文字列が付く）"
  default     = null
}

variable "force_destroy" {
  type        = bool
  description = "中身ごと削除可能にするか。環境を破棄する dev のみ true"
}

variable "versioning_enabled" {
  type        = bool
  description = "バージョニングを有効にするか"
  default     = false
}

variable "kms_key_arn" {
  type        = string
  description = "SSE-KMS の KMS キー ARN。null なら SSE-S3（AES256）"
  default     = null
}

variable "additional_statements" {
  type        = any
  description = "TLS 強制に加えてバケットポリシーへ足す Statement。バケット自身の ARN が要る場合は Resource の代わりに resource_suffix（\"\" ならバケット、\"/*\" ならオブジェクト）を指定する"
  default     = []
}
