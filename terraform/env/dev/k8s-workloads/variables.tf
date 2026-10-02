variable "region" {
  type        = string
  description = "infra と同じ AWS リージョン。クラスターの認証トークン（aws eks get-token）の取得に使う"
}

variable "kafka_delete_claim" {
  type        = bool
  description = "KafkaNodePool 削除時に PVC を一緒に削除するか。dev は true、stg/prd は false（データ保護）"
}

variable "kafka_broker_resources" {
  type = object({
    memory_request = string
    memory_limit   = string
    cpu_request    = string
    cpu_limit      = string
    storage_size   = string
    jvm_xms        = string
    jvm_xmx        = string
  })
  description = "broker の resources / storage / jvmOptions"
}

variable "kafka_controller_resources" {
  type = object({
    memory_request = string
    memory_limit   = string
    cpu_request    = string
    cpu_limit      = string
    storage_size   = string
    jvm_xms        = string
    jvm_xmx        = string
  })
  description = "controller の resources / storage / jvmOptions"
}

variable "kafka_topic_retention_ms" {
  type        = string
  description = "test-topic の retention.ms（ミリ秒の文字列）"
}
