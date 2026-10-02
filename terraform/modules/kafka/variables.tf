variable "kafka_manifest_dir" {
  type        = string
  description = "Kafka CR / KafkaNodePool / StorageClass / TargetGroupBinding / topics を含むディレクトリ"
}

variable "monitoring_manifest_dir" {
  type        = string
  description = "JMX メトリクス ConfigMap（kafka-metrics.yaml など）を含むディレクトリ"
}

variable "broker_count" {
  type        = number
  description = "Kafka broker の replica 数"
}

variable "controller_count" {
  type        = number
  description = "KRaft controller の replica 数"
}

variable "delete_claim" {
  type        = bool
  description = "KafkaNodePool 削除時に PVC を一緒に削除するか（Strimzi の deleteClaim）。dev は true、stg/prd は false（データ保護）"
}

variable "broker_resources" {
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

variable "controller_resources" {
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

variable "external_listener_name" {
  type        = string
  description = "kafka-cluster.yaml の external listener 名"
}

variable "broker_configs" {
  type        = any
  description = "external listener の configuration.brokers[] に注入する設定（kafka-config module の出力）"
}

variable "target_group_arns" {
  type        = map(string)
  description = "NLB の TargetGroup ARN。キーは Bootstrap / Broker<N>。Bootstrap + broker_count 件であること"

  validation {
    condition     = length(var.target_group_arns) == var.broker_count + 1
    error_message = "target_group_arns は Bootstrap + broker_count 件である必要があります。platform 側の broker_count と食い違っていないか確認してください。"
  }
}

variable "target_port" {
  type        = number
  description = "TargetGroup の接続先 port。external listener の内部 port（全 broker 共通）"
}

variable "nlb_security_group_id" {
  type        = string
  description = "NLB の SG。AWS LBC がノードの SG へ受け入れルールを追加する際の許可元"
}

variable "vpc_id" {
  type        = string
  description = "TargetGroupBinding に明示する VPC ID"
}

variable "topic_retention_ms" {
  type        = string
  description = "test-topic の retention.ms（ミリ秒の文字列）"
}
