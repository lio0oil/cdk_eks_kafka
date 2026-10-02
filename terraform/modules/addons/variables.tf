variable "cluster_name" {
  type        = string
  description = "EKS クラスター名"
}

variable "region" {
  type        = string
  description = "AWS LBC に渡すリージョン"
}

variable "vpc_id" {
  type        = string
  description = "AWS LBC に渡す VPC ID。指定しないと IMDS から取得しようとする"
}

variable "strimzi_version" {
  type        = string
  description = "Strimzi Operator の Helm chart バージョン"
}

variable "strimzi_chart_repo" {
  type        = string
  description = "Strimzi Operator の Helm chart リポジトリ。閉域網では内部ミラーを指定する"
}

variable "aws_lbc_chart_version" {
  type        = string
  description = "AWS Load Balancer Controller の Helm chart バージョン"
}

variable "aws_lbc_chart_repo" {
  type        = string
  description = "AWS Load Balancer Controller の Helm chart リポジトリ"
}

variable "addons_manifest_dir" {
  type        = string
  description = "gp3-storageclass.yaml と aws-lbc-iam-policy.json を含むディレクトリ"
}

variable "kafka_manifest_dir" {
  type        = string
  description = "kafka Namespace の namespace.yaml を含むディレクトリ"
}

variable "strimzi_replicas" {
  type        = number
  description = "Strimzi Operator の replica 数。2 以上なら leader election で冗長化される"
}

variable "strimzi_log_level" {
  type        = string
  description = "Strimzi Operator のログレベル"
}

variable "aws_lbc_replica_count" {
  type        = number
  description = "AWS Load Balancer Controller の replica 数"
}

variable "aws_lbc_wait_timeout_seconds" {
  type        = number
  description = "AWS LBC の Pod が Ready になるまで待つ上限（秒）。TargetGroupBinding の apply は webhook の起動が前提"
}
