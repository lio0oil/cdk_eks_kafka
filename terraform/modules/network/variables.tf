variable "cluster_name" {
  type        = string
  description = "リソース名の接頭辞に使うクラスター名"
}

variable "vpc_id" {
  type        = string
  description = "既存の VPC の ID（Terraform の管理外）"
}

variable "private_subnet_ids" {
  type        = list(string)
  description = "EKS のコントロールプレーン用の既存の private subnet の ID（Terraform の管理外）。2 つ以上の AZ にまたがること。ノードを置く subnet は node_subnet_id で、この中から 1 つ選ぶ"
}

variable "node_subnet_id" {
  type        = string
  description = "Kafka・system のノード、NLB、ESM を置く subnet。private_subnet_ids（EKS のコントロールプレーン用）の中から 1 つ選ぶ。1 AZ に揃えるため cross-zone は無効にする"

  validation {
    condition     = contains(var.private_subnet_ids, var.node_subnet_id)
    error_message = "node_subnet_id は private_subnet_ids のいずれかを指定してください。"
  }
}

variable "enable_interface_endpoints" {
  type        = bool
  description = "ECR / CloudWatch Logs / eks-auth の Interface VPC Endpoint を作るか"
}

variable "interface_endpoint_services" {
  type        = list(string)
  description = "enable_interface_endpoints=true のとき作る Interface VPC Endpoint のサービス名（com.amazonaws.<region>. の後ろ）。機微データやクレデンシャルを運ぶ API に限る"
}

variable "enable_vpc_flow_logs" {
  type        = bool
  description = "VPC Flow Logs を S3 に保存するか"
}

variable "log_force_destroy" {
  type        = bool
  description = "Flow Logs バケットを中身ごと削除可能にするか。環境を破棄する dev のみ true"
}

variable "nlb_ports" {
  type = list(object({
    name          = string
    listener_port = number
    node_port     = number
  }))
  description = "NLB の Listener / TargetGroup 1 件ごとの定義（kafka-config module の出力）"
}

variable "kafka_target_port" {
  type        = number
  description = "TargetGroup の接続先 port。external listener の内部 port（全 broker 共通）"
}

variable "private_dns_name" {
  type        = string
  description = "Kafka の advertisedHost に使う Private Hosted Zone 名"
}

variable "nlb_name" {
  type        = string
  description = "Kafka 共有 NLB の名前。TargetGroup 名もこの名前から導出する（リージョン内で一意が必要なため、同じアカウントの複数環境では環境ごとに変える）。再作成すると ARN が変わり PrivateLink が壊れるため、変更は慎重に行う"
}

variable "target_group_health_check" {
  type = object({
    interval            = number
    healthy_threshold   = number
    unhealthy_threshold = number
  })
  description = "TargetGroup の TCP health check（間隔の秒数と、healthy / unhealthy と判定する連続回数）"
}
