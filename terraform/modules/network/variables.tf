variable "cluster_name" {
  type        = string
  description = "リソース名の接頭辞に使うクラスター名"
}

variable "vpc_cidr" {
  type        = string
  description = "VPC の CIDR"
}

variable "public_subnet_cidrs" {
  type        = list(string)
  description = "public subnet の CIDR。AZ ごとに 1 つ"

  validation {
    condition     = length(var.public_subnet_cidrs) == length(var.private_subnet_cidrs)
    error_message = "public_subnet_cidrs と private_subnet_cidrs は AZ ごとに 1 つずつ、同じ数を指定してください。"
  }
}

variable "private_subnet_cidrs" {
  type        = list(string)
  description = "private subnet の CIDR。AZ ごとに 1 つ。この数が使う AZ の数になる"
}

variable "nat_gateways" {
  type        = number
  description = "NAT Gateway の台数。1 なら 1 台を共有し、AZ 数と同じなら AZ ごとに配置する"

  validation {
    condition     = var.nat_gateways == 1 || var.nat_gateways == length(var.private_subnet_cidrs)
    error_message = "nat_gateways は 1（共有）か AZ 数（private_subnet_cidrs の数、AZ ごと）のいずれかを指定してください。"
  }
}

variable "kafka_single_az" {
  type        = bool
  description = "Kafka NLB を 1 AZ 目の private subnet だけに置くか。true の場合 cross-zone は無効にする"
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
