output "vpc_id" {
  value = var.vpc_id
}

output "vpc_cidr_block" {
  value = data.aws_vpc.this.cidr_block
}

output "node_subnet_id" {
  description = "Kafka・system のノード、NLB、ESM を置く subnet"
  value       = var.node_subnet_id
}

output "private_subnet_ids" {
  description = "EKS のコントロールプレーン用の private subnet ID"
  value       = var.private_subnet_ids

  # 別の VPC の Subnet が混ざると NLB や EKS の作成が失敗する（または意図しない VPC に作られる）
  precondition {
    condition     = alltrue([for s in data.aws_subnet.private : s.vpc_id == var.vpc_id])
    error_message = "private_subnet_ids に、vpc_id の VPC に属さない subnet が含まれています。"
  }

  # EKS のコントロールプレーンは 2 つ以上の AZ の subnet を要求する
  precondition {
    condition     = length(distinct([for s in data.aws_subnet.private : s.availability_zone])) >= 2
    error_message = "private_subnet_ids は 2 つ以上の AZ にまたがる必要があります。"
  }
}

output "kafka_nlb_dns_name" {
  value = aws_lb.kafka.dns_name
}

output "kafka_nlb_security_group_id" {
  description = "TargetGroupBinding の networking.ingress.from に指定し、AWS LBC がノード SG へルールを追加できるようにする"
  value       = aws_security_group.kafka_nlb.id
}

output "kafka_target_group_arns" {
  description = "nlb_ports の name（Bootstrap / Broker<N>）をキーにした TargetGroup ARN"
  value       = { for k, tg in aws_lb_target_group.kafka : k => tg.arn }
}
