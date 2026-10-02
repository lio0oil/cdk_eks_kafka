output "vpc_id" {
  value = module.vpc.vpc_id
}

output "vpc_cidr_block" {
  value = module.vpc.vpc_cidr_block
}

output "private_subnet_ids" {
  description = "AZ 順の private subnet ID。先頭が kafka_single_az=true のときに使う 1 AZ 目"
  value       = module.vpc.private_subnets
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
