# ── k8s-addons / k8s-workloads / app 層が使う値 ───────────────────────────────────────────────────
# broker 数や controller 数のように、複数の層で食い違うと壊れる値は、tfvars に重ねて持たせず
# この出力を単一の真実の源にする。

output "cluster_name" {
  value = module.eks.cluster_name
}

output "cluster_endpoint" {
  value = module.eks.cluster_endpoint
}

output "cluster_certificate_authority_data" {
  value = module.eks.cluster_certificate_authority_data
}

output "vpc_id" {
  value = module.network.vpc_id
}

output "kafka_subnet_ids" {
  description = "Kafka 系（nodegroup / NLB / ESM）を置く subnet。kafka_single_az=true では 1 AZ 目だけ"
  value       = var.kafka_single_az ? slice(module.network.private_subnet_ids, 0, 1) : module.network.private_subnet_ids
}

output "broker_count" {
  value = var.broker_count
}

output "kafka_controller_count" {
  value = var.kafka_controller_count
}

output "kafka_external_listener_port" {
  value = module.kafka_config.external_listener_port
}

output "kafka_private_dns_name" {
  value = local.kafka_private_dns_name
}

output "kafka_nlb_security_group_id" {
  value = module.network.kafka_nlb_security_group_id
}

output "kafka_target_group_arns" {
  value = module.network.kafka_target_group_arns
}

output "esm_security_group_id" {
  value = module.consumer_infra.esm_security_group_id
}

output "data_bucket_arn" {
  value = module.consumer_infra.data_bucket_arn
}

output "kafka_consumer_artifact_bucket_name" {
  description = "Lambda コンシューマの function.zip のアップロード先"
  value       = module.consumer_infra.artifact_bucket_name
}

# ── 利用者向け ───────────────────────────────────────────────────────────

output "kafka_nlb_dns_name" {
  value = module.network.kafka_nlb_dns_name
}

output "test_ec2_start_session_command" {
  value = module.test_ec2.start_session_command
}

output "test_ec2_transfer_bucket_name" {
  value = module.test_ec2.transfer_bucket_name
}
