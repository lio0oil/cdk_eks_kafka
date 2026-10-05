locals {
  # PrivateLink 消費側が同名の Private Hosted Zone を作れば、advertisedHost の再接続先も同じ文字列で解決できる
  kafka_private_dns_name = "kafka.local"
}

module "kafka_config" {
  source = "../../../modules/kafka-config"

  manifest_dir    = "${path.module}/../../../manifests/kafka"
  broker_count    = var.broker_count
  advertised_host = local.kafka_private_dns_name
}

module "admin_role" {
  source = "../../../modules/admin-role"

  role_name = var.admin_role_name
}

module "network" {
  source = "../../../modules/network"

  cluster_name                = var.cluster_name
  vpc_id                      = var.vpc_id
  private_subnet_ids          = var.private_subnet_ids
  node_subnet_id              = var.node_subnet_id
  enable_interface_endpoints  = var.enable_interface_endpoints
  interface_endpoint_services = var.interface_endpoint_services
  enable_vpc_flow_logs        = var.enable_vpc_flow_logs
  log_force_destroy           = var.log_force_destroy
  nlb_ports                   = module.kafka_config.nlb_ports
  kafka_target_port           = module.kafka_config.external_listener_port
  private_dns_name            = local.kafka_private_dns_name
  nlb_name                    = var.kafka_nlb_name
  target_group_health_check   = var.kafka_target_group_health_check
}

module "eks" {
  source = "../../../modules/eks"

  cluster_name       = var.cluster_name
  kubernetes_version = var.kubernetes_version
  subnet_ids         = module.network.private_subnet_ids

  # Kafka・system の nodegroup は、コントロールプレーン用の subnet の中から選んだ 1 つ（NLB・ESM と同じ）に置く。
  # コントロールプレーンの ENI には複数 AZ の subnet が必須なため、subnet_ids は全 AZ のまま渡す。
  nodegroup_subnet_ids = [var.node_subnet_id]

  admin_role_arn            = module.admin_role.role_arn
  console_role_arns         = var.console_role_arns
  deletion_protection       = var.deletion_protection
  enable_control_plane_logs = var.enable_control_plane_logs
  addon_versions            = var.addon_versions
  nodegroup_ami_type        = var.nodegroup_ami_type

  endpoint_public_access                      = var.eks_endpoint_public_access
  endpoint_private_access                     = var.eks_endpoint_private_access
  bootstrap_cluster_creator_admin_permissions = var.eks_bootstrap_cluster_creator_admin_permissions
  control_plane_log_types                     = var.eks_control_plane_log_types
  upgrade_support_type                        = var.eks_upgrade_support_type
  nodegroup_capacity_type                     = var.nodegroup_capacity_type

  system_node_group = var.system_node_group
  kafka_broker_node_group = {
    instance_type = var.kafka_broker_instance_type
    count         = var.broker_count
  }
  kafka_controller_node_group = {
    instance_type = var.kafka_controller_instance_type
    count         = var.kafka_controller_count
  }
}

# self-managed Kafka の ESM のポーラー用 SG と、Lambda コンシューマのデータ用・アーティファクト用バケット。
# Lambda 本体と ESM はアプリ層（app）が管理する。
module "consumer_infra" {
  source = "../../../modules/consumer-infra"

  cluster_name                = var.cluster_name
  vpc_id                      = module.network.vpc_id
  kafka_nlb_security_group_id = module.network.kafka_nlb_security_group_id
  force_destroy               = var.log_force_destroy
}

# Session Manager で接続して EKS / Kafka NLB を検証する踏み台。stg/prd には残さない
# （恒久の踏み台が必要になった時点で別途検討する）。
module "test_ec2" {
  source = "../../../modules/test-ec2"

  vpc_id             = module.network.vpc_id
  subnet_id          = module.network.node_subnet_id
  kubernetes_version = var.kubernetes_version
  instance_type      = var.test_ec2_instance_type
}
