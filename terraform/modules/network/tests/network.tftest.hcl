# command = apply は mock provider に対する適用で、実 AWS には触れない。
# VPC ID など computed 値の受け渡し（参照の配線）を検証するために apply を使う。

mock_provider "aws" {
  mock_data "aws_availability_zones" {
    defaults = {
      names = ["ap-northeast-1a", "ap-northeast-1c", "ap-northeast-1d"]
    }
  }

  mock_data "aws_region" {
    defaults = {
      region = "ap-northeast-1"
    }
  }

  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "111111111111"
    }
  }

  # provider は ARN 引数の形式を検証するため、mock が生成する乱数文字列では通らない
  mock_resource "aws_lb" {
    defaults = {
      arn      = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:loadbalancer/net/kafka-shared-nlb/0123456789abcdef"
      dns_name = "kafka-shared-nlb-0123456789abcdef.elb.ap-northeast-1.amazonaws.com"
      zone_id  = "Z31GFT0UA1I2HV"
    }
  }

  mock_resource "aws_s3_bucket" {
    defaults = {
      arn = "arn:aws:s3:::eks-cluster-dev-vpc-flow-log-mock"
    }
  }
}

# TargetGroup は Listener との対応（キーごとの参照の配線）を検証するため、
# インスタンスごとに異なる ARN を与える。
override_resource {
  target = aws_lb_target_group.kafka["Bootstrap"]
  values = {
    arn = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-bootstrap/0000000000000001"
  }
}

override_resource {
  target = aws_lb_target_group.kafka["Broker0"]
  values = {
    arn = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-broker0/0000000000000002"
  }
}

override_resource {
  target = aws_lb_target_group.kafka["Broker1"]
  values = {
    arn = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-broker1/0000000000000003"
  }
}

override_resource {
  target = aws_lb_target_group.kafka["Broker2"]
  values = {
    arn = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-broker2/0000000000000004"
  }
}

override_resource {
  target = aws_lb_target_group.kafka["Broker3"]
  values = {
    arn = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-broker3/0000000000000005"
  }
}

variables {
  cluster_name                = "eks-cluster-dev"
  nat_gateways                = 1
  kafka_single_az             = true
  enable_interface_endpoints  = false
  enable_vpc_flow_logs        = false
  log_force_destroy           = true
  kafka_target_port           = 9094
  private_dns_name            = "kafka.local"
  vpc_cidr                    = "10.0.0.0/16"
  public_subnet_cidrs         = ["10.0.0.0/24", "10.0.1.0/24", "10.0.2.0/24"]
  private_subnet_cidrs        = ["10.0.3.0/24", "10.0.4.0/24", "10.0.5.0/24"]
  interface_endpoint_services = ["ecr.api", "ecr.dkr", "logs", "eks-auth"]
  nlb_name                    = "kafka-shared-nlb"
  target_group_health_check = {
    interval            = 10
    healthy_threshold   = 2
    unhealthy_threshold = 2
  }
  nlb_ports = [
    { name = "Bootstrap", listener_port = 9094, node_port = 30094 },
    { name = "Broker0", listener_port = 9095, node_port = 30095 },
    { name = "Broker1", listener_port = 9096, node_port = 30096 },
    { name = "Broker2", listener_port = 9097, node_port = 30097 },
  ]
}

run "vpc_layout_matches_legacy" {
  command = apply

  assert {
    condition     = module.vpc.vpc_cidr_block == "10.0.0.0/16"
    error_message = "VPC の CIDR が 10.0.0.0/16 ではない"
  }

  assert {
    condition     = module.vpc.public_subnets_cidr_blocks == tolist(["10.0.0.0/24", "10.0.1.0/24", "10.0.2.0/24"])
    error_message = "public subnet が /24 で 3 AZ に分割されていない"
  }

  assert {
    condition     = module.vpc.private_subnets_cidr_blocks == tolist(["10.0.3.0/24", "10.0.4.0/24", "10.0.5.0/24"])
    error_message = "private subnet が /24 で 3 AZ に分割されていない"
  }

  assert {
    condition     = alltrue([for s in module.vpc.public_subnet_objects : s.tags["kubernetes.io/role/elb"] == "1"])
    error_message = "public subnet に kubernetes.io/role/elb タグが付いていない"
  }

  assert {
    condition     = alltrue([for s in module.vpc.private_subnet_objects : s.tags["kubernetes.io/role/internal-elb"] == "1"])
    error_message = "private subnet に kubernetes.io/role/internal-elb タグが付いていない"
  }
}

run "single_nat_gateway_by_default" {
  command = apply

  assert {
    condition     = length(module.vpc.natgw_ids) == 1
    error_message = "nat_gateways=1 で NAT Gateway が 1 台にならない"
  }
}

run "nat_gateway_per_az" {
  command = apply

  variables {
    nat_gateways = 3
  }

  assert {
    condition     = length(module.vpc.natgw_ids) == 3
    error_message = "nat_gateways=3 で AZ ごとに NAT Gateway が作られない"
  }
}

run "nat_gateways_other_than_1_or_az_count_is_rejected" {
  command = plan

  variables {
    nat_gateways = 2
  }

  expect_failures = [var.nat_gateways]
}

run "s3_gateway_endpoint_is_attached_to_all_route_tables" {
  command = apply

  assert {
    condition     = aws_vpc_endpoint.s3.vpc_endpoint_type == "Gateway" && aws_vpc_endpoint.s3.service_name == "com.amazonaws.ap-northeast-1.s3"
    error_message = "S3 Gateway Endpoint ではない"
  }

  assert {
    condition = aws_vpc_endpoint.s3.route_table_ids == toset(concat(
      module.vpc.private_route_table_ids,
      module.vpc.public_route_table_ids,
    ))
    error_message = "S3 Gateway Endpoint が private / public 双方の route table に紐付いていない"
  }
}

run "interface_endpoints_are_disabled_by_default" {
  command = apply

  assert {
    condition     = length(aws_vpc_endpoint.interface) == 0
    error_message = "enable_interface_endpoints=false で Interface Endpoint が作られている"
  }

  assert {
    condition     = length(aws_security_group.interface_endpoint) == 0
    error_message = "enable_interface_endpoints=false で Endpoint 用 SG が作られている"
  }
}

run "interface_endpoints_cover_sensitive_apis" {
  command = apply

  variables {
    enable_interface_endpoints = true
  }

  assert {
    condition     = toset(keys(aws_vpc_endpoint.interface)) == toset(["ecr.api", "ecr.dkr", "logs", "eks-auth"])
    error_message = "ECR(api/dkr) / CloudWatch Logs / eks-auth の 4 つになっていない"
  }

  assert {
    condition = alltrue([
      for k, e in aws_vpc_endpoint.interface :
      e.vpc_endpoint_type == "Interface" && e.private_dns_enabled && e.service_name == "com.amazonaws.ap-northeast-1.${k}"
    ])
    error_message = "Interface Endpoint の type / private DNS / service 名が期待と異なる"
  }

  assert {
    condition     = alltrue([for e in aws_vpc_endpoint.interface : e.subnet_ids == toset(module.vpc.private_subnets)])
    error_message = "Interface Endpoint が全 private subnet に配置されていない"
  }

  assert {
    condition = (
      aws_vpc_security_group_ingress_rule.interface_endpoint_https[0].from_port == 443
      && aws_vpc_security_group_ingress_rule.interface_endpoint_https[0].to_port == 443
      && aws_vpc_security_group_ingress_rule.interface_endpoint_https[0].cidr_ipv4 == "10.0.0.0/16"
    )
    error_message = "Endpoint 用 SG が VPC CIDR からの 443 のみを許可していない"
  }
}

run "flow_logs_are_disabled_by_default" {
  command = apply

  assert {
    condition     = length(aws_flow_log.vpc) == 0 && length(module.flow_log_bucket) == 0
    error_message = "enable_vpc_flow_logs=false で Flow Logs / バケットが作られている"
  }
}

run "flow_logs_go_to_the_hardened_bucket" {
  command = apply

  variables {
    enable_vpc_flow_logs = true
    log_force_destroy    = false
  }

  assert {
    condition = (
      aws_flow_log.vpc[0].traffic_type == "ALL"
      && aws_flow_log.vpc[0].log_destination_type == "s3"
      && aws_flow_log.vpc[0].log_destination == module.flow_log_bucket[0].arn
    )
    error_message = "Flow Logs が ALL トラフィックを専用バケットへ送る設定になっていない"
  }

  # AWS は Flow Logs 作成時にバケットポリシーへ配信許可を自動追記するが、Terraform 管理の
  # ポリシーがそれを次回 apply で消すと配信が止まる。明示的に宣言されていることを固定する。
  assert {
    condition = anytrue([
      for s in jsondecode(module.flow_log_bucket[0].policy_json).Statement :
      s.Effect == "Allow" && try(s.Principal.Service == "delivery.logs.amazonaws.com", false) && s.Action == "s3:PutObject"
    ])
    error_message = "delivery.logs.amazonaws.com への PutObject 許可が宣言されていない"
  }

  assert {
    condition = anytrue([
      for s in jsondecode(module.flow_log_bucket[0].policy_json).Statement :
      s.Effect == "Allow" && try(s.Principal.Service == "delivery.logs.amazonaws.com", false) && s.Action == "s3:GetBucketAcl"
    ])
    error_message = "delivery.logs.amazonaws.com への GetBucketAcl 許可が宣言されていない"
  }
}

run "kafka_nlb_security_group_allows_each_listener_port_from_vpc" {
  command = apply

  assert {
    condition = {
      for k, r in aws_vpc_security_group_ingress_rule.kafka_nlb : k => r.from_port
      } == {
      Bootstrap = 9094
      Broker0   = 9095
      Broker1   = 9096
      Broker2   = 9097
    }
    error_message = "NLB の SG が listener port ごとの ingress を持っていない"
  }

  assert {
    condition = alltrue([
      for r in aws_vpc_security_group_ingress_rule.kafka_nlb :
      r.to_port == r.from_port && r.ip_protocol == "tcp" && r.cidr_ipv4 == "10.0.0.0/16"
    ])
    error_message = "NLB の SG ingress が VPC CIDR からの単一 TCP port になっていない"
  }
}

run "kafka_nlb_is_internal_and_single_az_in_dev" {
  command = apply

  assert {
    condition = (
      aws_lb.kafka.name == "kafka-shared-nlb"
      && aws_lb.kafka.load_balancer_type == "network"
      && aws_lb.kafka.internal
      && !aws_lb.kafka.enable_deletion_protection
    )
    error_message = "NLB が internal な network LB（kafka-shared-nlb）になっていない"
  }

  assert {
    condition     = aws_lb.kafka.security_groups == toset([aws_security_group.kafka_nlb.id])
    error_message = "NLB に専用 SG が紐付いていない"
  }

  assert {
    condition     = aws_lb.kafka.subnets == toset([module.vpc.private_subnets[0]]) && !aws_lb.kafka.enable_cross_zone_load_balancing
    error_message = "kafka_single_az=true で NLB が 1 AZ 目のみ・cross-zone 無効になっていない"
  }
}

run "kafka_nlb_spans_all_azs_when_multi_az" {
  command = apply

  variables {
    kafka_single_az = false
  }

  assert {
    condition     = aws_lb.kafka.subnets == toset(module.vpc.private_subnets) && aws_lb.kafka.enable_cross_zone_load_balancing
    error_message = "kafka_single_az=false で NLB が全 private subnet・cross-zone 有効になっていない"
  }
}

run "kafka_target_groups_use_ip_targets_with_client_ip_preserved" {
  command = apply

  assert {
    condition     = toset(keys(aws_lb_target_group.kafka)) == toset(["Bootstrap", "Broker0", "Broker1", "Broker2"])
    error_message = "TargetGroup が Bootstrap + broker 数ぶん作られていない"
  }

  assert {
    condition = alltrue([
      for tg in values(aws_lb_target_group.kafka) :
      tg.target_type == "ip" && tg.protocol == "TCP" && tg.port == 9094 && tg.preserve_client_ip == "true"
    ])
    error_message = "TargetGroup が ip / TCP / 内部 port 9094 / preserve_client_ip=true になっていない"
  }

  assert {
    condition = alltrue([
      for tg in values(aws_lb_target_group.kafka) :
      tg.health_check[0].protocol == "TCP"
      && tg.health_check[0].port == "9094"
      && tg.health_check[0].healthy_threshold == 2
      && tg.health_check[0].unhealthy_threshold == 2
      && tg.health_check[0].interval == 10
    ])
    error_message = "TargetGroup の health check が TCP:9094 / 閾値 2 / 間隔 10 秒ではない"
  }
}

run "kafka_listeners_forward_to_their_own_target_group" {
  command = apply

  assert {
    condition = {
      for k, l in aws_lb_listener.kafka : k => l.port
      } == {
      Bootstrap = 9094
      Broker0   = 9095
      Broker1   = 9096
      Broker2   = 9097
    }
    error_message = "Listener の port が nlb_ports と一致しない"
  }

  assert {
    condition = alltrue([
      for k, l in aws_lb_listener.kafka :
      l.protocol == "TCP"
      && l.load_balancer_arn == aws_lb.kafka.arn
      && l.default_action[0].type == "forward"
      && l.default_action[0].target_group_arn == aws_lb_target_group.kafka[k].arn
    ])
    error_message = "Listener が同名の TargetGroup へ TCP で forward していない"
  }
}

run "kafka_resources_scale_with_nlb_ports" {
  command = apply

  variables {
    nlb_ports = [
      { name = "Bootstrap", listener_port = 9094, node_port = 30094 },
      { name = "Broker0", listener_port = 9095, node_port = 30095 },
      { name = "Broker1", listener_port = 9096, node_port = 30096 },
      { name = "Broker2", listener_port = 9097, node_port = 30097 },
      { name = "Broker3", listener_port = 9098, node_port = 30098 },
    ]
  }

  assert {
    condition = (
      length(aws_lb_target_group.kafka) == 5
      && length(aws_lb_listener.kafka) == 5
      && length(aws_vpc_security_group_ingress_rule.kafka_nlb) == 5
    )
    error_message = "broker 追加時に TargetGroup / Listener / SG ingress が連動して増えない"
  }
}

run "kafka_private_hosted_zone_resolves_to_nlb" {
  command = apply

  assert {
    condition     = aws_route53_zone.kafka.name == "kafka.local" && one(aws_route53_zone.kafka.vpc).vpc_id == module.vpc.vpc_id
    error_message = "kafka.local の Private Hosted Zone が VPC に紐付いていない"
  }

  assert {
    condition = (
      aws_route53_record.kafka_nlb.name == "kafka.local"
      && aws_route53_record.kafka_nlb.type == "A"
      && one(aws_route53_record.kafka_nlb.alias).name == aws_lb.kafka.dns_name
      && one(aws_route53_record.kafka_nlb.alias).zone_id == aws_lb.kafka.zone_id
    )
    error_message = "kafka.local の A レコードが NLB への alias になっていない"
  }
}

run "kafka_endpoint_service_exposes_nlb_without_acceptance" {
  command = apply

  assert {
    condition = (
      !aws_vpc_endpoint_service.kafka.acceptance_required
      && aws_vpc_endpoint_service.kafka.network_load_balancer_arns == toset([aws_lb.kafka.arn])
      && aws_vpc_endpoint_service.kafka.tags["Name"] == "kafka-endpoint-service"
    )
    error_message = "Endpoint Service が NLB を承認不要で公開する設定になっていない"
  }
}

run "outputs_expose_wiring_for_other_modules" {
  command = apply

  assert {
    condition     = toset(keys(output.kafka_target_group_arns)) == toset(["Bootstrap", "Broker0", "Broker1", "Broker2"])
    error_message = "kafka_target_group_arns が nlb_ports 名で引けない"
  }

  assert {
    condition     = output.kafka_nlb_security_group_id == aws_security_group.kafka_nlb.id
    error_message = "kafka_nlb_security_group_id が NLB の SG を指していない"
  }
}

# CIDR・AZ 数・Endpoint の対象・NLB 名・health check は module に値を持たず、呼び出し側（env）が決める
run "network_settings_come_from_variables" {
  command = apply

  variables {
    vpc_cidr                    = "10.1.0.0/16"
    public_subnet_cidrs         = ["10.1.0.0/24", "10.1.1.0/24"]
    private_subnet_cidrs        = ["10.1.2.0/24", "10.1.3.0/24"]
    enable_interface_endpoints  = true
    interface_endpoint_services = ["logs"]
    nlb_name                    = "custom-kafka-nlb"
    nat_gateways                = 2
    target_group_health_check = {
      interval            = 30
      healthy_threshold   = 3
      unhealthy_threshold = 4
    }
  }

  assert {
    condition     = module.vpc.vpc_cidr_block == "10.1.0.0/16" && length(module.vpc.private_subnets) == 2 && length(module.vpc.public_subnets) == 2
    error_message = "vpc_cidr と subnet の CIDR 数（= AZ 数）が variables から決まっていない"
  }

  assert {
    condition     = length(module.vpc.natgw_ids) == 2
    error_message = "NAT Gateway が AZ ごと（AZ 数と同数）に作られていない"
  }

  assert {
    condition     = toset(keys(aws_vpc_endpoint.interface)) == toset(["logs"])
    error_message = "Interface Endpoint の対象が interface_endpoint_services から決まっていない"
  }

  assert {
    condition     = aws_lb.kafka.name == "custom-kafka-nlb"
    error_message = "NLB 名が nlb_name から決まっていない"
  }

  # TargetGroup 名はリージョン内で一意が必要なため、環境ごとに変わる nlb_name から導出する
  assert {
    condition = (
      aws_lb_target_group.kafka["Bootstrap"].name == "custom-kafka-nlb-bootstrap"
      && aws_lb_target_group.kafka["Broker0"].name == "custom-kafka-nlb-broker0"
    )
    error_message = "TargetGroup 名が nlb_name から導出されていない（同じアカウントの複数環境で衝突する）"
  }

  assert {
    condition = alltrue([
      for tg in values(aws_lb_target_group.kafka) :
      tg.health_check[0].interval == 30 && tg.health_check[0].healthy_threshold == 3 && tg.health_check[0].unhealthy_threshold == 4
    ])
    error_message = "TargetGroup の health check が variables から決まっていない"
  }
}

run "public_and_private_subnets_must_pair_up_per_az" {
  command = plan

  variables {
    public_subnet_cidrs = ["10.0.0.0/24"]
  }

  expect_failures = [var.public_subnet_cidrs]
}
