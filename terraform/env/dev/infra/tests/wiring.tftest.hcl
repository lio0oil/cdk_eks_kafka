# module 間の配線（kafka-config -> network など）と、他の層（k8s / app）へ渡す出力を、実際の
# kafka-cluster.yaml と tfvars で検証する。個々の module のテストでは見えない「for_each のキーが
# apply 時まで確定しない」類の問題も拾う。
# command = apply は mock provider に対する適用で、実 AWS には触れない。

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

  mock_data "aws_partition" {
    defaults = {
      partition = "aws"
    }
  }

  mock_data "aws_ssm_parameter" {
    defaults = {
      value = "ami-0123456789abcdef0"
    }
  }

  # provider は ARN 引数の形式を検証するため、mock が生成する乱数文字列では通らない
  mock_resource "aws_iam_role" {
    defaults = {
      arn = "arn:aws:iam::111111111111:role/mock"
    }
  }

  mock_resource "aws_lb" {
    defaults = {
      arn      = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:loadbalancer/net/kafka-shared-nlb/0123456789abcdef"
      dns_name = "kafka-shared-nlb-0123456789abcdef.elb.ap-northeast-1.amazonaws.com"
      zone_id  = "Z31GFT0UA1I2HV"
    }
  }

  mock_resource "aws_s3_bucket" {
    defaults = {
      arn = "arn:aws:s3:::mock-bucket"
    }
  }

  mock_resource "aws_lb_target_group" {
    defaults = {
      arn = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/mock/0123456789abcdef"
    }
  }

  mock_resource "aws_eks_cluster" {
    defaults = {
      endpoint              = "https://0123456789ABCDEF.gr7.ap-northeast-1.eks.amazonaws.com"
      certificate_authority = [{ data = "bW9jaw==" }]
    }
  }
}

run "dev_tfvars_flow_into_every_module" {
  command = apply

  assert {
    condition     = output.cluster_name == var.cluster_name
    error_message = "tfvars の cluster_name が eks module に渡っていない"
  }

  assert {
    condition     = toset(keys(output.kafka_target_group_arns)) == toset(["Bootstrap", "Broker0", "Broker1", "Broker2"])
    error_message = "kafka-cluster.yaml と broker_count から導出した port が network module の TargetGroup に渡っていない"
  }
}

# k8s 層は broker 数・controller 数を infra の出力から受け取る（2 つの tfvars に重ねて持たない）
run "values_shared_with_the_other_layers_are_exposed" {
  command = apply

  assert {
    condition = (
      output.broker_count == var.broker_count
      && output.kafka_controller_count == var.kafka_controller_count
      && output.kafka_external_listener_port == 9094
      && output.kafka_private_dns_name == "kafka.local"
    )
    error_message = "k8s / app 層が使う broker 数 / controller 数 / port / DNS 名が出力されていない"
  }

  assert {
    condition = (
      output.cluster_endpoint != null
      && output.cluster_certificate_authority_data != null
      && output.vpc_id != null
      && output.kafka_nlb_security_group_id != null
      && output.esm_security_group_id != null
      && output.data_bucket_arn != null
      && output.kafka_consumer_artifact_bucket_name != null
    )
    error_message = "k8s / app 層がクラスターへ接続し、リソースを参照するための出力が揃っていない"
  }
}

# kafka_single_az=true では nodegroup・NLB・ESM を 1 AZ 目に固定し、false では全 AZ に置く
run "kafka_subnets_follow_kafka_single_az" {
  command = apply

  assert {
    condition     = length(output.kafka_subnet_ids) == (var.kafka_single_az ? 1 : length(var.private_subnet_cidrs))
    error_message = "kafka_single_az に応じて Kafka 系の subnet が 1 AZ / 全 AZ にならない"
  }
}

run "broker_count_flows_into_the_nlb" {
  command = apply

  variables {
    broker_count = 5
  }

  assert {
    condition     = length(output.kafka_target_group_arns) == 6 && output.broker_count == 5
    error_message = "broker_count の増加が NLB の TargetGroup 数と出力に反映されていない"
  }
}

run "multi_az_settings_are_accepted" {
  command = apply

  variables {
    kafka_single_az            = false
    nat_gateways               = 3
    enable_interface_endpoints = true
    enable_vpc_flow_logs       = true
    log_force_destroy          = false
    deletion_protection        = true
    enable_control_plane_logs  = true
  }

  assert {
    condition     = length(output.kafka_subnet_ids) == 3
    error_message = "kafka_single_az=false で Kafka 系の subnet が全 AZ にならない"
  }
}
