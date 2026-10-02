# infra の出力（remote state）と tfvars が各 module に渡ることを mock で検証する。
# command = apply は mock provider に対する適用で、実 AWS・実クラスターには触れない。

mock_provider "aws" {
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

  # provider は ARN 引数の形式を検証するため、mock が生成する乱数文字列では通らない
  mock_resource "aws_iam_role" {
    defaults = {
      arn = "arn:aws:iam::111111111111:role/mock"
    }
  }

  mock_resource "aws_s3_bucket" {
    defaults = {
      arn = "arn:aws:s3:::mock-dlq-bucket"
    }
  }

  mock_resource "aws_sns_topic" {
    defaults = {
      arn = "arn:aws:sns:ap-northeast-1:111111111111:eks-cluster-dev-alertmanager"
    }
  }

  mock_resource "aws_cloudwatch_log_group" {
    defaults = {
      arn = "arn:aws:logs:ap-northeast-1:111111111111:log-group:/aws/eks/eks-cluster-dev/application"
    }
  }

  mock_resource "aws_lambda_function" {
    defaults = {
      arn = "arn:aws:lambda:ap-northeast-1:111111111111:function:mock"
    }
  }
}

mock_provider "archive" {}
mock_provider "helm" {}
mock_provider "kubernetes" {}

override_data {
  target = data.terraform_remote_state.infra
  values = {
    outputs = {
      cluster_name                        = "eks-cluster-dev"
      cluster_endpoint                    = "https://0123456789ABCDEF.gr7.ap-northeast-1.eks.amazonaws.com"
      cluster_certificate_authority_data  = "bW9jaw=="
      vpc_id                              = "vpc-0123456789abcdef0"
      kafka_subnet_ids                    = ["subnet-aaaa"]
      kafka_private_dns_name              = "kafka.local"
      broker_count                        = 3
      kafka_controller_count              = 3
      kafka_nlb_security_group_id         = "sg-0123456789abcdef0"
      esm_security_group_id               = "sg-0fedcba9876543210"
      data_bucket_arn                     = "arn:aws:s3:::mock-data-bucket"
      kafka_consumer_artifact_bucket_name = "mock-artifact-bucket"
      kafka_target_group_arns = {
        Bootstrap = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-bootstrap/1"
        Broker0   = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-broker0/2"
        Broker1   = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-broker1/3"
        Broker2   = "arn:aws:elasticloadbalancing:ap-northeast-1:111111111111:targetgroup/kafka-broker2/4"
      }
    }
  }
}

# infra の出力（クラスター名・VPC）と tfvars が各 module に渡り、全リソースが apply できることを確認する。
# module はいずれも出力を持たないため、apply の成功（for_each のキーが確定すること、各 provider の組み合わせ）
# そのものが検証になる。
run "infra_outputs_and_tfvars_flow_into_every_module" {
  command = apply
}
