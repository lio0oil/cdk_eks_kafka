# infra の出力（remote state）と tfvars が consumer_app に渡ることを mock で検証する。
# command = apply は mock provider に対する適用で、実 AWS には触れない。

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
}

override_data {
  target = data.terraform_remote_state.infra
  values = {
    outputs = {
      cluster_name                        = "eks-cluster-dev"
      kafka_subnet_ids                    = ["subnet-aaaa"]
      kafka_private_dns_name              = "kafka.local"
      kafka_external_listener_port        = 9094
      esm_security_group_id               = "sg-0fedcba9876543210"
      data_bucket_arn                     = "arn:aws:s3:::mock-data-bucket"
      kafka_consumer_artifact_bucket_name = "mock-artifact-bucket"
    }
  }
}

override_data {
  target = data.terraform_remote_state.k8s_workloads
  values = {
    outputs = {
      test_topic_name = "test-topic"
    }
  }
}

variables {
  kafka_consumer_code_object_version = "3HL4kqtJlcpXroDTDmJ.rmSpXd3dIbrHY"
}

run "infra_outputs_and_tfvars_flow_into_the_consumer" {
  command = apply

  assert {
    condition     = output.function_name == "kafka-lambda-consumer-eks-cluster-dev"
    error_message = "infra の cluster_name が consumer_app に渡っていない"
  }

  assert {
    condition     = output.dlq_bucket_name != null
    error_message = "DLQ バケット名が出力されていない"
  }
}
