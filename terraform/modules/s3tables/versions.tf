terraform {
  required_version = ">= 1.10"

  required_providers {
    aws = {
      source  = "hashicorp/aws"
      version = ">= 6.0"
    }
    # aws provider の aws_s3tables_table は Iceberg の partition spec に対応していないため、
    # テーブルだけ Cloud Control 経由の awscc provider で作る
    awscc = {
      source  = "hashicorp/awscc"
      version = "~> 1.0"
    }
  }
}
