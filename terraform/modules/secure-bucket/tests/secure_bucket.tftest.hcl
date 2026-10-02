mock_provider "aws" {
  mock_resource "aws_s3_bucket" {
    defaults = {
      arn = "arn:aws:s3:::mock-bucket"
    }
  }
}

variables {
  bucket_name   = "mock-bucket"
  force_destroy = true
}

run "bucket_blocks_public_access" {
  command = apply

  assert {
    condition = (
      aws_s3_bucket_public_access_block.this.block_public_acls
      && aws_s3_bucket_public_access_block.this.block_public_policy
      && aws_s3_bucket_public_access_block.this.ignore_public_acls
      && aws_s3_bucket_public_access_block.this.restrict_public_buckets
    )
    error_message = "public access が全遮断になっていない"
  }
}

run "bucket_is_encrypted_with_sse_s3_by_default" {
  command = apply

  assert {
    condition = one([
      for r in aws_s3_bucket_server_side_encryption_configuration.this.rule :
      r.apply_server_side_encryption_by_default[0].sse_algorithm
    ]) == "AES256"
    error_message = "既定で SSE-S3（AES256）になっていない"
  }
}

run "bucket_can_use_a_kms_key" {
  command = apply

  variables {
    kms_key_arn = "arn:aws:kms:ap-northeast-1:111111111111:key/00000000-0000-0000-0000-000000000000"
  }

  assert {
    condition = (
      one([for r in aws_s3_bucket_server_side_encryption_configuration.this.rule : r.apply_server_side_encryption_by_default[0].sse_algorithm]) == "aws:kms"
      && one([for r in aws_s3_bucket_server_side_encryption_configuration.this.rule : r.apply_server_side_encryption_by_default[0].kms_master_key_id]) == "arn:aws:kms:ap-northeast-1:111111111111:key/00000000-0000-0000-0000-000000000000"
    )
    error_message = "kms_key_arn が SSE-KMS として設定されていない"
  }
}

run "policy_denies_non_tls_access" {
  command = apply

  assert {
    condition = (
      jsondecode(output.policy_json).Statement[0].Effect == "Deny"
      && jsondecode(output.policy_json).Statement[0].Condition.Bool["aws:SecureTransport"] == "false"
      && toset(jsondecode(output.policy_json).Statement[0].Resource) == toset(["arn:aws:s3:::mock-bucket", "arn:aws:s3:::mock-bucket/*"])
    )
    error_message = "バケットとオブジェクトへの TLS 以外のアクセスを拒否していない"
  }
}

run "versioning_is_off_unless_requested" {
  command = apply

  assert {
    condition     = length(aws_s3_bucket_versioning.this) == 0
    error_message = "既定でバージョニングが有効になっている"
  }
}

run "versioning_can_be_enabled" {
  command = apply

  variables {
    versioning_enabled = true
  }

  assert {
    condition     = one(aws_s3_bucket_versioning.this[0].versioning_configuration).status == "Enabled"
    error_message = "versioning_enabled=true でバージョニングが有効にならない"
  }
}

run "force_destroy_and_name_are_passed_through" {
  command = apply

  assert {
    condition     = aws_s3_bucket.this.bucket == "mock-bucket" && aws_s3_bucket.this.force_destroy
    error_message = "bucket_name / force_destroy が反映されていない"
  }
}

run "prefix_is_used_when_no_name_is_given" {
  command = apply

  variables {
    bucket_name   = null
    bucket_prefix = "eks-cluster-dev-flow-log-"
  }

  assert {
    condition     = aws_s3_bucket.this.bucket_prefix == "eks-cluster-dev-flow-log-"
    error_message = "bucket_name が null のとき bucket_prefix が使われていない"
  }
}

run "additional_statements_are_appended_with_the_bucket_arn" {
  command = apply

  variables {
    additional_statements = [
      {
        Sid             = "Delivery"
        Effect          = "Allow"
        Principal       = { Service = "delivery.logs.amazonaws.com" }
        Action          = "s3:PutObject"
        resource_suffix = "/AWSLogs/*"
      },
      {
        Sid             = "AclCheck"
        Effect          = "Allow"
        Principal       = { Service = "delivery.logs.amazonaws.com" }
        Action          = "s3:GetBucketAcl"
        resource_suffix = ""
      },
    ]
  }

  assert {
    condition = (
      length(jsondecode(output.policy_json).Statement) == 3
      && jsondecode(output.policy_json).Statement[1].Resource == "arn:aws:s3:::mock-bucket/AWSLogs/*"
      && jsondecode(output.policy_json).Statement[2].Resource == "arn:aws:s3:::mock-bucket"
    )
    error_message = "追加 Statement の Resource にバケット ARN と resource_suffix が展開されていない"
  }

  # resource_suffix は入力用の独自キーで、ポリシーには出さない
  assert {
    condition     = !contains(keys(jsondecode(output.policy_json).Statement[1]), "resource_suffix")
    error_message = "resource_suffix がポリシーに漏れている"
  }
}

run "outputs_expose_the_bucket" {
  command = apply

  assert {
    condition     = output.arn == aws_s3_bucket.this.arn && output.bucket == aws_s3_bucket.this.bucket
    error_message = "arn / bucket 出力がバケットを指していない"
  }
}
