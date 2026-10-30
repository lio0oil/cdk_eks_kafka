# tfvars の値で merge_infra が組み上がり、層の出力として公開されることを mock で検証する。
# command = apply は mock provider に対する適用で、実 AWS には触れない。
# module 内のリソースの属性は root のテストから参照できないため、入力の値そのもの（保持日数など）は
# module 側のテストで検査し、ここでは tfvars の全項目が型を満たして module に渡ることと、出力の配線を見る。

mock_provider "aws" {
  mock_resource "aws_emrserverless_application" {
    defaults = {
      arn = "arn:aws:emr-serverless:ap-northeast-1:111111111111:/applications/00mockapplication"
    }
  }
}

# mock の既定値は全インスタンスで同じ ARN になり、バケットの取り違えを検出できない。
# override_module は terraform 1.16.4 でクラッシュするため、module 内のリソースを個別に上書きする。
override_resource {
  target = module.merge_infra.aws_s3_bucket.this["staging"]
  values = {
    arn = "arn:aws:s3:::mock-staging"
  }
}

override_resource {
  target = module.merge_infra.aws_s3_bucket.this["artifact"]
  values = {
    arn = "arn:aws:s3:::mock-artifact"
  }
}

run "tfvars_are_accepted_by_the_module" {
  command = apply

  assert {
    condition     = output.emr_application_id != null && output.staging_bucket_name != null
    error_message = "tfvars の値で merge_infra が組み上がらない"
  }
}

run "outputs_expose_the_module_outputs" {
  command = apply

  assert {
    condition = (
      output.staging_bucket_name == module.merge_infra.staging_bucket_name
      && output.staging_bucket_arn == "arn:aws:s3:::mock-staging"
      && output.artifact_bucket_name == module.merge_infra.artifact_bucket_name
      && output.artifact_bucket_arn == "arn:aws:s3:::mock-artifact"
      && output.emr_application_id == module.merge_infra.emr_application_id
      && output.emr_application_arn == "arn:aws:emr-serverless:ap-northeast-1:111111111111:/applications/00mockapplication"
    )
    error_message = "層の出力が merge_infra の出力を指していない"
  }
}
