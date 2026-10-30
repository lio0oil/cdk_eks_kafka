# infra の出力（remote state）と変数が merge_app に渡ることを mock で検証する。
# command = apply は mock provider に対する適用で、実 AWS には触れない。
# terraform.tfvars は実環境の値を持ちリポジトリに置かないため、必要な変数はすべてここで与える。

mock_provider "aws" {
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
}

override_data {
  target = data.terraform_remote_state.infra
  values = {
    outputs = {
      staging_bucket_name  = "infra-staging"
      staging_bucket_arn   = "arn:aws:s3:::infra-staging"
      artifact_bucket_name = "infra-artifact"
      artifact_bucket_arn  = "arn:aws:s3:::infra-artifact"
      emr_application_id   = "00infraapplication"
      emr_application_arn  = "arn:aws:emr-serverless:ap-northeast-1:111111111111:/applications/00infraapplication"
    }
  }
}

variables {
  region      = "ap-northeast-1"
  environment = "dev"
  name_prefix = "wiring-dev"

  subnet_ids            = ["subnet-wiring"]
  esm_security_group_id = "sg-wiring"
  bootstrap_dns_name    = "kafka.wiring"
  bootstrap_port        = 9999
  topic_name            = "wiring-topic"

  table_bucket_arn = "arn:aws:s3tables:ap-northeast-1:222222222222:bucket/wiring-table-bucket"
  namespace_name   = "wiring_ns"

  function_object_version = "wiring-version-id"

  lambda_timeout_seconds          = 45
  lambda_memory_mb                = 768
  log_retention_days              = 3
  esm_starting_position           = "TRIM_HORIZON"
  esm_batch_size                  = 77
  esm_max_batching_window_seconds = 4

  schedule_expression     = "rate(7 minutes)"
  merge_margin_seconds    = 200
  spark_submit_parameters = "--conf spark.wiring=1"
}

run "variables_flow_into_the_module" {
  command = apply

  assert {
    condition = (
      output.function_name == "wiring-dev-writer"
      && output.schedule_name == "wiring-dev-merge"
    )
    error_message = "name_prefix が module に渡っていない"
  }

  assert {
    condition = (
      jsondecode(module.merge_app.schedule_input).jobDriver.sparkSubmit.entryPointArguments == [
        "--staging-bucket", "infra-staging",
        "--target", "s3tablesbucket.wiring_ns.sample_readings",
        "--margin-seconds", "200",
        "--contract", "staging_schema.json",
      ]
      && endswith(jsondecode(module.merge_app.schedule_input).jobDriver.sparkSubmit.sparkSubmitParameters, "--conf spark.wiring=1")
    )
    error_message = "namespace_name / table_name / merge_margin_seconds / spark_submit_parameters が module に渡っていない"
  }
}

run "lambda_esm_and_schedule_settings_flow_into_the_module" {
  command = apply

  assert {
    condition = (
      module.merge_app.applied_settings.lambda_timeout_seconds == 45
      && module.merge_app.applied_settings.lambda_memory_mb == 768
      && module.merge_app.applied_settings.log_retention_days == 3
      && module.merge_app.applied_settings.function_object_version == "wiring-version-id"
      && module.merge_app.applied_settings.schedule_expression == "rate(7 minutes)"
    )
    error_message = "Lambda の timeout / memory / ログ保持日数 / VersionId / schedule_expression が module に渡っていない"
  }

  assert {
    condition = (
      jsonencode(module.merge_app.applied_settings.esm_topics) == jsonencode(["wiring-topic"])
      && module.merge_app.applied_settings.esm_bootstrap_servers == "kafka.wiring:9999"
      && jsonencode(module.merge_app.applied_settings.esm_source_access) == jsonencode(["VPC_SECURITY_GROUP=security_group:sg-wiring", "VPC_SUBNET=subnet:subnet-wiring"])
      && module.merge_app.applied_settings.esm_starting_position == "TRIM_HORIZON"
      && module.merge_app.applied_settings.esm_batch_size == 77
      && module.merge_app.applied_settings.esm_max_batching_window_seconds == 4
    )
    error_message = "ESM の topic / bootstrap / subnet / SG / 開始位置 / バッチ設定が module に渡っていない"
  }
}

run "infra_outputs_flow_into_the_module" {
  command = apply

  assert {
    condition = (
      jsondecode(module.merge_app.schedule_input).applicationId == "00infraapplication"
      && jsondecode(module.merge_app.schedule_input).jobDriver.sparkSubmit.entryPoint == "s3://infra-artifact/merger/job.py"
    )
    error_message = "infra の emr_application_id / artifact_bucket_name が module に渡っていない"
  }

  assert {
    condition = (
      strcontains(module.merge_app.role_policies.writer, "arn:aws:s3:::infra-staging/incoming/*")
      && !strcontains(module.merge_app.role_policies.writer, "infra-artifact")
    )
    error_message = "infra の staging_bucket_arn が Lambda ロールに渡っていない、または artifact と取り違えている"
  }

  assert {
    condition = (
      strcontains(module.merge_app.role_policies.job, "arn:aws:s3:::infra-staging/state/*")
      && strcontains(module.merge_app.role_policies.job, "arn:aws:s3:::infra-artifact/*")
      && !strcontains(module.merge_app.role_policies.job, "arn:aws:s3:::infra-staging/*")
    )
    error_message = "infra の staging_bucket_arn / artifact_bucket_arn がジョブ実行ロールに正しく渡っていない"
  }

  assert {
    condition     = strcontains(module.merge_app.role_policies.scheduler, "/applications/00infraapplication")
    error_message = "infra の emr_application_arn が Scheduler 実行ロールに渡っていない"
  }
}

run "existing_resources_and_the_table_bucket_flow_into_the_module" {
  command = apply

  assert {
    condition = (
      strcontains(module.merge_app.role_policies.job, "arn:aws:s3tables:ap-northeast-1:222222222222:bucket/wiring-table-bucket/table/*")
      && !strcontains(module.merge_app.role_policies.writer, "wiring-table-bucket")
    )
    error_message = "table_bucket_arn がジョブ実行ロールだけに渡っていない"
  }

  assert {
    condition = (
      endswith(
        jsondecode(module.merge_app.schedule_input).jobDriver.sparkSubmit.sparkSubmitParameters,
        "--conf spark.wiring=1"
      )
      && strcontains(
        jsondecode(module.merge_app.schedule_input).jobDriver.sparkSubmit.sparkSubmitParameters,
        "warehouse=arn:aws:s3tables:ap-northeast-1:222222222222:bucket/wiring-table-bucket"
      )
    )
    error_message = "table_bucket_arn が catalog の warehouse に渡っていない"
  }
}

run "outputs_expose_the_module_outputs" {
  command = apply

  assert {
    condition = (
      output.function_name == module.merge_app.function_name
      && output.schedule_name == module.merge_app.schedule_name
      && output.job_role_arn == "arn:aws:iam::111111111111:role/mock"
    )
    error_message = "層の出力が merge_app の出力を指していない"
  }
}
