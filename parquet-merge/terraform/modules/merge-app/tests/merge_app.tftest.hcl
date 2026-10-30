mock_provider "aws" {
  mock_data "aws_partition" {
    defaults = {
      partition = "aws"
    }
  }
}

# mock の既定値は全インスタンスで同じ ARN になり、ロールの取り違えを検出できない。ロールごとに別の値を与える。
override_resource {
  target = aws_iam_role.writer
  values = {
    arn = "arn:aws:iam::111111111111:role/mock-writer"
  }
}

override_resource {
  target = aws_iam_role.job
  values = {
    arn = "arn:aws:iam::111111111111:role/mock-job"
  }
}

override_resource {
  target = aws_iam_role.scheduler
  values = {
    arn = "arn:aws:iam::111111111111:role/mock-scheduler"
  }
}

variables {
  name_prefix           = "parquet-merge-dev"
  subnet_ids            = ["subnet-aaaa"]
  esm_security_group_id = "sg-0123456789abcdef0"
  bootstrap_dns_name    = "kafka.local"
  bootstrap_port        = 9094
  table_bucket_arn      = "arn:aws:s3tables:ap-northeast-1:111111111111:bucket/mock-table-bucket"
  namespace_name        = "events"

  staging_bucket_name  = "mock-staging"
  staging_bucket_arn   = "arn:aws:s3:::mock-staging"
  artifact_bucket_name = "mock-artifact"
  artifact_bucket_arn  = "arn:aws:s3:::mock-artifact"
  emr_application_id   = "00mockapplication"
  emr_application_arn  = "arn:aws:emr-serverless:ap-northeast-1:111111111111:/applications/00mockapplication"

  s3_object_version = "3HL4kqtJlcpXroDTDmJ.rmSpXd3dIbrHY"

  esm_batch_size                  = 100
  esm_max_batching_window_seconds = 1
  log_retention_days              = 7

  spark_submit_parameters = "--conf spark.mock=1"
}

run "lambda_runs_the_uploaded_package_with_the_staging_bucket" {
  command = apply

  assert {
    condition = (
      aws_lambda_function.writer.function_name == "parquet-merge-dev-writer"
      && aws_lambda_function.writer.runtime == "python3.13"
      && aws_lambda_function.writer.handler == "handler.lambda_handler"
      && aws_lambda_function.writer.role == "arn:aws:iam::111111111111:role/mock-writer"
    )
    error_message = "Lambda の名前 / runtime / handler / 実行ロールが期待と異なる"
  }

  # function.zip に同梱する wheel はこの runtime とアーキテクチャ向けにビルドする
  assert {
    condition     = jsonencode(aws_lambda_function.writer.architectures) == jsonencode(["x86_64"])
    error_message = "アーキテクチャが x86_64 でない（build-artifacts.sh の wheel と一致しなくなる）"
  }

  assert {
    condition = (
      aws_lambda_function.writer.s3_bucket == "mock-artifact"
      && aws_lambda_function.writer.s3_key == "function.zip"
      && aws_lambda_function.writer.s3_object_version == "3HL4kqtJlcpXroDTDmJ.rmSpXd3dIbrHY"
    )
    error_message = "デプロイパッケージが artifact バケットの指定 VersionId から読まれていない"
  }

  assert {
    condition     = one(aws_lambda_function.writer.environment).variables == tomap({ STAGING_BUCKET = "mock-staging" })
    error_message = "環境変数 STAGING_BUCKET が staging バケット名になっていない"
  }

  assert {
    condition     = length(aws_lambda_function.writer.vpc_config) == 0
    error_message = "Lambda が VPC にアタッチされている（ポーラーの ENI は ESM 側が作る）"
  }
}

run "lambda_timeout_and_memory_default_and_follow_variables" {
  command = apply

  assert {
    condition     = aws_lambda_function.writer.timeout == 60 && aws_lambda_function.writer.memory_size == 512
    error_message = "timeout / memory の既定値が 60 秒 / 512MB でない"
  }

  assert {
    condition     = aws_cloudwatch_log_group.writer.name == "/aws/lambda/parquet-merge-dev-writer" && aws_cloudwatch_log_group.writer.retention_in_days == 7
    error_message = "ロググループの名前 / 保持日数が期待と異なる"
  }
}

run "lambda_timeout_and_memory_can_be_changed" {
  command = apply

  variables {
    lambda_timeout_seconds = 30
    lambda_memory_mb       = 1024
    merge_margin_seconds   = 60
  }

  assert {
    condition     = aws_lambda_function.writer.timeout == 30 && aws_lambda_function.writer.memory_size == 1024
    error_message = "timeout / memory が variables から決まっていない"
  }
}

run "writer_role_is_assumed_by_lambda_with_basic_execution" {
  command = apply

  assert {
    condition = (
      aws_iam_role.writer.name == "parquet-merge-dev-writer"
      && jsondecode(aws_iam_role.writer.assume_role_policy).Statement[0].Principal.Service == "lambda.amazonaws.com"
      && jsondecode(aws_iam_role.writer.assume_role_policy).Statement[0].Action == "sts:AssumeRole"
    )
    error_message = "Lambda 実行ロールの名前 / 信頼先が期待と異なる"
  }

  assert {
    condition     = aws_iam_role_policy_attachment.writer_basic_execution.policy_arn == "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
    error_message = "AWSLambdaBasicExecutionRole が付いていない"
  }
}

# Lambda は staging に書くだけ。本テーブルへの書き込み、staging の読み取り・削除、state/ には触れない。
run "writer_role_can_only_put_into_incoming_and_dlq" {
  command = apply

  assert {
    condition = (
      toset(flatten([
        for s in jsondecode(aws_iam_role_policy.writer.policy).Statement : s.Action if !startswith(s.Action[0], "ec2:")
      ])) == toset(["s3:PutObject"])
    )
    error_message = "ENI 管理以外の許可が s3:PutObject だけになっていない"
  }

  assert {
    condition = (
      toset(flatten([
        for s in jsondecode(aws_iam_role_policy.writer.policy).Statement : s.Resource if !startswith(s.Action[0], "ec2:")
      ])) == toset(["arn:aws:s3:::mock-staging/incoming/*", "arn:aws:s3:::mock-staging/dlq/*"])
    )
    error_message = "PutObject の対象が staging の incoming/* と dlq/* だけになっていない"
  }

  assert {
    condition = (
      !strcontains(aws_iam_role_policy.writer.policy, "s3tables")
      && !strcontains(aws_iam_role_policy.writer.policy, "mock-table-bucket")
      && !strcontains(aws_iam_role_policy.writer.policy, "state/")
      && !strcontains(aws_iam_role_policy.writer.policy, "s3:Delete")
      && !strcontains(aws_iam_role_policy.writer.policy, "s3:Get")
      && !strcontains(aws_iam_role_policy.writer.policy, "mock-artifact")
    )
    error_message = "Lambda ロールが本テーブル / state / artifact に触れる、または Delete 系を許可している"
  }

  # ESM の ENI 管理権限は実行ロールへの自動付与の対象外。self-managed Kafka の VPC アクセス必須権限を明示的に付与する
  assert {
    condition = (
      toset(flatten([
        for s in jsondecode(aws_iam_role_policy.writer.policy).Statement : s.Action if startswith(s.Action[0], "ec2:")
        ])) == toset([
        "ec2:CreateNetworkInterface",
        "ec2:DescribeNetworkInterfaces",
        "ec2:DescribeVpcs",
        "ec2:DeleteNetworkInterface",
        "ec2:DescribeSubnets",
        "ec2:DescribeSecurityGroups",
      ])
    )
    error_message = "ESM の ENI 管理に必要な ec2 権限が揃っていない"
  }
}

run "esm_subscribes_the_topic_inside_the_vpc" {
  command = apply

  assert {
    condition = (
      aws_lambda_event_source_mapping.kafka.function_name == aws_lambda_function.writer.arn
      && aws_lambda_event_source_mapping.kafka.topics == toset(["sample-readings"])
      && aws_lambda_event_source_mapping.kafka.starting_position == "LATEST"
      && one(aws_lambda_event_source_mapping.kafka.self_managed_event_source).endpoints["KAFKA_BOOTSTRAP_SERVERS"] == "kafka.local:9094"
      && one(aws_lambda_event_source_mapping.kafka.self_managed_kafka_event_source_config).consumer_group_id == "parquet-merge-dev-lambda-esm"
    )
    error_message = "ESM の関数 / topic / 開始位置 / bootstrap / consumer group が期待と異なる"
  }

  # 既存の Kafka の external listener は plaintext・無認証のため、SASL / TLS は設定せず VPC 接続の設定だけを渡す
  assert {
    condition = toset([
      for c in aws_lambda_event_source_mapping.kafka.source_access_configuration : "${c.type}=${c.uri}"
      ]) == toset([
      "VPC_SECURITY_GROUP=security_group:sg-0123456789abcdef0",
      "VPC_SUBNET=subnet:subnet-aaaa",
    ])
    error_message = "ESM の VPC_SUBNET / VPC_SECURITY_GROUP が期待と異なる"
  }

  assert {
    condition     = aws_lambda_event_source_mapping.kafka.batch_size == 100 && aws_lambda_event_source_mapping.kafka.maximum_batching_window_in_seconds == 1
    error_message = "バッチサイズ 100 / バッチウィンドウ 1 秒が配線されていない"
  }

  # 出力は 1 オブジェクトの PUT で、一部だけ成功する状態がない。失敗はバッチ全体の再試行で扱う
  assert {
    condition     = try(length(aws_lambda_event_source_mapping.kafka.function_response_types), 0) == 0
    error_message = "ReportBatchItemFailures（function_response_types）が設定されている"
  }
}

run "esm_follows_the_variables" {
  command = apply

  variables {
    topic_name                      = "other-topic"
    subnet_ids                      = ["subnet-aaaa", "subnet-bbbb"]
    esm_batch_size                  = 50
    esm_max_batching_window_seconds = 5
    esm_starting_position           = "TRIM_HORIZON"
  }

  assert {
    condition = (
      aws_lambda_event_source_mapping.kafka.topics == toset(["other-topic"])
      && aws_lambda_event_source_mapping.kafka.batch_size == 50
      && aws_lambda_event_source_mapping.kafka.maximum_batching_window_in_seconds == 5
      && aws_lambda_event_source_mapping.kafka.starting_position == "TRIM_HORIZON"
    )
    error_message = "topic / バッチ設定 / 開始位置が variables から決まっていない"
  }

  assert {
    condition = toset([
      for c in aws_lambda_event_source_mapping.kafka.source_access_configuration : c.uri if c.type == "VPC_SUBNET"
    ]) == toset(["subnet:subnet-aaaa", "subnet:subnet-bbbb"])
    error_message = "全 subnet が ESM に渡っていない"
  }
}

run "job_role_is_assumed_by_emr_serverless" {
  command = apply

  assert {
    condition = (
      aws_iam_role.job.name == "parquet-merge-dev-job"
      && jsondecode(aws_iam_role.job.assume_role_policy).Statement[0].Principal.Service == "emr-serverless.amazonaws.com"
      && jsondecode(aws_iam_role.job.assume_role_policy).Statement[0].Action == "sts:AssumeRole"
    )
    error_message = "ジョブ実行ロールの名前 / 信頼先が期待と異なる"
  }
}

# ジョブは staging を読み、state/ にだけ書く。staging の削除は不可（ライフサイクルが消す）。
run "job_role_reads_staging_and_writes_only_state" {
  command = apply

  assert {
    condition = (
      toset(flatten([
        for s in jsondecode(aws_iam_role_policy.job.policy).Statement : [
          for r in s.Resource : "${join(",", sort(s.Action))}@${r}" if !startswith(s.Action[0], "s3tables:")
        ]
        ])) == toset([
        "s3:GetObject@arn:aws:s3:::mock-staging/incoming/*",
        "s3:GetObject,s3:PutObject@arn:aws:s3:::mock-staging/state/*",
        "s3:GetObject@arn:aws:s3:::mock-artifact/*",
        "s3:ListBucket@arn:aws:s3:::mock-staging",
        "s3:ListBucket@arn:aws:s3:::mock-artifact",
      ])
    )
    error_message = "ジョブ実行ロールの S3 の許可が、staging の読み取り / state の読み書き / artifact の読み取りだけになっていない"
  }

  assert {
    condition = (
      !strcontains(aws_iam_role_policy.job.policy, "Delete")
      && !strcontains(aws_iam_role_policy.job.policy, "s3:*")
      && !strcontains(aws_iam_role_policy.job.policy, "\"*\"")
    )
    error_message = "ジョブ実行ロールが Delete 系またはワイルドカードの許可を持つ"
  }

  assert {
    condition     = !strcontains(aws_iam_role_policy.job.policy, "dlq/")
    error_message = "ジョブ実行ロールが dlq/ に触れる"
  }
}

run "job_role_touches_only_the_given_table_bucket" {
  command = apply

  assert {
    condition = (
      toset(flatten([
        for s in jsondecode(aws_iam_role_policy.job.policy).Statement : s.Action if startswith(s.Action[0], "s3tables:")
        ])) == toset([
        "s3tables:GetTableBucket",
        "s3tables:GetNamespace",
        "s3tables:GetTable",
        "s3tables:GetTableMetadataLocation",
        "s3tables:GetTableData",
        "s3tables:PutTableData",
        "s3tables:UpdateTableMetadataLocation",
      ])
    )
    error_message = "S3 Tables の許可が、読み書きに必要な 7 アクションと一致しない"
  }

  assert {
    condition = (
      toset(flatten([
        for s in jsondecode(aws_iam_role_policy.job.policy).Statement : s.Resource if startswith(s.Action[0], "s3tables:")
        ])) == toset([
        "arn:aws:s3tables:ap-northeast-1:111111111111:bucket/mock-table-bucket",
        "arn:aws:s3tables:ap-northeast-1:111111111111:bucket/mock-table-bucket/table/*",
      ])
    )
    error_message = "S3 Tables の許可の対象が table_bucket_arn のバケットとそのテーブルだけになっていない"
  }
}

run "job_role_follows_the_table_bucket_variable" {
  command = apply

  variables {
    table_bucket_arn = "arn:aws:s3tables:ap-northeast-1:222222222222:bucket/another"
  }

  assert {
    condition = (
      toset(flatten([
        for s in jsondecode(aws_iam_role_policy.job.policy).Statement : s.Resource if startswith(s.Action[0], "s3tables:")
        ])) == toset([
        "arn:aws:s3tables:ap-northeast-1:222222222222:bucket/another",
        "arn:aws:s3tables:ap-northeast-1:222222222222:bucket/another/table/*",
      ])
      && !strcontains(aws_iam_role_policy.job.policy, "mock-table-bucket")
    )
    error_message = "ジョブ実行ロールの S3 Tables の許可が table_bucket_arn に追従していない、または別のバケットを含む"
  }

  assert {
    condition     = !strcontains(aws_iam_role_policy.writer.policy, "another")
    error_message = "Lambda ロールが本テーブルのバケットを含む"
  }
}

run "scheduler_role_can_only_start_the_application_and_pass_the_job_role" {
  command = apply

  assert {
    condition = (
      aws_iam_role.scheduler.name == "parquet-merge-dev-scheduler"
      && jsondecode(aws_iam_role.scheduler.assume_role_policy).Statement[0].Principal.Service == "scheduler.amazonaws.com"
      && jsondecode(aws_iam_role.scheduler.assume_role_policy).Statement[0].Action == "sts:AssumeRole"
    )
    error_message = "Scheduler 実行ロールの名前 / 信頼先が期待と異なる"
  }

  assert {
    condition = (
      toset(flatten([
        for s in jsondecode(aws_iam_role_policy.scheduler.policy).Statement : [
          for r in s.Resource : "${join(",", sort(s.Action))}@${r}"
        ]
        ])) == toset([
        "emr-serverless:StartJobRun@arn:aws:emr-serverless:ap-northeast-1:111111111111:/applications/00mockapplication",
        "iam:PassRole@arn:aws:iam::111111111111:role/mock-job",
      ])
    )
    error_message = "StartJobRun が対象アプリだけ、PassRole がジョブ実行ロールだけになっていない"
  }

  # PassRole を EMR Serverless 以外のサービスに渡せないようにする
  assert {
    condition = [
      for s in jsondecode(aws_iam_role_policy.scheduler.policy).Statement : s.Condition if s.Action[0] == "iam:PassRole"
    ][0] == { StringEquals = { "iam:PassedToService" = "emr-serverless.amazonaws.com" } }
    error_message = "PassRole が iam:PassedToService = emr-serverless.amazonaws.com に限られていない"
  }
}

run "schedule_calls_start_job_run_directly_every_5_minutes" {
  command = apply

  assert {
    condition = (
      aws_scheduler_schedule.merge.name == "parquet-merge-dev-merge"
      && aws_scheduler_schedule.merge.schedule_expression == "rate(5 minutes)"
      && one(aws_scheduler_schedule.merge.flexible_time_window).mode == "OFF"
    )
    error_message = "Scheduler の名前 / schedule_expression / 柔軟な時間枠が期待と異なる"
  }

  assert {
    condition = (
      one(aws_scheduler_schedule.merge.target).arn == "arn:aws:scheduler:::aws-sdk:emrserverless:startJobRun"
      && one(aws_scheduler_schedule.merge.target).role_arn == "arn:aws:iam::111111111111:role/mock-scheduler"
    )
    error_message = "ターゲットが EMR Serverless の StartJobRun（ユニバーサルターゲット）と Scheduler 実行ロールになっていない"
  }

  # 次の実行が同じ範囲を処理し直すため、失敗した実行の再試行は要らない
  assert {
    condition     = one(one(aws_scheduler_schedule.merge.target).retry_policy).maximum_retry_attempts == 0
    error_message = "Scheduler の再試行が無効になっていない"
  }
}

# jsonencode は < と > を < > に変換する。Scheduler は置換対象の文字列をそのまま探すため、
# コンテキスト属性は変換されずに入力へ入っている必要がある。
run "schedule_input_carries_the_execution_id_as_the_client_token" {
  command = apply

  assert {
    condition     = strcontains(one(aws_scheduler_schedule.merge.target).input, "\"clientToken\":\"<aws.scheduler.execution-id>\"")
    error_message = "clientToken が Scheduler のコンテキスト属性 <aws.scheduler.execution-id> のままの文字列になっていない"
  }

  assert {
    condition     = !strcontains(one(aws_scheduler_schedule.merge.target).input, "\\u003c")
    error_message = "入力に \\u003c が残っている（jsonencode のエスケープ）"
  }
}

run "schedule_input_starts_the_merge_job" {
  command = apply

  assert {
    condition = (
      jsondecode(one(aws_scheduler_schedule.merge.target).input).applicationId == "00mockapplication"
      && jsondecode(one(aws_scheduler_schedule.merge.target).input).executionRoleArn == "arn:aws:iam::111111111111:role/mock-job"
    )
    error_message = "入力の applicationId / executionRoleArn が EMR Serverless アプリ / ジョブ実行ロールと異なる"
  }

  assert {
    condition = (
      jsondecode(one(aws_scheduler_schedule.merge.target).input).jobDriver.sparkSubmit.entryPoint == "s3://mock-artifact/merger/job.py"
      && jsondecode(one(aws_scheduler_schedule.merge.target).input).jobDriver.sparkSubmit.entryPointArguments == [
        "--staging-bucket", "mock-staging",
        "--target", "s3tablesbucket.events.sample_readings",
        "--margin-seconds", "120",
        "--contract", "staging_schema.json",
      ]
    )
    error_message = "入力の entryPoint / entryPointArguments（staging バケット、対象テーブル、margin、contract）が期待と異なる"
  }

  assert {
    condition = (
      jsondecode(one(aws_scheduler_schedule.merge.target).input).jobDriver.sparkSubmit.sparkSubmitParameters
      == "--py-files s3://mock-artifact/merger/modules.zip --files s3://mock-artifact/merger/staging_schema.json --conf spark.sql.catalog.s3tablesbucket.warehouse=arn:aws:s3tables:ap-northeast-1:111111111111:bucket/mock-table-bucket --conf spark.mock=1"
    )
    error_message = "sparkSubmitParameters が py-files / files / warehouse / 変数の順になっていない"
  }
}

run "schedule_follows_the_variables" {
  command = apply

  variables {
    schedule_expression  = "rate(10 minutes)"
    merge_margin_seconds = 300
    namespace_name       = "other_ns"
    table_name           = "other_table"
    catalog_name         = "cat"
  }

  assert {
    condition     = aws_scheduler_schedule.merge.schedule_expression == "rate(10 minutes)"
    error_message = "schedule_expression が variables から決まっていない"
  }

  assert {
    condition = (
      jsondecode(one(aws_scheduler_schedule.merge.target).input).jobDriver.sparkSubmit.entryPointArguments == [
        "--staging-bucket", "mock-staging",
        "--target", "cat.other_ns.other_table",
        "--margin-seconds", "300",
        "--contract", "staging_schema.json",
      ]
      && strcontains(jsondecode(one(aws_scheduler_schedule.merge.target).input).jobDriver.sparkSubmit.sparkSubmitParameters, "spark.sql.catalog.cat.warehouse=")
    )
    error_message = "対象テーブル / margin / catalog 名が variables から決まっていない"
  }
}

# writer のラベルは呼び出し開始の分で、PUT はその後 timeout までの間に起こり得る。margin が timeout より短いと、
# まだ書き込み中の分を閉じたものとして処理してしまう。30 秒は Lambda と EMR の時計のずれの余裕。
run "margin_equal_to_timeout_plus_30_seconds_is_accepted" {
  command = plan

  variables {
    lambda_timeout_seconds = 60
    merge_margin_seconds   = 90
  }
}

run "margin_below_timeout_plus_30_seconds_is_rejected" {
  command = plan

  variables {
    lambda_timeout_seconds = 60
    merge_margin_seconds   = 89
  }

  expect_failures = [var.merge_margin_seconds]
}

run "raising_the_timeout_without_the_margin_is_rejected" {
  command = plan

  variables {
    lambda_timeout_seconds = 120
  }

  expect_failures = [var.merge_margin_seconds]
}

run "outputs_expose_the_function_the_schedule_and_the_job_role" {
  command = apply

  assert {
    condition = (
      output.function_name == "parquet-merge-dev-writer"
      && output.schedule_name == "parquet-merge-dev-merge"
      && output.job_role_arn == "arn:aws:iam::111111111111:role/mock-job"
    )
    error_message = "出力が Lambda / Scheduler / ジョブ実行ロールを指していない"
  }
}
