data "aws_partition" "current" {}

locals {
  function_name = "${var.name_prefix}-writer"

  # 置き場所は scripts/build-artifacts.sh の出力と、そのアップロード手順に合わせる
  function_key       = "function.zip"
  job_script_key     = "merger/job.py"
  job_py_files_key   = "merger/modules.zip"
  job_contract_key   = "merger/staging_schema.json"
  job_contract_local = "staging_schema.json"

  # --files で渡した contract はドライバの作業ディレクトリに置かれる
  spark_submit_parameters = join(" ", [
    "--py-files s3://${var.artifact_bucket_name}/${local.job_py_files_key}",
    "--files s3://${var.artifact_bucket_name}/${local.job_contract_key}",
    "--conf spark.sql.catalog.${var.catalog_name}.warehouse=${var.table_bucket_arn}",
    var.spark_submit_parameters,
  ])

  # Scheduler がジョブの実行ごとに置き換えるコンテキスト属性。jsonencode は < > を < > に
  # 変換してしまい Scheduler が認識できなくなるため、仮の文字列を入れて後で置き換える。
  execution_id_placeholder = "EXECUTION_ID_PLACEHOLDER"
  start_job_run_input = replace(jsonencode({
    applicationId    = var.emr_application_id
    executionRoleArn = aws_iam_role.job.arn
    clientToken      = local.execution_id_placeholder
    jobDriver = {
      sparkSubmit = {
        entryPoint = "s3://${var.artifact_bucket_name}/${local.job_script_key}"
        entryPointArguments = [
          "--staging-bucket", var.staging_bucket_name,
          "--target", "${var.catalog_name}.${var.namespace_name}.${var.table_name}",
          "--margin-seconds", tostring(var.merge_margin_seconds),
          "--contract", local.job_contract_local,
        ]
        sparkSubmitParameters = local.spark_submit_parameters
      }
    }
  }), local.execution_id_placeholder, "<aws.scheduler.execution-id>")
}

# Lambda が先に自動作成すると、同名のロググループの作成が失敗するため、関数より前に作る
resource "aws_cloudwatch_log_group" "writer" {
  name              = "/aws/lambda/${local.function_name}"
  retention_in_days = var.log_retention_days
}

resource "aws_iam_role" "writer" {
  name = local.function_name
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "writer_basic_execution" {
  role       = aws_iam_role.writer.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "writer" {
  name = "writer"
  role = aws_iam_role.writer.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid    = "WriteStaging"
        Effect = "Allow"
        Action = ["s3:PutObject"]
        Resource = [
          "${var.staging_bucket_arn}/incoming/*",
          "${var.staging_bucket_arn}/dlq/*",
        ]
      },
      # ESM の ENI 管理権限は実行ロールへの自動付与の対象外。AWS Lambda Developer Guide の
      # self-managed Kafka の VPC アクセス必須権限を明示的に付与する。
      {
        Sid    = "EsmNetworkInterfaces"
        Effect = "Allow"
        Action = [
          "ec2:CreateNetworkInterface",
          "ec2:DescribeNetworkInterfaces",
          "ec2:DescribeVpcs",
          "ec2:DeleteNetworkInterface",
          "ec2:DescribeSubnets",
          "ec2:DescribeSecurityGroups",
        ]
        Resource = ["*"]
      },
    ]
  })
}

# runtime とアーキテクチャは function.zip に同梱する wheel（scripts/build-artifacts.sh）と一致させる。
# Lambda 本体は VPC にアタッチしない。self-managed Kafka の ESM の VPC 接続は AWS 側のポーラーが行う。
resource "aws_lambda_function" "writer" {
  function_name = local.function_name
  role          = aws_iam_role.writer.arn
  runtime       = "python3.13"
  architectures = ["x86_64"]
  handler       = "handler.lambda_handler"
  timeout       = var.lambda_timeout_seconds
  memory_size   = var.lambda_memory_mb

  s3_bucket         = var.artifact_bucket_name
  s3_key            = local.function_key
  s3_object_version = var.s3_object_version

  environment {
    variables = {
      STAGING_BUCKET = var.staging_bucket_name
    }
  }

  depends_on = [
    aws_iam_role_policy_attachment.writer_basic_execution,
    aws_cloudwatch_log_group.writer,
  ]
}

# 既存の Kafka の external listener は plaintext・無認証のため、SASL / TLS は設定せず VPC 接続の設定だけを渡す。
# function_response_types（ReportBatchItemFailures）は設定しない。出力は 1 オブジェクトの PUT で、
# 一部だけ成功する状態がなく、失敗はバッチ全体の再試行で扱う。
resource "aws_lambda_event_source_mapping" "kafka" {
  function_name     = aws_lambda_function.writer.arn
  topics            = [var.topic_name]
  starting_position = var.esm_starting_position

  batch_size                         = var.esm_batch_size
  maximum_batching_window_in_seconds = var.esm_max_batching_window_seconds

  self_managed_event_source {
    endpoints = {
      KAFKA_BOOTSTRAP_SERVERS = "${var.bootstrap_dns_name}:${var.bootstrap_port}"
    }
  }

  self_managed_kafka_event_source_config {
    consumer_group_id = "${var.name_prefix}-lambda-esm"
  }

  source_access_configuration {
    type = "VPC_SECURITY_GROUP"
    uri  = "security_group:${var.esm_security_group_id}"
  }

  dynamic "source_access_configuration" {
    for_each = var.subnet_ids

    content {
      type = "VPC_SUBNET"
      uri  = "subnet:${source_access_configuration.value}"
    }
  }

  # ENI 管理権限が無いうちに ESM を作ると、ポーラーが Kafka に接続できない
  depends_on = [aws_iam_role_policy.writer]
}

resource "aws_iam_role" "job" {
  name = "${var.name_prefix}-job"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "emr-serverless.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

# staging の DeleteObject は付けない。古いファイルはライフサイクルが消し、ジョブは消さない。
# ListBucket が無いと、存在しない state/watermark.json の取得が NoSuchKey ではなく AccessDenied になる。
resource "aws_iam_role_policy" "job" {
  name = "job"
  role = aws_iam_role.job.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ListBuckets"
        Effect   = "Allow"
        Action   = ["s3:ListBucket"]
        Resource = [var.staging_bucket_arn, var.artifact_bucket_arn]
      },
      {
        Sid      = "ReadIncoming"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${var.staging_bucket_arn}/incoming/*"]
      },
      {
        Sid      = "ReadWriteState"
        Effect   = "Allow"
        Action   = ["s3:GetObject", "s3:PutObject"]
        Resource = ["${var.staging_bucket_arn}/state/*"]
      },
      {
        Sid      = "ReadArtifacts"
        Effect   = "Allow"
        Action   = ["s3:GetObject"]
        Resource = ["${var.artifact_bucket_arn}/*"]
      },
      # Iceberg の読み書きと commit（メタデータポインタの更新）に要るアクションを列挙する。
      # テーブルは UUID で指されるため、テーブル名ではなくバケット配下のテーブルすべてを対象にする。
      {
        Sid    = "ReadWriteTables"
        Effect = "Allow"
        Action = [
          "s3tables:GetTableBucket",
          "s3tables:GetNamespace",
          "s3tables:GetTable",
          "s3tables:GetTableMetadataLocation",
          "s3tables:GetTableData",
          "s3tables:PutTableData",
          "s3tables:UpdateTableMetadataLocation",
        ]
        Resource = [var.table_bucket_arn, "${var.table_bucket_arn}/table/*"]
      },
    ]
  })
}

resource "aws_iam_role" "scheduler" {
  name = "${var.name_prefix}-scheduler"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "scheduler.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy" "scheduler" {
  name = "scheduler"
  role = aws_iam_role.scheduler.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "StartJobRun"
        Effect   = "Allow"
        Action   = ["emr-serverless:StartJobRun"]
        Resource = [var.emr_application_arn]
      },
      {
        Sid       = "PassJobRole"
        Effect    = "Allow"
        Action    = ["iam:PassRole"]
        Resource  = [aws_iam_role.job.arn]
        Condition = { StringEquals = { "iam:PassedToService" = "emr-serverless.amazonaws.com" } }
      },
    ]
  })
}

resource "aws_scheduler_schedule" "merge" {
  name                = "${var.name_prefix}-merge"
  schedule_expression = var.schedule_expression

  flexible_time_window {
    mode = "OFF"
  }

  target {
    arn      = "arn:${data.aws_partition.current.partition}:scheduler:::aws-sdk:emrserverless:startJobRun"
    role_arn = aws_iam_role.scheduler.arn
    input    = local.start_job_run_input

    # MERGE は冪等で次の実行が同じ範囲を処理し直すため、失敗した実行を再試行しない
    retry_policy {
      maximum_retry_attempts = 0
    }
  }

  depends_on = [aws_iam_role_policy.scheduler]
}
