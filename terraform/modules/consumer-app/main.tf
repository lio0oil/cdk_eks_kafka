data "aws_partition" "current" {}

data "aws_region" "current" {}

data "aws_caller_identity" "current" {}

locals {
  function_name = "kafka-lambda-consumer-${var.cluster_name}"

  # S3 Tables は別 state の管理（EMR consumer 用の別ライフサイクル）のため、名前から ARN を組み立てる
  table_bucket_arn = "arn:${data.aws_partition.current.partition}:s3tables:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:bucket/${var.table_bucket_name}"
}

resource "aws_cloudwatch_log_group" "consumer" {
  name              = "/aws/lambda/${local.function_name}"
  retention_in_days = var.log_retention_days
  skip_destroy      = !var.force_destroy
}

# S3 の on_failure destination はバケット単位でしか指定できない（キー・プレフィックスを絞る手段が無い）
# ため、データ用バケットを流用すると読み取り用途のデータと失敗レコードが混在する。専用バケットを持つ。
module "dlq_bucket" {
  source = "../secure-bucket"

  bucket_prefix = "kafka-consumer-dlq-"
  force_destroy = var.force_destroy
}

resource "aws_iam_role" "consumer" {
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

resource "aws_iam_role_policy_attachment" "basic_execution" {
  role       = aws_iam_role.consumer.name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_iam_role_policy" "consumer" {
  name = "consumer"
  role = aws_iam_role.consumer.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [
      {
        Sid      = "ReadDataBucket"
        Effect   = "Allow"
        Action   = ["s3:GetObject*", "s3:GetBucket*", "s3:List*"]
        Resource = [var.data_bucket_arn, "${var.data_bucket_arn}/*"]
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
        Resource = "*"
      },
      # PutTableData だけでは Iceberg クライアントの commit（メタデータポインタ更新）が完了できない
      {
        Sid    = "WriteIcebergTables"
        Effect = "Allow"
        Action = [
          "s3tables:PutTableData",
          "s3tables:GetTable",
          "s3tables:GetTableMetadataLocation",
          "s3tables:UpdateTableMetadataLocation",
        ]
        Resource = [local.table_bucket_arn, "${local.table_bucket_arn}/table/*"]
      },
      {
        Sid      = "WriteDlqBucket"
        Effect   = "Allow"
        Action   = ["s3:PutObject", "s3:ListBucket"]
        Resource = [module.dlq_bucket.arn, "${module.dlq_bucket.arn}/*"]
      },
    ]
  })
}

# Lambda 本体は VPC にアタッチしない。self-managed Kafka の ESM では VPC 接続（ENI 作成）は
# ESM のソース設定を通じて AWS 側のポーラーが行うため、関数コード自体が VPC 内リソースに
# アクセスしない限り VPC 常駐は不要（ハンドラは現状、受信レコードをログ出力するだけのスタブ）。
resource "aws_lambda_function" "consumer" {
  function_name = local.function_name
  role          = aws_iam_role.consumer.arn
  runtime       = var.lambda_runtime
  handler       = "index.handler"
  timeout       = var.lambda_timeout_seconds

  s3_bucket         = var.artifact_bucket_name
  s3_key            = "function.zip"
  s3_object_version = var.code_object_version

  # ハンドラ側で logger.setLevel() を固定すると ALC の application_log_level が無視されるため、
  # コードでは設定しない。ApplicationLogLevel は JSON 形式でないと指定できない（Lambda API の制約）。
  logging_config {
    log_format            = "JSON"
    application_log_level = var.lambda_application_log_level
    log_group             = aws_cloudwatch_log_group.consumer.name
  }

  depends_on = [aws_iam_role_policy_attachment.basic_execution]
}

# external listener（NLB 経由、plaintext・無認証）を bootstrap servers として使うため、
# source_access_configuration は VPC_SUBNET / VPC_SECURITY_GROUP のみで SASL / TLS 認証は不要。
resource "aws_lambda_event_source_mapping" "kafka" {
  function_name     = aws_lambda_function.consumer.arn
  topics            = [var.topic_name]
  starting_position = var.esm_starting_position

  batch_size = var.esm_batch_size
  # 既定の 500ms だと呼び出し回数が増えやすいため 1 秒に伸ばす。秒単位でしか指定できず、一度変更すると
  # 既定には ESM の再作成でしか戻せない。
  maximum_batching_window_in_seconds = var.esm_max_batching_window_seconds

  self_managed_event_source {
    endpoints = {
      KAFKA_BOOTSTRAP_SERVERS = "${var.bootstrap_dns_name}:${var.bootstrap_port}"
    }
  }

  self_managed_kafka_event_source_config {
    consumer_group_id = "${var.cluster_name}-lambda-esm"
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

  # 破棄されたレコードを追える手段として DLQ バケットを宛先にする。retry が未設定（無限）でも
  # on_failure を設定すると、無限リトライと DLQ の組み合わせを避けるため Lambda 側が実質
  # MaximumRetryAttempts=10 を自動適用する。
  destination_config {
    on_failure {
      destination_arn = module.dlq_bucket.arn
    }
  }

  # ENI 管理権限が無いうちに ESM を作ると、ポーラーが Kafka に接続できない
  depends_on = [aws_iam_role_policy.consumer]
}
