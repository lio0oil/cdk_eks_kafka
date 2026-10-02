mock_provider "aws" {
  mock_data "aws_partition" {
    defaults = {
      partition = "aws"
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

variables {
  cluster_name          = "eks-cluster-dev"
  subnet_ids            = ["subnet-aaaa"]
  esm_security_group_id = "sg-0123456789abcdef0"
  data_bucket_arn       = "arn:aws:s3:::mock-data-bucket"
  artifact_bucket_name  = "mock-artifact-bucket"
  code_object_version   = "3HL4kqtJlcpXroDTDmJ.rmSpXd3dIbrHY"
  bootstrap_dns_name    = "kafka.local"
  bootstrap_port        = 9094
  topic_name            = "test-topic"
  table_bucket_name     = "kafka-events-dev"
  log_retention_days    = 7
  force_destroy         = true

  lambda_runtime                  = "python3.13"
  lambda_timeout_seconds          = 30
  lambda_application_log_level    = "INFO"
  esm_starting_position           = "LATEST"
  esm_batch_size                  = 100
  esm_max_batching_window_seconds = 1
}

run "lambda_runs_the_uploaded_package_outside_the_vpc" {
  command = apply

  assert {
    condition = (
      aws_lambda_function.consumer.function_name == "kafka-lambda-consumer-eks-cluster-dev"
      && aws_lambda_function.consumer.runtime == "python3.13"
      && aws_lambda_function.consumer.handler == "index.handler"
      && aws_lambda_function.consumer.timeout == 30
      && aws_lambda_function.consumer.role == aws_iam_role.consumer.arn
    )
    error_message = "Lambda の名前 / runtime / handler / timeout / 実行ロールが期待と異なる"
  }

  assert {
    condition = (
      aws_lambda_function.consumer.s3_bucket == "mock-artifact-bucket"
      && aws_lambda_function.consumer.s3_key == "function.zip"
      && aws_lambda_function.consumer.s3_object_version == "3HL4kqtJlcpXroDTDmJ.rmSpXd3dIbrHY"
    )
    error_message = "デプロイパッケージが artifact バケットの指定 VersionId から読まれていない"
  }

  # self-managed Kafka の ESM では VPC 接続（ENI 作成）は AWS 側のポーラーが行うため、
  # 関数コード自体が VPC 内リソースにアクセスしない限り Lambda を VPC に置く必要はない
  assert {
    condition     = length(aws_lambda_function.consumer.vpc_config) == 0
    error_message = "Lambda が VPC にアタッチされている"
  }
}

# ハンドラ側で logger.setLevel() を固定すると ALC の application_log_level が無視される。
# ApplicationLogLevel は JSON 形式でないと指定できない（Lambda API の制約）
run "logs_use_advanced_logging_controls" {
  command = apply

  assert {
    condition = (
      aws_lambda_function.consumer.logging_config[0].log_format == "JSON"
      && aws_lambda_function.consumer.logging_config[0].application_log_level == "INFO"
      && aws_lambda_function.consumer.logging_config[0].log_group == aws_cloudwatch_log_group.consumer.name
    )
    error_message = "ログが JSON 形式 / INFO / 専用ロググループになっていない"
  }

  assert {
    condition = (
      aws_cloudwatch_log_group.consumer.name == "/aws/lambda/kafka-lambda-consumer-eks-cluster-dev"
      && aws_cloudwatch_log_group.consumer.retention_in_days == 7
      && !aws_cloudwatch_log_group.consumer.skip_destroy
    )
    error_message = "ロググループの名前 / 保持日数 / 削除時の扱いが期待と異なる"
  }
}

run "stg_prd_keep_the_log_group_on_destroy" {
  command = apply

  variables {
    force_destroy = false
  }

  assert {
    condition     = aws_cloudwatch_log_group.consumer.skip_destroy
    error_message = "force_destroy=false でロググループが destroy 時に残らない"
  }
}

run "execution_role_is_assumed_by_lambda_with_basic_execution" {
  command = apply

  assert {
    condition = (
      aws_iam_role.consumer.name == "kafka-lambda-consumer-eks-cluster-dev"
      && jsondecode(aws_iam_role.consumer.assume_role_policy).Statement[0].Principal.Service == "lambda.amazonaws.com"
    )
    error_message = "実行ロールの名前 / 信頼先が期待と異なる"
  }

  assert {
    condition     = aws_iam_role_policy_attachment.basic_execution.policy_arn == "arn:aws:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
    error_message = "AWSLambdaBasicExecutionRole が付いていない"
  }
}

run "role_can_read_data_and_write_only_its_own_targets" {
  command = apply

  assert {
    condition = (
      toset([for s in jsondecode(aws_iam_role_policy.consumer.policy).Statement : s.Sid]) == toset([
        "ReadDataBucket", "EsmNetworkInterfaces", "WriteIcebergTables", "WriteDlqBucket",
      ])
    )
    error_message = "ポリシーが 4 つの用途（データ読み取り / ENI / Iceberg 書き込み / DLQ 書き込み）に分かれていない"
  }

  assert {
    condition = (
      [for s in jsondecode(aws_iam_role_policy.consumer.policy).Statement : s if s.Sid == "ReadDataBucket"][0].Action == ["s3:GetObject*", "s3:GetBucket*", "s3:List*"]
      && toset([for s in jsondecode(aws_iam_role_policy.consumer.policy).Statement : s if s.Sid == "ReadDataBucket"][0].Resource) == toset(["arn:aws:s3:::mock-data-bucket", "arn:aws:s3:::mock-data-bucket/*"])
    )
    error_message = "データ用バケットが読み取り専用になっていない"
  }

  # ESM の ENI 管理権限は実行ロールへの自動付与の対象外。AWS Lambda Developer Guide の
  # self-managed Kafka VPC アクセスの必須権限を明示的に付与する
  assert {
    condition = (
      toset([for s in jsondecode(aws_iam_role_policy.consumer.policy).Statement : s if s.Sid == "EsmNetworkInterfaces"][0].Action) == toset([
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

  # PutTableData だけでは Iceberg クライアントの commit（メタデータポインタ更新）が完了できない
  assert {
    condition = (
      toset([for s in jsondecode(aws_iam_role_policy.consumer.policy).Statement : s if s.Sid == "WriteIcebergTables"][0].Action) == toset([
        "s3tables:PutTableData",
        "s3tables:GetTable",
        "s3tables:GetTableMetadataLocation",
        "s3tables:UpdateTableMetadataLocation",
      ])
      && toset([for s in jsondecode(aws_iam_role_policy.consumer.policy).Statement : s if s.Sid == "WriteIcebergTables"][0].Resource) == toset([
        "arn:aws:s3tables:ap-northeast-1:111111111111:bucket/kafka-events-dev",
        "arn:aws:s3tables:ap-northeast-1:111111111111:bucket/kafka-events-dev/table/*",
      ])
    )
    error_message = "S3 Tables の書き込み権限が対象 table bucket とテーブルに限定されていない"
  }

  # S3 の on_failure destination への配送に必要な権限だけを DLQ バケットに付与する
  assert {
    condition = (
      toset([for s in jsondecode(aws_iam_role_policy.consumer.policy).Statement : s if s.Sid == "WriteDlqBucket"][0].Action) == toset(["s3:PutObject", "s3:ListBucket"])
      && toset([for s in jsondecode(aws_iam_role_policy.consumer.policy).Statement : s if s.Sid == "WriteDlqBucket"][0].Resource) == toset(["arn:aws:s3:::mock-dlq-bucket", "arn:aws:s3:::mock-dlq-bucket/*"])
    )
    error_message = "DLQ バケットへの書き込み権限が PutObject / ListBucket に限定されていない"
  }
}

run "esm_subscribes_the_topic_through_the_nlb_inside_the_vpc" {
  command = apply

  assert {
    condition = (
      aws_lambda_event_source_mapping.kafka.function_name == aws_lambda_function.consumer.arn
      && aws_lambda_event_source_mapping.kafka.topics == toset(["test-topic"])
      && aws_lambda_event_source_mapping.kafka.starting_position == "LATEST"
      && one(aws_lambda_event_source_mapping.kafka.self_managed_event_source).endpoints["KAFKA_BOOTSTRAP_SERVERS"] == "kafka.local:9094"
      && one(aws_lambda_event_source_mapping.kafka.self_managed_kafka_event_source_config).consumer_group_id == "eks-cluster-dev-lambda-esm"
    )
    error_message = "ESM の topic / 開始位置 / bootstrap / consumer group が期待と異なる"
  }

  # external listener は plaintext・無認証のため SASL / TLS の設定は不要で、VPC 接続の設定だけを渡す
  assert {
    condition = toset([
      for c in aws_lambda_event_source_mapping.kafka.source_access_configuration : "${c.type}=${c.uri}"
      ]) == toset([
      "VPC_SECURITY_GROUP=security_group:sg-0123456789abcdef0",
      "VPC_SUBNET=subnet:subnet-aaaa",
    ])
    error_message = "ESM の VPC_SUBNET / VPC_SECURITY_GROUP が期待と異なる"
  }

  # 既定の 500ms だと呼び出し回数が増えやすいため 1 秒に伸ばす
  assert {
    condition     = aws_lambda_event_source_mapping.kafka.batch_size == 100 && aws_lambda_event_source_mapping.kafka.maximum_batching_window_in_seconds == 1
    error_message = "バッチサイズ 100 / バッチウィンドウ 1 秒になっていない"
  }

  assert {
    condition     = one(one(aws_lambda_event_source_mapping.kafka.destination_config).on_failure).destination_arn == "arn:aws:s3:::mock-dlq-bucket"
    error_message = "処理に失敗したレコードの送り先が DLQ バケットになっていない"
  }
}

run "esm_follows_all_given_subnets" {
  command = apply

  variables {
    subnet_ids = ["subnet-aaaa", "subnet-bbbb", "subnet-cccc"]
  }

  assert {
    condition = toset([
      for c in aws_lambda_event_source_mapping.kafka.source_access_configuration : c.uri if c.type == "VPC_SUBNET"
    ]) == toset(["subnet:subnet-aaaa", "subnet:subnet-bbbb", "subnet:subnet-cccc"])
    error_message = "multi-AZ で全 subnet が ESM に渡っていない"
  }
}

# Lambda の runtime / timeout / ログレベルと、ESM の開始位置 / バッチ設定は、module に値を持たず呼び出し側が決める
run "lambda_and_esm_settings_come_from_variables" {
  command = apply

  variables {
    lambda_runtime                  = "python3.12"
    lambda_timeout_seconds          = 60
    lambda_application_log_level    = "DEBUG"
    esm_starting_position           = "TRIM_HORIZON"
    esm_batch_size                  = 50
    esm_max_batching_window_seconds = 5
  }

  assert {
    condition = (
      aws_lambda_function.consumer.runtime == "python3.12"
      && aws_lambda_function.consumer.timeout == 60
      && aws_lambda_function.consumer.logging_config[0].application_log_level == "DEBUG"
    )
    error_message = "Lambda の runtime / timeout / ログレベルが variables から決まっていない"
  }

  assert {
    condition = (
      aws_lambda_event_source_mapping.kafka.starting_position == "TRIM_HORIZON"
      && aws_lambda_event_source_mapping.kafka.batch_size == 50
      && aws_lambda_event_source_mapping.kafka.maximum_batching_window_in_seconds == 5
    )
    error_message = "ESM の開始位置 / バッチサイズ / バッチウィンドウが variables から決まっていない"
  }
}
