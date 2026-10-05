data "aws_partition" "current" {}

locals {
  application_log_group_name = "/aws/eks/${var.cluster_name}/application"
}

# ── CloudWatch Logs（コンテナログ）──────────────────────────────────────────

resource "aws_cloudwatch_log_group" "application" {
  name              = local.application_log_group_name
  retention_in_days = var.log_retention_days
  skip_destroy      = var.retain_log_groups
}

# ── Alertmanager 通知配送先（SNS Topic）──────────────────────────────────────
# webhook URL / Secrets Manager / ESO を介さず、Pod Identity の sns:Publish で SNS に直接 publish する。
# subscriber（Email / AWS Chatbot 等）は通知先の運用が確定してから手動か別の変更で追加する。

resource "aws_sns_topic" "alertmanager" {
  name = "${var.cluster_name}-alertmanager"
}

# ── SNS → Lambda → CloudWatch Logs（dev のテスト用に通知本文を確認する経路）─────────
# dev では Email / Teams の subscriber を用意せず、publish された通知本文を Lambda が stdout に
# 書き出し、Lambda のロググループで確認する。stg/prd は実通知先へ配送するため作らない。

data "archive_file" "alertmanager_sns_log_forwarder" {
  count = var.enable_alertmanager_sns_log_forwarder ? 1 : 0

  type        = "zip"
  output_path = "${path.module}/alertmanager-sns-log-forwarder.zip"

  source {
    filename = "index.py"
    content  = <<-EOT
      import json


      def handler(event, context):
          for record in event["Records"]:
              print(json.dumps(record["Sns"], ensure_ascii=False))
    EOT
  }
}

resource "aws_cloudwatch_log_group" "alertmanager_sns_log_forwarder" {
  count = var.enable_alertmanager_sns_log_forwarder ? 1 : 0

  name              = "/aws/lambda/${var.cluster_name}-alertmanager-sns-log-forwarder"
  retention_in_days = var.log_retention_days
  skip_destroy      = var.retain_log_groups
}

resource "aws_iam_role" "alertmanager_sns_log_forwarder" {
  count = var.enable_alertmanager_sns_log_forwarder ? 1 : 0

  name = "${var.cluster_name}-alertmanager-sns-log-forwarder"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "lambda.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "alertmanager_sns_log_forwarder" {
  count = var.enable_alertmanager_sns_log_forwarder ? 1 : 0

  role       = aws_iam_role.alertmanager_sns_log_forwarder[0].name
  policy_arn = "arn:${data.aws_partition.current.partition}:iam::aws:policy/service-role/AWSLambdaBasicExecutionRole"
}

resource "aws_lambda_function" "alertmanager_sns_log_forwarder" {
  count = var.enable_alertmanager_sns_log_forwarder ? 1 : 0

  function_name    = "${var.cluster_name}-alertmanager-sns-log-forwarder"
  role             = aws_iam_role.alertmanager_sns_log_forwarder[0].arn
  runtime          = var.sns_log_forwarder_runtime
  handler          = "index.handler"
  timeout          = var.sns_log_forwarder_timeout_seconds
  filename         = data.archive_file.alertmanager_sns_log_forwarder[0].output_path
  source_code_hash = data.archive_file.alertmanager_sns_log_forwarder[0].output_base64sha256

  logging_config {
    log_format = "Text"
    log_group  = aws_cloudwatch_log_group.alertmanager_sns_log_forwarder[0].name
  }

  depends_on = [aws_iam_role_policy_attachment.alertmanager_sns_log_forwarder]
}

resource "aws_lambda_permission" "alertmanager_sns_invoke" {
  count = var.enable_alertmanager_sns_log_forwarder ? 1 : 0

  statement_id  = "AllowExecutionFromSNS"
  action        = "lambda:InvokeFunction"
  function_name = aws_lambda_function.alertmanager_sns_log_forwarder[0].function_name
  principal     = "sns.amazonaws.com"
  source_arn    = aws_sns_topic.alertmanager.arn
}

resource "aws_sns_topic_subscription" "alertmanager_log_forwarder" {
  count = var.enable_alertmanager_sns_log_forwarder ? 1 : 0

  topic_arn = aws_sns_topic.alertmanager.arn
  protocol  = "lambda"
  endpoint  = aws_lambda_function.alertmanager_sns_log_forwarder[0].arn

  depends_on = [aws_lambda_permission.alertmanager_sns_invoke]
}

# ── monitoring Namespace と ServiceAccount ──────────────────────────────────
# Pod Identity の紐付け先 ServiceAccount は、Namespace が存在しないと作れない。
# chart 側では ServiceAccount を作らない（create: false）ため、ここで作っておく。

resource "kubernetes_manifest" "namespace" {
  manifest = yamldecode(file("${var.manifest_dir}/namespace.yaml"))
}

resource "kubernetes_service_account_v1" "fluent_bit" {
  metadata {
    name      = "fluent-bit"
    namespace = "monitoring"
  }

  depends_on = [kubernetes_manifest.namespace]
}

resource "kubernetes_service_account_v1" "grafana" {
  metadata {
    name      = "grafana"
    namespace = "monitoring"
  }

  depends_on = [kubernetes_manifest.namespace]
}

resource "kubernetes_service_account_v1" "alertmanager" {
  metadata {
    name      = "alertmanager"
    namespace = "monitoring"
  }

  depends_on = [kubernetes_manifest.namespace]
}

# ── Pod Identity ───────────────────────────────────────────────────────────

module "fluent_bit_identity" {
  source = "../pod-identity"

  cluster_name    = var.cluster_name
  role_name       = "fluent-bit-pod-identity-${var.cluster_name}"
  namespace       = "monitoring"
  service_account = "fluent-bit"
  policy_statements = [{
    Effect = "Allow"
    Action = [
      "logs:CreateLogStream",
      "logs:PutLogEvents",
      "logs:DescribeLogGroups",
      "logs:DescribeLogStreams",
    ]
    Resource = [aws_cloudwatch_log_group.application.arn, "${aws_cloudwatch_log_group.application.arn}:*"]
  }]
}

# in-cluster Prometheus を datasource として直接 query するだけで AWS API を叩かないため、権限は付与しない
module "grafana_identity" {
  source = "../pod-identity"

  cluster_name    = var.cluster_name
  role_name       = "grafana-pod-identity-${var.cluster_name}"
  namespace       = "monitoring"
  service_account = "grafana"
}

module "alertmanager_identity" {
  source = "../pod-identity"

  cluster_name    = var.cluster_name
  role_name       = "alertmanager-pod-identity-${var.cluster_name}"
  namespace       = "monitoring"
  service_account = "alertmanager"
  policy_statements = [{
    Effect   = "Allow"
    Action   = ["sns:Publish"]
    Resource = aws_sns_topic.alertmanager.arn
  }]
}

# ── kube-prometheus-stack ──────────────────────────────────────────────────
# in-cluster Prometheus + Operator + Grafana + Alertmanager（3 replica HA、SNS receiver）を chart 同梱で
# 導入する。Grafana の datasource は chart 既定の in-cluster Prometheus をそのまま使う。

resource "helm_release" "kube_prometheus_stack" {
  name       = "kube-prometheus-stack"
  chart      = "kube-prometheus-stack"
  repository = var.kube_prometheus_stack_chart_repo
  version    = var.kube_prometheus_stack_chart_version
  namespace  = "monitoring"

  # chart は Prometheus Operator の admission webhook（PrometheusRule を検証、failurePolicy=Fail）を
  # 導入する。Pod が Ready になるまで待たないと、直後に apply する PrometheusRule が webhook 呼び出しに
  # 失敗する。Prometheus / Alertmanager の PVC は gp3 StorageClass を要求するため、StorageClass が
  # 未作成だと PVC が Pending のまま install がタイムアウトする（呼び出し側で addons module に依存させる）。
  wait    = true
  timeout = var.kube_prometheus_stack_wait_timeout_seconds

  values = [templatefile("${var.manifest_dir}/kube-prometheus-stack-values.yaml.tftpl", {
    REGION                      = var.region
    SNS_TOPIC_ARN               = aws_sns_topic.alertmanager.arn
    PROMETHEUS_RETENTION        = var.prometheus_resources.retention
    PROMETHEUS_STORAGE_SIZE     = var.prometheus_resources.storage_size
    PROMETHEUS_SCRAPE_INTERVAL  = var.prometheus_resources.scrape_interval
    PROMETHEUS_MEMORY_REQUEST   = var.prometheus_resources.memory_request
    PROMETHEUS_MEMORY_LIMIT     = var.prometheus_resources.memory_limit
    PROMETHEUS_CPU_REQUEST      = var.prometheus_resources.cpu_request
    PROMETHEUS_CPU_LIMIT        = var.prometheus_resources.cpu_limit
    PROMETHEUS_REPLICAS         = tostring(var.prometheus_replicas)
    ALERTMANAGER_MEMORY_REQUEST = var.alertmanager_resources.memory_request
    ALERTMANAGER_MEMORY_LIMIT   = var.alertmanager_resources.memory_limit
    ALERTMANAGER_CPU_REQUEST    = var.alertmanager_resources.cpu_request
    ALERTMANAGER_CPU_LIMIT      = var.alertmanager_resources.cpu_limit
    ALERTMANAGER_REPLICAS       = tostring(var.alertmanager_replicas)
    ALERTMANAGER_STORAGE_SIZE   = var.alertmanager_resources.storage_size
  })]

  depends_on = [
    kubernetes_manifest.namespace,
    kubernetes_service_account_v1.grafana,
    kubernetes_service_account_v1.alertmanager,
    module.grafana_identity,
    module.alertmanager_identity,
  ]
}

# ── Fluent Bit（ログ → CloudWatch Logs）──────────────────────────────────────

resource "helm_release" "fluent_bit" {
  name       = "fluent-bit"
  chart      = "fluent-bit"
  repository = var.fluent_bit_chart_repo
  version    = var.fluent_bit_chart_version
  namespace  = "monitoring"

  values = [templatefile("${var.manifest_dir}/fluent-bit-values.yaml.tftpl", {
    REGION                    = var.region
    LOG_GROUP_NAME            = aws_cloudwatch_log_group.application.name
    FLUENT_BIT_MEMORY_REQUEST = var.fluent_bit_resources.memory_request
    FLUENT_BIT_MEMORY_LIMIT   = var.fluent_bit_resources.memory_limit
    FLUENT_BIT_CPU_REQUEST    = var.fluent_bit_resources.cpu_request
    FLUENT_BIT_CPU_LIMIT      = var.fluent_bit_resources.cpu_limit
    FLUENT_BIT_MEM_BUF_LIMIT  = var.fluent_bit_resources.mem_buf_limit
  })]

  depends_on = [
    kubernetes_service_account_v1.fluent_bit,
    module.fluent_bit_identity,
  ]
}
