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

  mock_resource "aws_sns_topic" {
    defaults = {
      arn = "arn:aws:sns:ap-northeast-1:111111111111:eks-cluster-dev-alertmanager"
    }
  }

  mock_resource "aws_cloudwatch_log_group" {
    defaults = {
      arn = "arn:aws:logs:ap-northeast-1:111111111111:log-group:/aws/eks/eks-cluster-dev/application"
    }
  }

  mock_resource "aws_lambda_function" {
    defaults = {
      arn = "arn:aws:lambda:ap-northeast-1:111111111111:function:mock"
    }
  }
}

mock_provider "archive" {}
mock_provider "helm" {}
mock_provider "kubernetes" {}

variables {
  cluster_name                               = "eks-cluster-dev"
  region                                     = "ap-northeast-1"
  manifest_dir                               = "../../manifests/monitoring"
  kube_prometheus_stack_chart_version        = "84.5.0"
  kube_prometheus_stack_chart_repo           = "https://prometheus-community.github.io/helm-charts"
  fluent_bit_chart_version                   = "0.57.3"
  fluent_bit_chart_repo                      = "https://fluent.github.io/helm-charts"
  log_retention_days                         = 7
  kube_prometheus_stack_wait_timeout_seconds = 900
  sns_log_forwarder_runtime                  = "python3.13"
  sns_log_forwarder_timeout_seconds          = 30
  retain_log_groups                          = false
  enable_alertmanager_sns_log_forwarder      = true
  prometheus_replicas                        = 2
  alertmanager_replicas                      = 3
  prometheus_resources = {
    memory_request  = "512Mi"
    memory_limit    = "1Gi"
    cpu_request     = "250m"
    cpu_limit       = "500m"
    storage_size    = "20Gi"
    retention       = "15d"
    scrape_interval = "30s"
  }
  alertmanager_resources = {
    memory_request = "64Mi"
    memory_limit   = "128Mi"
    cpu_request    = "50m"
    cpu_limit      = "100m"
    storage_size   = "1Gi"
  }
  fluent_bit_resources = {
    memory_request = "128Mi"
    memory_limit   = "256Mi"
    cpu_request    = "50m"
    cpu_limit      = "200m"
    mem_buf_limit  = "5MB"
  }
}

run "container_logs_go_to_a_named_log_group" {
  command = apply

  assert {
    condition = (
      aws_cloudwatch_log_group.application.name == "/aws/eks/eks-cluster-dev/application"
      && aws_cloudwatch_log_group.application.retention_in_days == 7
      && !aws_cloudwatch_log_group.application.skip_destroy
    )
    error_message = "コンテナログ用ロググループの名前 / 保持日数 / 削除時の扱いが期待と異なる"
  }
}

run "stg_prd_keep_log_groups_on_destroy" {
  command = apply

  variables {
    retain_log_groups  = true
    log_retention_days = 30
  }

  assert {
    condition     = aws_cloudwatch_log_group.application.skip_destroy && aws_cloudwatch_log_group.application.retention_in_days == 30
    error_message = "retain_log_groups=true でロググループが destroy 時に残る設定になっていない"
  }
}

run "monitoring_namespace_and_service_accounts" {
  command = apply

  assert {
    condition     = kubernetes_manifest.namespace.manifest.metadata.name == "monitoring"
    error_message = "monitoring Namespace が適用されていない"
  }

  assert {
    condition = alltrue([
      for sa in [kubernetes_service_account_v1.fluent_bit, kubernetes_service_account_v1.grafana, kubernetes_service_account_v1.alertmanager] :
      sa.metadata[0].namespace == "monitoring"
      ]) && (
      kubernetes_service_account_v1.fluent_bit.metadata[0].name == "fluent-bit"
      && kubernetes_service_account_v1.grafana.metadata[0].name == "grafana"
      && kubernetes_service_account_v1.alertmanager.metadata[0].name == "alertmanager"
    )
    error_message = "fluent-bit / grafana / alertmanager の ServiceAccount が monitoring namespace に作られていない"
  }
}

# Pod Identity なら AWS SDK の認証チェーンが自動でクレデンシャルを取得するため、
# webhook URL などの平文シークレットを管理せず、Topic ARN に限定した sns:Publish だけを付与する
run "alertmanager_can_only_publish_to_its_own_topic" {
  command = apply

  assert {
    condition = (
      jsondecode(module.alertmanager_identity.policy_json).Statement[0].Action == ["sns:Publish"]
      && jsondecode(module.alertmanager_identity.policy_json).Statement[0].Resource == "arn:aws:sns:ap-northeast-1:111111111111:eks-cluster-dev-alertmanager"
    )
    error_message = "Alertmanager の権限が自身の SNS Topic への sns:Publish だけになっていない"
  }

  assert {
    condition     = aws_sns_topic.alertmanager.name == "eks-cluster-dev-alertmanager"
    error_message = "SNS Topic の名前が <cluster>-alertmanager ではない"
  }
}

run "fluent_bit_can_only_write_to_the_application_log_group" {
  command = apply

  assert {
    condition = (
      toset(jsondecode(module.fluent_bit_identity.policy_json).Statement[0].Action) == toset([
        "logs:CreateLogStream",
        "logs:PutLogEvents",
        "logs:DescribeLogGroups",
        "logs:DescribeLogStreams",
      ])
      && toset(jsondecode(module.fluent_bit_identity.policy_json).Statement[0].Resource) == toset([
        "arn:aws:logs:ap-northeast-1:111111111111:log-group:/aws/eks/eks-cluster-dev/application",
        "arn:aws:logs:ap-northeast-1:111111111111:log-group:/aws/eks/eks-cluster-dev/application:*",
      ])
    )
    error_message = "Fluent Bit の権限がアプリケーションログ用ロググループへの書き込みだけになっていない"
  }
}

# in-cluster Prometheus を直接 query するだけで AWS API を叩かないため、Role は作るが権限は付与しない
run "grafana_gets_no_aws_permissions" {
  command = apply

  assert {
    condition     = module.grafana_identity.policy_json == null
    error_message = "Grafana に AWS 権限が付与されている"
  }
}

run "kube_prometheus_stack_is_installed_and_waits_for_ready" {
  command = apply

  assert {
    condition = (
      helm_release.kube_prometheus_stack.chart == "kube-prometheus-stack"
      && helm_release.kube_prometheus_stack.repository == "https://prometheus-community.github.io/helm-charts"
      && helm_release.kube_prometheus_stack.version == "84.5.0"
      && helm_release.kube_prometheus_stack.namespace == "monitoring"
    )
    error_message = "kube-prometheus-stack の chart / リポジトリ / バージョン / namespace が期待と異なる"
  }

  # chart は PrometheusRule を検証する admission webhook（failurePolicy=Fail）を導入する。
  # Ready まで待たないと、直後に apply する PrometheusRule が webhook 呼び出しに失敗する
  assert {
    condition     = helm_release.kube_prometheus_stack.wait && helm_release.kube_prometheus_stack.timeout == 900
    error_message = "kube-prometheus-stack が Pod の Ready を 15 分まで待つ設定になっていない"
  }
}

run "prometheus_and_alertmanager_get_their_settings" {
  command = apply

  assert {
    condition = (
      yamldecode(helm_release.kube_prometheus_stack.values[0]).prometheus.prometheusSpec.replicas == 2
      && yamldecode(helm_release.kube_prometheus_stack.values[0]).prometheus.prometheusSpec.retention == "15d"
      && yamldecode(helm_release.kube_prometheus_stack.values[0]).prometheus.prometheusSpec.scrapeInterval == "30s"
      && yamldecode(helm_release.kube_prometheus_stack.values[0]).prometheus.prometheusSpec.resources.limits.memory == "1Gi"
    )
    error_message = "Prometheus の replicas / retention / scrapeInterval / resources が反映されていない"
  }

  assert {
    condition = (
      yamldecode(helm_release.kube_prometheus_stack.values[0]).alertmanager.alertmanagerSpec.replicas == 3
      && yamldecode(helm_release.kube_prometheus_stack.values[0]).alertmanager.alertmanagerSpec.resources.requests.memory == "64Mi"
    )
    error_message = "Alertmanager の replicas / resources が反映されていない"
  }

  assert {
    condition = (
      yamldecode(helm_release.kube_prometheus_stack.values[0]).alertmanager.serviceAccount.create == false
      && yamldecode(helm_release.kube_prometheus_stack.values[0]).alertmanager.serviceAccount.name == "alertmanager"
      && yamldecode(helm_release.kube_prometheus_stack.values[0]).grafana.serviceAccount.name == "grafana"
    )
    error_message = "chart が ServiceAccount を作らず、Pod Identity の紐付け先を使う設定になっていない"
  }
}

run "alertmanager_notifies_through_the_sns_topic_with_sigv4" {
  command = apply

  assert {
    condition = (
      yamldecode(helm_release.kube_prometheus_stack.values[0]).alertmanager.config.receivers[1].name == "default-sns"
      && yamldecode(helm_release.kube_prometheus_stack.values[0]).alertmanager.config.receivers[1].sns_configs[0].topic_arn == "arn:aws:sns:ap-northeast-1:111111111111:eks-cluster-dev-alertmanager"
      && yamldecode(helm_release.kube_prometheus_stack.values[0]).alertmanager.config.receivers[1].sns_configs[0].sigv4.region == "ap-northeast-1"
    )
    error_message = "Alertmanager の receiver が SNS Topic へ sigv4 で送る設定になっていない"
  }
}

run "fluent_bit_ships_logs_to_the_application_log_group" {
  command = apply

  assert {
    condition = (
      helm_release.fluent_bit.chart == "fluent-bit"
      && helm_release.fluent_bit.repository == "https://fluent.github.io/helm-charts"
      && helm_release.fluent_bit.version == "0.57.3"
      && helm_release.fluent_bit.namespace == "monitoring"
    )
    error_message = "Fluent Bit の chart / リポジトリ / バージョン / namespace が期待と異なる"
  }

  assert {
    condition = (
      strcontains(yamldecode(helm_release.fluent_bit.values[0]).config.outputs, "log_group_name    /aws/eks/eks-cluster-dev/application")
      && strcontains(yamldecode(helm_release.fluent_bit.values[0]).config.outputs, "region            ap-northeast-1")
      && strcontains(yamldecode(helm_release.fluent_bit.values[0]).config.inputs, "Mem_Buf_Limit     5MB")
      && yamldecode(helm_release.fluent_bit.values[0]).resources.limits.memory == "256Mi"
    )
    error_message = "Fluent Bit の送り先ロググループ / リージョン / バッファ上限 / resources が反映されていない"
  }
}

run "sns_log_forwarder_makes_notifications_visible_in_dev" {
  command = apply

  assert {
    condition = (
      aws_lambda_function.alertmanager_sns_log_forwarder[0].runtime == "python3.13"
      && aws_lambda_function.alertmanager_sns_log_forwarder[0].handler == "index.handler"
      && aws_lambda_function.alertmanager_sns_log_forwarder[0].timeout == 30
    )
    error_message = "ログ転送 Lambda の runtime / handler / timeout が期待と異なる"
  }

  assert {
    condition = (
      aws_sns_topic_subscription.alertmanager_log_forwarder[0].protocol == "lambda"
      && aws_sns_topic_subscription.alertmanager_log_forwarder[0].topic_arn == aws_sns_topic.alertmanager.arn
      && aws_sns_topic_subscription.alertmanager_log_forwarder[0].endpoint == aws_lambda_function.alertmanager_sns_log_forwarder[0].arn
    )
    error_message = "SNS Topic が Lambda を購読する設定になっていない"
  }

  assert {
    condition = (
      aws_lambda_permission.alertmanager_sns_invoke[0].principal == "sns.amazonaws.com"
      && aws_lambda_permission.alertmanager_sns_invoke[0].action == "lambda:InvokeFunction"
      && aws_lambda_permission.alertmanager_sns_invoke[0].source_arn == aws_sns_topic.alertmanager.arn
    )
    error_message = "SNS からの Lambda 呼び出しが Topic に限定して許可されていない"
  }
}

# stg/prd は実通知先（Email / Chatbot 等）に配送するため、検証用の Lambda は作らない
run "sns_log_forwarder_is_absent_when_disabled" {
  command = apply

  variables {
    enable_alertmanager_sns_log_forwarder = false
  }

  assert {
    condition = (
      length(aws_lambda_function.alertmanager_sns_log_forwarder) == 0
      && length(aws_sns_topic_subscription.alertmanager_log_forwarder) == 0
      && length(aws_lambda_permission.alertmanager_sns_invoke) == 0
    )
    error_message = "無効にしても検証用 Lambda / 購読 / 許可が作られている"
  }
}

# Helm の Ready 待ちの上限と、dev のログ転送 Lambda の runtime / timeout は、module に値を持たず呼び出し側が決める
run "monitoring_settings_come_from_variables" {
  command = apply

  variables {
    kube_prometheus_stack_wait_timeout_seconds = 1200
    sns_log_forwarder_runtime                  = "python3.12"
    sns_log_forwarder_timeout_seconds          = 60
  }

  assert {
    condition     = helm_release.kube_prometheus_stack.timeout == 1200
    error_message = "kube-prometheus-stack の Ready 待ちの上限が variables から決まっていない"
  }

  assert {
    condition = (
      aws_lambda_function.alertmanager_sns_log_forwarder[0].runtime == "python3.12"
      && aws_lambda_function.alertmanager_sns_log_forwarder[0].timeout == 60
    )
    error_message = "ログ転送 Lambda の runtime / timeout が variables から決まっていない"
  }
}
