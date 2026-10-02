mock_provider "aws" {
  mock_resource "aws_iam_role" {
    defaults = {
      arn = "arn:aws:iam::111111111111:role/mock"
    }
  }
}

variables {
  cluster_name    = "eks-cluster-dev"
  role_name       = "alertmanager-pod-identity-eks-cluster-dev"
  namespace       = "monitoring"
  service_account = "alertmanager"
  policy_statements = [{
    Effect   = "Allow"
    Action   = ["sns:Publish"]
    Resource = "arn:aws:sns:ap-northeast-1:111111111111:topic"
  }]
}

run "role_is_assumed_by_eks_pods_with_session_tags" {
  command = apply

  assert {
    condition     = aws_iam_role.this.name == "alertmanager-pod-identity-eks-cluster-dev"
    error_message = "Role 名が入力と一致しない"
  }

  # Pod Identity は AssumeRole に加えて TagSession が必要
  assert {
    condition = (
      jsondecode(aws_iam_role.this.assume_role_policy).Statement[0].Principal.Service == "pods.eks.amazonaws.com"
      && toset(jsondecode(aws_iam_role.this.assume_role_policy).Statement[0].Action) == toset(["sts:AssumeRole", "sts:TagSession"])
    )
    error_message = "信頼先が pods.eks.amazonaws.com で AssumeRole + TagSession を許可していない"
  }
}

run "role_is_associated_with_the_service_account" {
  command = apply

  assert {
    condition = (
      aws_eks_pod_identity_association.this.cluster_name == "eks-cluster-dev"
      && aws_eks_pod_identity_association.this.namespace == "monitoring"
      && aws_eks_pod_identity_association.this.service_account == "alertmanager"
      && aws_eks_pod_identity_association.this.role_arn == aws_iam_role.this.arn
    )
    error_message = "ServiceAccount と Role の Pod Identity 紐付けが期待と異なる"
  }

  assert {
    condition     = output.role_arn == aws_iam_role.this.arn
    error_message = "role_arn 出力が Role の ARN を指していない"
  }
}

run "given_statements_become_the_inline_policy" {
  command = apply

  assert {
    condition     = length(aws_iam_role_policy.this) == 1
    error_message = "Statement を渡したのにインラインポリシーが作られていない"
  }

  assert {
    condition = (
      jsondecode(aws_iam_role_policy.this[0].policy).Version == "2012-10-17"
      && jsondecode(aws_iam_role_policy.this[0].policy).Statement[0].Action[0] == "sns:Publish"
      && jsondecode(aws_iam_role_policy.this[0].policy).Statement[0].Resource == "arn:aws:sns:ap-northeast-1:111111111111:topic"
    )
    error_message = "インラインポリシーの内容が入力の Statement と一致しない"
  }

  # 呼び出し側が最小権限（対象リソースの限定など）を検証できるよう、ポリシーを出力する
  assert {
    condition     = output.policy_json == aws_iam_role_policy.this[0].policy
    error_message = "policy_json 出力がインラインポリシーを指していない"
  }
}

# AWS API を叩かないワークロード（Grafana など）は Role だけ用意し、権限は付与しない
run "no_statements_means_no_policy" {
  command = apply

  variables {
    policy_statements = []
  }

  assert {
    condition     = length(aws_iam_role_policy.this) == 0
    error_message = "Statement が空なのにインラインポリシーが作られている"
  }

  assert {
    condition     = output.policy_json == null
    error_message = "ポリシーが無いとき policy_json は null になるべき"
  }
}
