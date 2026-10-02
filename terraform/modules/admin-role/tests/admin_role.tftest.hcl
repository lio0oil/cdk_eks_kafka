mock_provider "aws" {
  mock_data "aws_caller_identity" {
    defaults = {
      account_id = "111111111111"
    }
  }

  mock_data "aws_partition" {
    defaults = {
      partition = "aws"
    }
  }

  mock_resource "aws_iam_role" {
    defaults = {
      arn = "arn:aws:iam::111111111111:role/eks-cluster-admin"
    }
  }
}

variables {
  role_name = "eks-cluster-admin-eks-cluster-dev"
}

run "role_uses_given_name" {
  command = apply

  assert {
    condition     = aws_iam_role.admin.name == "eks-cluster-admin-eks-cluster-dev"
    error_message = "Role 名が入力と一致しない"
  }
}

# 本番では運用者・CI/CD の Role を ARN で明示列挙する（TODO.md 参照）。現状は CDK と同じく
# 同一アカウントの全 IAM Principal が AssumeRole できる。
run "any_principal_in_the_account_can_assume_the_role" {
  command = apply

  assert {
    condition = (
      jsondecode(aws_iam_role.admin.assume_role_policy).Statement[0].Principal.AWS == "arn:aws:iam::111111111111:root"
      && jsondecode(aws_iam_role.admin.assume_role_policy).Statement[0].Action == "sts:AssumeRole"
    )
    error_message = "信頼先が同一アカウントの root になっていない"
  }
}

run "output_exposes_role_arn" {
  command = apply

  assert {
    condition     = output.role_arn == aws_iam_role.admin.arn
    error_message = "role_arn 出力が Role の ARN を指していない"
  }
}
