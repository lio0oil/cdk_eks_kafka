mock_provider "aws" {
  mock_data "aws_ssm_parameter" {
    defaults = {
      value = "ami-0123456789abcdef0"
    }
  }

  mock_resource "aws_iam_role" {
    defaults = {
      arn = "arn:aws:iam::111111111111:role/mock"
    }
  }

  mock_resource "aws_s3_bucket" {
    defaults = {
      arn = "arn:aws:s3:::mock-transfer-bucket"
    }
  }
}

variables {
  vpc_id             = "vpc-0123456789abcdef0"
  subnet_id          = "subnet-aaaa"
  kubernetes_version = "1.35"
  instance_type      = "t4g.small"
}

# SSH 鍵運用は廃止し、Session Manager のみで接続する
run "instance_is_reachable_only_through_session_manager" {
  command = apply

  assert {
    condition = (
      jsondecode(aws_iam_role.this.assume_role_policy).Statement[0].Principal.Service == "ec2.amazonaws.com"
      && aws_iam_role_policy_attachment.ssm.policy_arn == "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
    )
    error_message = "EC2 が AmazonSSMManagedInstanceCore で Session Manager 接続できる Role になっていない"
  }

  # インバウンドは一切開けず、Session Manager の outbound 経路に依存する
  assert {
    condition     = length(aws_security_group.this.ingress) == 0 && aws_security_group.this.vpc_id == "vpc-0123456789abcdef0"
    error_message = "踏み台の SG がインバウンドを開けている"
  }

  assert {
    condition = (
      aws_vpc_security_group_egress_rule.all.cidr_ipv4 == "0.0.0.0/0"
      && aws_vpc_security_group_egress_rule.all.ip_protocol == "-1"
    )
    error_message = "踏み台の送信（SSM / パッケージ取得）が許可されていない"
  }
}

run "instance_runs_in_the_private_subnet_with_imdsv2" {
  command = apply

  assert {
    condition = (
      aws_instance.this.instance_type == var.instance_type
      && aws_instance.this.ami == "ami-0123456789abcdef0"
      && aws_instance.this.subnet_id == "subnet-aaaa"
      && !aws_instance.this.associate_public_ip_address
      && aws_instance.this.iam_instance_profile == aws_iam_instance_profile.this.name
      && aws_instance.this.vpc_security_group_ids == toset([aws_security_group.this.id])
    )
    error_message = "踏み台の instance type / AMI / subnet / プロファイル / SG が期待と異なる"
  }

  assert {
    condition     = one(aws_instance.this.metadata_options).http_tokens == "required"
    error_message = "IMDSv2 が必須になっていない"
  }
}

run "user_data_installs_kubectl_matching_the_cluster_and_helm" {
  command = apply

  assert {
    condition = (
      strcontains(aws_instance.this.user_data, "https://dl.k8s.io/release/v1.35.0/bin/linux/$${K8S_ARCH}/kubectl")
      && strcontains(aws_instance.this.user_data, "get-helm-3")
      && strcontains(aws_instance.this.user_data, "dnf install -y jq tar gzip git unzip bash-completion")
    )
    error_message = "EKS と同じ 1.35 系の kubectl / helm / 基本ツールが user data で導入されていない"
  }
}

# SSH 不可方針のため scp / rsync は使わず、S3 を中継してローカルと EC2 の間で双方向に転送する
run "role_can_only_read_and_write_the_transfer_bucket" {
  command = apply

  assert {
    condition = (
      toset(jsondecode(aws_iam_role_policy.transfer.policy).Statement[0].Resource) == toset(["arn:aws:s3:::mock-transfer-bucket", "arn:aws:s3:::mock-transfer-bucket/*"])
      && toset(jsondecode(aws_iam_role_policy.transfer.policy).Statement[0].Action) == toset([
        "s3:GetObject*", "s3:GetBucket*", "s3:List*", "s3:DeleteObject*", "s3:PutObject",
        "s3:PutObjectLegalHold", "s3:PutObjectRetention", "s3:PutObjectTagging", "s3:PutObjectVersionTagging", "s3:Abort*",
      ])
    )
    error_message = "転送用バケットへの読み書きだけに限定されていない"
  }
}

run "outputs_tell_how_to_connect" {
  command = apply

  assert {
    condition     = output.start_session_command == "aws ssm start-session --target ${aws_instance.this.id}"
    error_message = "start_session_command が接続コマンドになっていない"
  }

  assert {
    condition     = output.instance_id == aws_instance.this.id && output.transfer_bucket_name != null
    error_message = "instance_id / transfer_bucket_name が出力されていない"
  }
}

run "instance_type_comes_from_variables" {
  command = apply

  variables {
    instance_type = "t4g.medium"
  }

  assert {
    condition     = aws_instance.this.instance_type == "t4g.medium"
    error_message = "踏み台の instance type が variables から決まっていない"
  }
}
