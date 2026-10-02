# SSH 鍵運用は廃止し、AmazonSSMManagedInstanceCore による Session Manager のみで接続する。
# インバウンドは一切開けず、Session Manager の outbound 経路に依存する。

resource "aws_iam_role" "this" {
  name_prefix = "test-ec2-"
  assume_role_policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect    = "Allow"
      Principal = { Service = "ec2.amazonaws.com" }
      Action    = "sts:AssumeRole"
    }]
  })
}

resource "aws_iam_role_policy_attachment" "ssm" {
  role       = aws_iam_role.this.name
  policy_arn = "arn:aws:iam::aws:policy/AmazonSSMManagedInstanceCore"
}

resource "aws_iam_instance_profile" "this" {
  name_prefix = "test-ec2-"
  role        = aws_iam_role.this.name
}

# SSH 不可の方針のため scp / rsync は使わず、S3 を中継してローカルと EC2 の間で双方向に転送する。
module "transfer_bucket" {
  source = "../secure-bucket"

  bucket_prefix = "test-ec2-transfer-"
  force_destroy = true
}

resource "aws_iam_role_policy" "transfer" {
  name = "transfer-bucket"
  role = aws_iam_role.this.id
  policy = jsonencode({
    Version = "2012-10-17"
    Statement = [{
      Effect = "Allow"
      Action = [
        "s3:GetObject*",
        "s3:GetBucket*",
        "s3:List*",
        "s3:DeleteObject*",
        "s3:PutObject",
        "s3:PutObjectLegalHold",
        "s3:PutObjectRetention",
        "s3:PutObjectTagging",
        "s3:PutObjectVersionTagging",
        "s3:Abort*",
      ]
      Resource = [module.transfer_bucket.arn, "${module.transfer_bucket.arn}/*"]
    }]
  })
}

resource "aws_security_group" "this" {
  name_prefix = "test-ec2-"
  description = "Test EC2 (Session Manager only, no inbound)."
  vpc_id      = var.vpc_id
}

resource "aws_vpc_security_group_egress_rule" "all" {
  security_group_id = aws_security_group.this.id
  description       = "Allow all outbound traffic by default"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "-1"
}

data "aws_ssm_parameter" "al2023_arm64" {
  name = "/aws/service/ami-amazon-linux-latest/al2023-ami-kernel-6.1-arm64"
}

resource "aws_instance" "this" {
  ami                         = data.aws_ssm_parameter.al2023_arm64.value
  instance_type               = var.instance_type
  subnet_id                   = var.subnet_id
  iam_instance_profile        = aws_iam_instance_profile.this.name
  vpc_security_group_ids      = [aws_security_group.this.id]
  associate_public_ip_address = false

  metadata_options {
    http_tokens = "required"
  }

  user_data = <<-EOT
    #!/bin/bash
    set -eux
    dnf install -y jq tar gzip git unzip bash-completion
    ARCH="$(uname -m)"
    case "$ARCH" in aarch64) K8S_ARCH=arm64 ;; x86_64) K8S_ARCH=amd64 ;; esac
    curl -fsSL -o /usr/local/bin/kubectl "https://dl.k8s.io/release/v${var.kubernetes_version}.0/bin/linux/$${K8S_ARCH}/kubectl"
    chmod +x /usr/local/bin/kubectl
    curl -fsSL https://raw.githubusercontent.com/helm/helm/main/scripts/get-helm-3 | bash
  EOT

  tags = {
    Name = "test-ec2"
  }

  # 最新 AMI の更新のたびに踏み台を作り直さない（検証用で、作り直したいときは taint する）
  lifecycle {
    ignore_changes = [ami]
  }
}
