# CloudWatch Logs より保存コストが大幅に安く、Athena でクエリできる S3 を送り先にする。
#
# Flow Logs の作成時に AWS が配信許可をバケットポリシーへ自動追記するが、Terraform 管理のポリシーが
# 次回 apply でそれを消すと配信が止まる。AWS ドキュメントの必須ステートメントを明示的に宣言して、
# 自動追記との食い違い（drift）を避ける。
module "flow_log_bucket" {
  count  = var.enable_vpc_flow_logs ? 1 : 0
  source = "../secure-bucket"

  bucket_prefix = "${var.cluster_name}-vpc-flow-log-"
  force_destroy = var.log_force_destroy

  additional_statements = [
    {
      Sid             = "AWSLogDeliveryWrite"
      Effect          = "Allow"
      Principal       = { Service = "delivery.logs.amazonaws.com" }
      Action          = "s3:PutObject"
      resource_suffix = "/AWSLogs/${data.aws_caller_identity.current.account_id}/*"
      Condition = {
        StringEquals = {
          "s3:x-amz-acl"      = "bucket-owner-full-control"
          "aws:SourceAccount" = data.aws_caller_identity.current.account_id
        }
        ArnLike = {
          "aws:SourceArn" = "arn:aws:logs:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:*"
        }
      }
    },
    {
      Sid             = "AWSLogDeliveryAclCheck"
      Effect          = "Allow"
      Principal       = { Service = "delivery.logs.amazonaws.com" }
      Action          = "s3:GetBucketAcl"
      resource_suffix = ""
      Condition = {
        StringEquals = {
          "aws:SourceAccount" = data.aws_caller_identity.current.account_id
        }
        ArnLike = {
          "aws:SourceArn" = "arn:aws:logs:${data.aws_region.current.region}:${data.aws_caller_identity.current.account_id}:*"
        }
      }
    },
  ]
}

# rejected だけでは不審なドロップを見逃し、accepted だけでは既知通信に埋もれるため ALL を記録する。
resource "aws_flow_log" "vpc" {
  count = var.enable_vpc_flow_logs ? 1 : 0

  vpc_id               = module.vpc.vpc_id
  traffic_type         = "ALL"
  log_destination_type = "s3"
  log_destination      = module.flow_log_bucket[0].arn

  # バケットポリシーの配信許可が確定してから作る
  depends_on = [module.flow_log_bucket]
}
