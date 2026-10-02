# self-managed Kafka の Event Source Mapping（ESM）のポーラー ENI 用 SG。
# Kafka NLB と AWS の HTTPS API 以外へは送信できない（SG を作ると既定の全許可 egress は外れる）。
#
# ESM が動くために必須の通信の最低限を、基盤側（infra）が用意する。これは下限であり、app が他の
# 接続先と通信する場合は、app 側で別の SG を作って ESM に追加する（SG は許可の和集合）。
# なお SG では上限を強制できない（app が広い許可の SG を追加できる）。強制するなら IAM 側で制御する。
resource "aws_security_group" "esm" {
  name        = "${var.cluster_name}-kafka-esm"
  description = "Self-managed Kafka ESM poller ENI (outbound to Kafka NLB and AWS HTTPS APIs only)."
  vpc_id      = var.vpc_id
}

# bootstrap 接続後、Kafka クライアントは各 partition のリーダー broker（advertised port は broker ごとに
# 異なる）にも直接接続するため、単一 port には絞れない。宛先を Kafka NLB の SG 参照に限定することで、
# port 番号を broker_count に追従させずに「NLB 以外への送信不可」を維持する。
resource "aws_vpc_security_group_egress_rule" "to_kafka_nlb" {
  security_group_id            = aws_security_group.esm.id
  description                  = "Kafka NLB"
  referenced_security_group_id = var.kafka_nlb_security_group_id
  ip_protocol                  = "tcp"
  from_port                    = 0
  to_port                      = 65535
}

# ESM のポーラー ENI は Kafka への到達性に加え、Lambda invoke API と STS への到達性も必須
# （self-managed Kafka の VPC アクセス要件）。欠けると ESM が Kafka に接続できなくなる。
# NAT Gateway 越しに到達させる想定で、Lambda / STS には宛先を絞れるプレフィックスリストが無いため
# 0.0.0.0/0:443 とする。
resource "aws_vpc_security_group_egress_rule" "https_to_aws_apis" {
  security_group_id = aws_security_group.esm.id
  description       = "Lambda invoke API and STS"
  cidr_ipv4         = "0.0.0.0/0"
  ip_protocol       = "tcp"
  from_port         = 443
  to_port           = 443
}

# Lambda 実行時の設定・データの読み取り用。読み取りの許可は、アプリ側が Lambda の実行ロールに対して行う。
module "data_bucket" {
  source = "../secure-bucket"

  bucket_prefix = "kafka-consumer-data-"
  force_destroy = var.force_destroy
}

# Lambda デプロイパッケージ（zip）の格納先。バージョニングを有効にし、アプリ側は特定の VersionId を
# 明示参照する（S3Key だけではコードの変更を検知できないため）。
module "artifact_bucket" {
  source = "../secure-bucket"

  bucket_prefix      = "kafka-consumer-artifact-"
  force_destroy      = var.force_destroy
  versioning_enabled = true
}
