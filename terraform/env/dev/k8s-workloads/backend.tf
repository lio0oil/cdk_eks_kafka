# state は S3 に保存する。バケットは Terraform の管理外で先に作成してある（自分自身の state を入れる
# バケットは、その state では作れないため）。backend ブロックには変数を使えないので、値は直接書く。
# ロックは S3 のネイティブロック（use_lockfile）で行うため、DynamoDB は不要。
terraform {
  backend "s3" {
    bucket       = "ekscdk-tfstate-9025850b"
    key          = "dev/k8s-workloads.tfstate"
    region       = "ap-northeast-1"
    encrypt      = true
    use_lockfile = true
  }
}
