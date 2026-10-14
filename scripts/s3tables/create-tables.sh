#!/usr/bin/env bash
# tables/<テーブル名>.json の定義で、まだ無いテーブルだけを作る。既にあるテーブルには触れない
# （作成後のスキーマ進化は Spark の ALTER TABLE で行うため、この定義で上書きしない）。
#
# table bucket と namespace は Terraform（infra）が作る。値はその output から渡す:
#   scripts/s3tables/create-tables.sh \
#     "$(terraform -chdir=terraform/env/dev/infra output -raw table_bucket_arn)" \
#     "$(terraform -chdir=terraform/env/dev/infra output -raw namespace_name)"
set -euo pipefail

if (($# != 2)); then
  echo "使い方: $0 <table-bucket-arn> <namespace>" >&2
  exit 1
fi
table_bucket_arn="$1"
namespace="$2"

script_dir="$(cd "$(dirname "$0")" && pwd)"

for definition in "${script_dir}"/tables/*.json; do
  name="$(basename "${definition}" .json)"

  if error="$(aws s3tables get-table --table-bucket-arn "${table_bucket_arn}" --namespace "${namespace}" --name "${name}" 2>&1 >/dev/null)"; then
    echo "skip: ${name}（作成済み）"
  elif grep -q NotFoundException <<<"${error}"; then
    aws s3tables create-table \
      --table-bucket-arn "${table_bucket_arn}" \
      --namespace "${namespace}" \
      --name "${name}" \
      --format ICEBERG \
      --metadata "file://${definition}" >/dev/null
    echo "create: ${name}"
  else
    echo "${error}" >&2
    exit 1
  fi
done
