#!/usr/bin/env bash
# lifecycle { prevent_destroy } は terraform test の後始末（destroy）では無視されるため検証できない。
# データを持つ table bucket と、その暗号鍵・namespace に付いていることを、定義のテキストで固定する。
set -euo pipefail

cd "$(dirname "$0")/.."

# resource_block TYPE NAME: main.tf の resource ブロック（インデント 0 の閉じ括弧まで）を出力する
resource_block() {
  awk -v header="resource \"$1\" \"$2\" {" '$0 == header { in_block = 1 } in_block { print } in_block && /^}/ { exit }' main.tf
}

for resource in "aws_kms_key s3tables" "aws_s3tables_table_bucket this" "aws_s3tables_namespace events"; do
  # shellcheck disable=SC2086
  block="$(resource_block ${resource})"
  if ! grep -Eq '^[[:space:]]*prevent_destroy[[:space:]]*=[[:space:]]*true$' <<<"${block}"; then
    echo "prevent_destroy = true がありません: ${resource}" >&2
    exit 1
  fi
done
