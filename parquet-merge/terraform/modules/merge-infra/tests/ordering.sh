#!/usr/bin/env bash
# terraform test は depends_on を検証できないため、依存グラフで作成順序を固定する。
set -euo pipefail

cd "$(dirname "$0")/.."
source ../../scripts/graph.sh

# public access block と同時に適用すると OperationAborted で競合する
expect_dependency aws_s3_bucket_policy.this aws_s3_bucket_public_access_block.this
