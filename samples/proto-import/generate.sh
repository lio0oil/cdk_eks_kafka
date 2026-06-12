#!/usr/bin/env bash
# import を含む .proto のコンパイル。-I で指定した protos/ が import 解決のルートになる。
#
# --python_out は渡したファイルだけ生成するため find で全件列挙する。未使用の .proto が
# 混ざっても生成物が増えるだけで害はなく、列挙漏れによる ModuleNotFoundError を防げる。
set -euo pipefail
cd "$(dirname "$0")"

mkdir -p gen
find protos -name '*.proto' -print0 | xargs -0 \
  protoc \
  -I protos \
  --python_out=gen

# consumer (Spark from_protobuf 等) が読む FileDescriptorSet はエントリポイントだけ渡す。
# --include_imports が依存 (well-known types 含む) を自動で辿って 1 ファイルにバンドル
# するため、protos/ に未参照の .proto があっても .desc には入らない。
protoc \
  -I protos \
  --include_imports \
  --descriptor_set_out=gen/events.desc \
  protos/service/event.proto
