#!/usr/bin/env bash
# テーブル定義 JSON が、consumer の想定するスキーマ・partition と、CreateTable API の入力形式を満たすことを検証する。
set -euo pipefail

cd "$(dirname "$0")/.."
tables_dir="tables"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

expect_json() {
  local description="$1" file="$2" filter="$3" expected="$4" actual
  actual="$(jq -cS "${filter}" "${file}")"
  [[ "${actual}" == "$(jq -cS . <<<"${expected}")" ]] || fail "${description}: 期待=${expected} 実際=${actual}"
}

event="${tables_dir}/sample_events_event.json"
dlq="${tables_dir}/sample_events_dlq.json"

actual_files="$(cd "${tables_dir}" && printf '%s\n' *.json | sort | tr '\n' ' ')"
[[ "${actual_files}" == "sample_events_dlq.json sample_events_event.json " ]] || fail "テーブル定義の一覧が想定と異なる: ${actual_files}"

# Envelope をそのまま rawdata に保存し、extra_type に oneof case 名を入れる
expect_json "event の列" "${event}" '.iceberg.schema.fields' '[
  {"id":1,"name":"event_id","type":"long","required":true},
  {"id":2,"name":"event_datetime","type":"timestamp","required":true},
  {"id":3,"name":"extra_type","type":"string","required":true},
  {"id":4,"name":"rawdata","type":"binary","required":true}
]'

# 低カーディナリティで等値フィルタに使う identity(extra_type) を先頭に、続けて day(event_datetime)
expect_json "event の partition" "${event}" '.iceberg.partitionSpec.fields' '[
  {"sourceId":3,"transform":"identity","name":"extra_type","fieldId":1000},
  {"sourceId":2,"transform":"day","name":"event_datetime_day","fieldId":1001}
]'

# schema_name は header に proto-schema が無い失敗（missing_schema）で NULL になるため任意
expect_json "dlq の列" "${dlq}" '.iceberg.schema.fields' '[
  {"id":1,"name":"failed_at","type":"timestamp","required":true},
  {"id":2,"name":"schema_name","type":"string","required":false},
  {"id":3,"name":"rawdata","type":"binary","required":true},
  {"id":4,"name":"reason","type":"string","required":true}
]'

expect_json "dlq の partition" "${dlq}" '.iceberg.partitionSpec.fields' '[
  {"sourceId":2,"transform":"identity","name":"schema_name","fieldId":1000},
  {"sourceId":1,"transform":"day","name":"failed_at_day","fieldId":1001}
]'

for table in "${event}" "${dlq}"; do
  expect_json "${table} の write モード" "${table}" '.iceberg.properties' '{
    "write.delete.mode":"copy-on-write",
    "write.merge.mode":"copy-on-write",
    "write.update.mode":"copy-on-write"
  }'

  # partition の sourceId は schema の列 id を指し、partition の fieldId は列 id と衝突しない 1000 番台で重複しない
  expect_json "${table} の partition 参照" "${table}" \
    '(.iceberg.schema.fields | map(.id)) as $ids | .iceberg.partitionSpec.fields | all(.sourceId as $s | $ids | index($s) != null)' \
    'true'
  expect_json "${table} の partition fieldId" "${table}" \
    '.iceberg.partitionSpec.fields | (map(.fieldId) | unique | length) == length and all(.fieldId >= 1000)' \
    'true'
done

# aws CLI のパラメータ検証を通ること。到達できない endpoint を指定し、検証を通過した後の接続失敗で止める
# （検証に失敗すると "Parameter validation failed" などで止まり、接続は試みられない）。
for table in "${event}" "${dlq}"; do
  output="$(
    AWS_ACCESS_KEY_ID=dummy AWS_SECRET_ACCESS_KEY=dummy AWS_DEFAULT_REGION=ap-northeast-1 \
      AWS_EC2_METADATA_DISABLED=true AWS_MAX_ATTEMPTS=1 \
      aws s3tables create-table \
      --table-bucket-arn arn:aws:s3tables:ap-northeast-1:111111111111:bucket/dummy \
      --namespace events --name dummy --format ICEBERG \
      --metadata "file://${table}" \
      --endpoint-url http://127.0.0.1:9 --cli-connect-timeout 2 2>&1 || true
  )"
  grep -q "Could not connect to the endpoint URL" <<<"${output}" \
    || fail "${table} が CreateTable の入力形式を満たしていない: ${output}"
done

echo "OK: schemas"
