#!/usr/bin/env bash
# create-tables.sh の振る舞いを、aws を差し替えて検証する（実 AWS には触れない）。
set -euo pipefail

cd "$(dirname "$0")/.."
script_dir="$(pwd)"
script="${script_dir}/create-tables.sh"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

mkdir -p "${work}/bin"
cat >"${work}/bin/aws" <<'STUB'
#!/usr/bin/env bash
# 呼び出しを記録し、STUB_EXISTING に含まれるテーブルだけ get-table を成功させる。
echo "$*" >>"${STUB_LOG}"
operation="$2"
name=""
while (($# > 0)); do
  [[ "$1" == "--name" ]] && name="$2"
  shift
done
if [[ "${operation}" == "get-table" ]]; then
  if [[ " ${STUB_EXISTING:-} " == *" ${name} "* ]]; then
    echo '{}'
  elif [[ -n "${STUB_GET_ERROR:-}" ]]; then
    echo "An error occurred (AccessDeniedException) when calling the GetTable operation: denied" >&2
    exit 254
  else
    echo "An error occurred (NotFoundException) when calling the GetTable operation: not found" >&2
    exit 254
  fi
fi
STUB
chmod +x "${work}/bin/aws"

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

arn="arn:aws:s3tables:ap-northeast-1:111111111111:bucket/kafka-events-dev"

run_script() {
  : >"${work}/log"
  STUB_LOG="${work}/log" PATH="${work}/bin:${PATH}" "$@"
}

creates() { grep -c '^s3tables create-table ' "${work}/log" || true; }

# 無いテーブルは、テーブル定義 JSON のファイル名を名前にして作る
STUB_EXISTING="" run_script "${script}" "${arn}" events >/dev/null
[[ "$(creates)" == "2" ]] || fail "2 テーブルとも作られるはず: $(cat "${work}/log")"
for name in sample_events_event sample_events_dlq; do
  grep -qxF "s3tables create-table --table-bucket-arn ${arn} --namespace events --name ${name} --format ICEBERG --metadata file://${script_dir}/tables/${name}.json" "${work}/log" \
    || fail "${name} の create-table の引数が想定と異なる: $(cat "${work}/log")"
done

# 既にあるテーブルには触れない（スキーマ進化を妨げない）
STUB_EXISTING="sample_events_event" run_script "${script}" "${arn}" events >/dev/null
[[ "$(creates)" == "1" ]] || fail "dlq だけが作られるはず: $(cat "${work}/log")"
grep -q -- '--name sample_events_dlq --format ICEBERG' "${work}/log" || fail "dlq が作られていない"

STUB_EXISTING="sample_events_event sample_events_dlq" run_script "${script}" "${arn}" events >/dev/null
[[ "$(creates)" == "0" ]] || fail "両方あれば何も作らないはず: $(cat "${work}/log")"

# NotFound 以外の失敗（権限不足など）を「無い」と解釈して作成に進まない
if STUB_GET_ERROR=1 run_script "${script}" "${arn}" events >/dev/null 2>"${work}/err"; then
  fail "get-table が権限エラーなら失敗するはず"
fi
[[ "$(creates)" == "0" ]] || fail "get-table が失敗したら作成に進まないはず"
grep -q AccessDeniedException "${work}/err" || fail "get-table のエラーが表示されていない"

# 引数が足りなければ使い方を示して失敗する
if run_script "${script}" "${arn}" >/dev/null 2>"${work}/err"; then
  fail "namespace が無ければ失敗するはず"
fi
grep -q '使い方' "${work}/err" || fail "使い方が表示されていない"

echo "OK: create-tables"
