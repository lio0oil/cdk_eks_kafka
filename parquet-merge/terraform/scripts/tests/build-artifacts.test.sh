#!/usr/bin/env bash
# build-artifacts.sh が作る成果物の中身を検査する（AWS には触れない）。
# 依存の wheel を取得するためネットワークが要り、時間もかかるため、check.sh には組み込まない。
#   bash parquet-merge/terraform/scripts/tests/build-artifacts.test.sh
set -euo pipefail

cd "$(dirname "$0")/../../.."
root="$(pwd)"
script="${root}/terraform/scripts/build-artifacts.sh"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

fail() {
  echo "FAIL: $*" >&2
  exit 1
}

# --- 出力先の保護（mktemp 配下の一時ディレクトリだけを使う） ---
# 無関係なファイルを含む既存ディレクトリは消さずに拒否する
unrelated="${work}/unrelated"
mkdir -p "${unrelated}"
echo keep >"${unrelated}/important.txt"
if bash "${script}" "${unrelated}" >/dev/null 2>"${work}/reject.err"; then
  fail "無関係なファイルを含む出力先を拒否しなかった"
fi
[[ -f "${unrelated}/important.txt" ]] || fail "拒否した出力先のファイルが消えた"
[[ -s "${work}/reject.err" ]] || fail "出力先を拒否した理由が表示されない"

# function.zip だけで merger/ がないディレクトリは、このスクリプトの成果物とみなさない
partial="${work}/partial"
mkdir -p "${partial}"
echo keep >"${partial}/function.zip"
if bash "${script}" "${partial}" >/dev/null 2>&1; then
  fail "function.zip だけのディレクトリを拒否しなかった"
fi
[[ -f "${partial}/function.zip" ]] || fail "拒否した出力先の function.zip が消えた"

# 出力先が通常のファイルなら拒否する
file_target="${work}/file-target"
echo keep >"${file_target}"
if bash "${script}" "${file_target}" >/dev/null 2>&1; then
  fail "通常のファイルを出力先として拒否しなかった"
fi
[[ -f "${file_target}" ]] || fail "拒否した出力先のファイルが消えた"

out="${work}/build"
stdout="$(bash "${script}" "${out}")" || fail "build-artifacts.sh が失敗した"

function_zip="${out}/function.zip"
modules_zip="${out}/merger/modules.zip"
function_entries="$(unzip -Z1 "${function_zip}")"
modules_entries="$(unzip -Z1 "${modules_zip}")"

has_entry() {
  grep -qxF -- "$2" <<<"$1"
}

# --- function.zip ---
for name in handler.py decode.py schema.py readings_pb2.py staging_schema.json; do
  has_entry "${function_entries}" "${name}" || fail "function.zip の直下に ${name} がない"
done

unzip -p "${function_zip}" staging_schema.json | cmp -s - "${root}/contract/staging_schema.json" ||
  fail "function.zip の staging_schema.json が contract/staging_schema.json と異なる"

has_entry "${function_entries}" "pyarrow/__init__.py" || fail "function.zip に pyarrow がない"
has_entry "${function_entries}" "google/protobuf/__init__.py" || fail "function.zip に protobuf がない"

# 実行環境（Linux x86_64、python3.13）向けの wheel であること。ビルドしたマシンの wheel ではない
has_entry "${function_entries}" "pyarrow/lib.cpython-313-x86_64-linux-gnu.so" ||
  fail "pyarrow が Lambda（python3.13、x86_64、Linux）向けの wheel でない"

# boto3 は Lambda のランタイムが提供する
if grep -qE '^(boto3|botocore|s3transfer)/' <<<"${function_entries}"; then
  fail "function.zip に boto3 系のパッケージが入っている"
fi
# 依存パッケージ自身の tests（pyarrow/tests など）は対象外。writer の tests/ が入っていないことを見る
if grep -qE '^tests/' <<<"${function_entries}"; then
  fail "function.zip の直下に tests/ が入っている"
fi
if grep -qE '\.pyi$|__pycache__' <<<"${function_entries}"; then
  fail "function.zip に .pyi か __pycache__ が入っている"
fi

# Lambda の展開後サイズの上限は 250MB
unzipped_bytes="$(unzip -Zt "${function_zip}" | awk '{print $3}')"
((unzipped_bytes < 262144000)) || fail "function.zip の展開後サイズが 250MB 以上: ${unzipped_bytes}"
echo "function.zip: $(stat -c %s "${function_zip}") bytes, 展開後 ${unzipped_bytes} bytes"

# --- MERGE ジョブ ---
[[ -f "${out}/merger/job.py" ]] || fail "merger/job.py がない"
cmp -s "${out}/merger/job.py" "${root}/merger/job.py" ||
  fail "成果物の merger/job.py（${out}/merger/job.py）がソースの merger/job.py（${root}/merger/job.py）と異なる"
cmp -s "${out}/merger/staging_schema.json" "${root}/contract/staging_schema.json" ||
  fail "merger/staging_schema.json が contract/staging_schema.json と異なる"

for name in merge.py incoming.py staging_schema.py; do
  has_entry "${modules_entries}" "${name}" || fail "modules.zip の直下に ${name} がない"
done
if has_entry "${modules_entries}" "job.py"; then
  fail "modules.zip にエントリポイントの job.py が入っている"
fi
if grep -qE '(^|/)tests/|__pycache__' <<<"${modules_entries}"; then
  fail "modules.zip に tests/ か __pycache__ が入っている"
fi

# --- アップロード手順 ---
# module が参照するキーと、手順に出すキーが一致していること
module_key() {
  sed -nE "s/^[[:space:]]*$1[[:space:]]*=[[:space:]]*\"([^\"]+)\".*/\1/p" "${root}/terraform/modules/merge-app/main.tf"
}
for local_name in function_key job_script_key job_py_files_key job_contract_key; do
  key="$(module_key "${local_name}")"
  [[ -n "${key}" ]] || fail "merge-app の local.${local_name} が読めない"
  grep -qF "s3://\${ARTIFACT_BUCKET}/${key}" <<<"${stdout}" || fail "手順に ${key} のアップロードがない"
done
grep -qF "aws s3 cp" <<<"${stdout}" || fail "手順に aws s3 cp がない"
grep -qF "head-object" <<<"${stdout}" || fail "手順に VersionId の取得（head-object）がない"
grep -qF "VersionId" <<<"${stdout}" || fail "手順に VersionId がない"

# --- 再実行 ---
# 前回の成果物ディレクトリは上書きできる。残骸が混ざらないよう、前回だけにあるファイルは消える
echo stale >"${out}/merger/stale.txt"
bash "${script}" "${out}" >/dev/null || fail "成果物ディレクトリへの再実行が失敗した"
[[ ! -e "${out}/merger/stale.txt" ]] || fail "再実行後に前回の残骸が残っている"
[[ -f "${out}/function.zip" ]] || fail "再実行後に function.zip がない"

# 空のディレクトリも許可する
empty="${work}/empty"
mkdir -p "${empty}"
bash "${script}" "${empty}" >/dev/null || fail "空のディレクトリへの出力が失敗した"
[[ -f "${empty}/merger/modules.zip" ]] || fail "空のディレクトリへの出力後に modules.zip がない"

echo "OK"
