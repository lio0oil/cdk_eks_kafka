#!/usr/bin/env bash
# function.zip と MERGE ジョブの成果物を作る。AWS は呼ばず、アップロードもしない。
# 使い方: build-artifacts.sh [出力先]（既定は parquet-merge/terraform/build、gitignore 済み）
set -euo pipefail

root="$(cd "$(dirname "$0")/../.." && pwd)"
out="${1:-${root}/terraform/build}"

work="$(mktemp -d)"
trap 'rm -rf "${work}"' EXIT

# 出力先は作り直すため、消してよいものだけを許可する。
# 存在しない、空のディレクトリ、このスクリプトが過去に作ったディレクトリ（function.zip と merger/）のいずれか。
if [[ -e "${out}" || -L "${out}" ]]; then
  if [[ ! -d "${out}" || -L "${out}" ]]; then
    echo "出力先 ${out} はディレクトリではない。別のパスを指定すること" >&2
    exit 1
  fi
  if [[ -n "$(ls -A "${out}")" && ! (-f "${out}/function.zip" && -d "${out}/merger") ]]; then
    echo "出力先 ${out} は空でなく、このスクリプトの成果物（function.zip と merger/）でもないため消さない。別のパスを指定すること" >&2
    exit 1
  fi
fi

rm -rf "${out}"
mkdir -p "${out}/merger"
out="$(cd "${out}" && pwd)"

# Lambda（python3.13、x86_64、Amazon Linux 2023）向けの wheel を取る。merge-app の architectures と揃える。
# boto3 は Lambda のランタイムが提供するため、依存を uv.lock から出す時点で writer の宣言に含まれない。
package="${work}/function"
mkdir -p "${package}"
uv export --project "${root}/writer" --frozen --no-dev --no-emit-project --quiet -o "${work}/requirements.txt"
uv pip install --quiet --target "${package}" --no-deps --only-binary :all: \
  --python-version 3.13 --python-platform x86_64-manylinux_2_28 -r "${work}/requirements.txt"

for source in "${root}"/writer/*.py; do
  cp "${source}" "${package}/"
done
# handler の既定の読み込み先（自身と同じディレクトリ）に置く
cp "${root}/contract/staging_schema.json" "${package}/staging_schema.json"

find "${package}" -name __pycache__ -type d -prune -exec rm -rf {} +
(cd "${package}" && zip -q -r -X "${out}/function.zip" .)

# py-files で渡すモジュール。エントリポイントの job.py は別のオブジェクトとして置く
zip -q -j -X "${out}/merger/modules.zip" \
  "${root}/merger/merge.py" "${root}/merger/incoming.py" "${root}/merger/staging_schema.py"
cp "${root}/merger/job.py" "${out}/merger/job.py"
cp "${root}/contract/staging_schema.json" "${out}/merger/staging_schema.json"

cat <<INSTRUCTIONS
作成した成果物:
  ${out}/function.zip
  ${out}/merger/job.py
  ${out}/merger/modules.zip
  ${out}/merger/staging_schema.json

アップロード（infra を適用して artifact バケットを作った後。この script は実行しない）:

  ARTIFACT_BUCKET="\$(terraform -chdir=parquet-merge/terraform/env/dev/infra output -raw artifact_bucket_name)"
  aws s3 cp ${out}/function.zip s3://\${ARTIFACT_BUCKET}/function.zip
  aws s3 cp ${out}/merger/job.py s3://\${ARTIFACT_BUCKET}/merger/job.py
  aws s3 cp ${out}/merger/modules.zip s3://\${ARTIFACT_BUCKET}/merger/modules.zip
  aws s3 cp ${out}/merger/staging_schema.json s3://\${ARTIFACT_BUCKET}/merger/staging_schema.json

function.zip の VersionId を取得し、env/dev/app/terraform.tfvars の function_object_version に書いて app を適用する:

  aws s3api head-object --bucket "\${ARTIFACT_BUCKET}" --key function.zip --query VersionId --output text
INSTRUCTIONS
