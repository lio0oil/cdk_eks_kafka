#!/usr/bin/env bash
# 全 module と env の root を、実 AWS・実 backend に触れずに検証する（validate / test / 順序チェック）。
# pre-commit から呼ばれる。
set -euo pipefail

cd "$(dirname "$0")/../.."

# init のたびに約 800MB の AWS provider を重複して取得しないよう、プラグインキャッシュを使う
export TF_PLUGIN_CACHE_DIR="${HOME}/.terraform.d/plugin-cache"
mkdir -p "${TF_PLUGIN_CACHE_DIR}"

copies=()
cleanup() {
  for c in "${copies[@]}"; do
    rm -rf "${c}"
  done
}
trap cleanup EXIT

# terraform test 内の相対パスは cwd 基準のため、ディレクトリごとに -chdir で実行する
check() {
  local dir="$1" script
  terraform -chdir="${dir}" init -backend=false -input=false >/dev/null
  terraform -chdir="${dir}" validate
  if [[ -d "${dir}/tests" ]]; then
    terraform -chdir="${dir}" test
  fi
  for script in "${dir}"/tests/*.sh; do
    if [[ -e "${script}" ]]; then
      bash "${script}"
    fi
  done
}

for dir in terraform/modules/*/; do
  check "${dir%/}"
done

# env の root は S3 backend を宣言している。terraform graph（順序チェック）は backend の初期化を要求し、
# 初期化には認証情報が要るため、backend.tf を除いたコピーで検証する。相対パスを保つため同じ深さに置く。
# 実際に init 済みの .terraform（S3 backend の設定）にも触れない。
for dir in terraform/env/*/*/; do
  dir="${dir%/}"
  copy="${dir}.check"
  rm -rf "${copy}"
  copies+=("${copy}")
  cp -r "${dir}" "${copy}"
  rm -rf "${copy}/.terraform" "${copy}/backend.tf"
  check "${copy}"
done
