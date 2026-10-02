# terraform test は depends_on を検証できないため、各 module の tests/ordering.sh が依存グラフで
# 作成順序を固定する。module のディレクトリで source して使う。
#
# terraform graph は推移簡約して出力する（別の経路で到達できる依存は辺として現れない）ため、
# 辺の有無ではなく到達可能性で判定する。

graph="$(terraform graph)"

# expect_dependency A B: A が B に（直接または推移的に）依存していることを確認する
expect_dependency() {
  local from="$1" to="$2" current next
  local -A seen=()
  local queue=("${from}")

  while ((${#queue[@]} > 0)); do
    current="${queue[0]}"
    queue=("${queue[@]:1}")
    [[ -n "${seen[${current}]:-}" ]] && continue
    seen["${current}"]=1
    [[ "${current}" == "${to}" ]] && return 0
    while IFS= read -r next; do
      queue+=("${next}")
    done < <(grep -F "\"${current}\" -> " <<<"${graph}" | sed -E 's/.*-> "([^"]+)";.*/\1/')
  done

  echo "依存が欠けています: ${from} -> ${to}" >&2
  exit 1
}
