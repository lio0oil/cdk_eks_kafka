#!/usr/bin/env bash
# terraform test は depends_on を検証できないため、依存グラフで Addon と nodegroup の作成順序を固定する。
# 順序が崩れると、CNI が無いノードが NotReady のまま nodegroup の作成がタイムアウトする（before_compute）、
# またはノードの無い状態で Addon の作成がタイムアウトする（after_compute）。
set -euo pipefail

cd "$(dirname "$0")/.."
source ../../scripts/graph.sh

expect_dependency aws_eks_node_group.this aws_eks_addon.before_compute
expect_dependency aws_eks_addon.after_compute aws_eks_node_group.this
