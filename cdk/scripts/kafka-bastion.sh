#!/usr/bin/env bash
# Kubernetes Pod (socat) を踏み台にして、ローカルから Kafka NLB (internal) へ
# kubectl port-forward で接続するための起動/停止スクリプト。
# NLB / kafka-cluster.yaml など既存のクラスタ構成は一切変更しない。
#
# advertised host は cdk/ekscdk/constructs/network.py の KAFKA_PRIVATE_DNS_NAME
# (Route53 Private Hosted Zone、固定値 "kafka.local") なので NLB の生 DNS 名を
# CloudFormation から取得する必要はない。
#
# 使い方:
#   bash kafka-bastion.sh up     # Pod 起動 + port-forward 開始 + /etc/hosts 追記
#   bash kafka-bastion.sh down   # port-forward 停止 + Pod 削除 + /etc/hosts 復元
#
# 起動後: bootstrap.servers=kafka.local:9094 でローカルから producer/consumer を
#         そのまま実行できる（現状 kafka-cluster.yaml は tls: false なので平文でよい）。
#
# 前提: kubectl に認証済みであること。EksCdkStack がデプロイ済みであること。
set -euo pipefail

NAMESPACE="default"
POD_NAME="kafka-bastion"
KAFKA_DNS="kafka.local"
PORTS=(9094 9095 9096 9097)
STATE_DIR="/tmp/kafka-bastion"
PID_FILE="${STATE_DIR}/port-forward.pid"

usage() {
  echo "usage: $0 {up|down}" >&2
  exit 1
}

cmd_up() {
  mkdir -p "${STATE_DIR}"

  # 既存の同名 Pod があれば作り直す
  kubectl delete pod "${POD_NAME}" -n "${NAMESPACE}" --ignore-not-found --wait=true

  cat <<EOF | kubectl apply -f -
apiVersion: v1
kind: Pod
metadata:
  name: ${POD_NAME}
  namespace: ${NAMESPACE}
spec:
  restartPolicy: Never
  containers:
    - name: bastion
      image: alpine/socat
      command: ["sh", "-c"]
      args:
        - |
          for PORT in ${PORTS[*]}; do
            socat TCP-LISTEN:\${PORT},fork,reuseaddr TCP:${KAFKA_DNS}:\${PORT} &
          done
          wait
EOF

  kubectl wait --for=condition=Ready "pod/${POD_NAME}" -n "${NAMESPACE}" --timeout=60s

  # port-forward をバックグラウンド起動（1 プロセスで 4 ポート同時）
  FORWARD_ARGS=()
  for PORT in "${PORTS[@]}"; do
    FORWARD_ARGS+=("${PORT}:${PORT}")
  done
  nohup kubectl port-forward "pod/${POD_NAME}" -n "${NAMESPACE}" "${FORWARD_ARGS[@]}" \
    > "${STATE_DIR}/port-forward.log" 2>&1 &
  echo $! > "${PID_FILE}"

  sleep 2
  if ! kill -0 "$(cat "${PID_FILE}")" 2>/dev/null; then
    echo "port-forward の起動に失敗しました。ログ: ${STATE_DIR}/port-forward.log" >&2
    exit 1
  fi

  # /etc/hosts に追記（重複させない。コンテナ内での作業を想定）
  if ! grep -q "${KAFKA_DNS}" /etc/hosts; then
    echo "127.0.0.1 ${KAFKA_DNS}" | sudo tee -a /etc/hosts > /dev/null
  fi

  echo "起動完了: bootstrap.servers=${KAFKA_DNS}:9094"
}

cmd_down() {
  if [ -f "${PID_FILE}" ]; then
    kill "$(cat "${PID_FILE}")" 2>/dev/null || true
    rm -f "${PID_FILE}"
  fi

  kubectl delete pod "${POD_NAME}" -n "${NAMESPACE}" --ignore-not-found

  sudo sed -i "/${KAFKA_DNS//./\\.}/d" /etc/hosts

  echo "停止・削除完了"
}

case "${1:-}" in
  up) cmd_up ;;
  down) cmd_down ;;
  *) usage ;;
esac
