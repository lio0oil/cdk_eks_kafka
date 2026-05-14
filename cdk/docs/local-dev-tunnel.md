# ローカル開発機から Kafka NLB に接続する（SSM Port Forwarding）

ローカルの開発コンテナ（Rancher Desktop など）から、`TestEc2Stack` の踏み台 EC2 経由で internal NLB に TCP トンネルを張り、Kafka クライアントを動かす手順。

クラスタ内 Pod から検証する場合は [cdk/README.md](../README.md) の「### 7. NLB 経由の Kafka 接続確認」を参照。

## 前提

- `dev` 環境で `EksCdkStack` と `TestEc2Stack` がデプロイ済み（`TestEc2Stack` は `dev` のみ存在）
- ローカル AWS 認証が通っており、IAM に `ssm:StartSession` / `ssm:TerminateSession` がある
- 作業はコンテナ内で完結させる（ホスト OS の `/etc/hosts` には触れない）

## なぜ単純な「1 ポート転送」では動かないか

Kafka クライアントは bootstrap に接続後、サーバーから返ってきた **advertised.listeners** のホスト名・ポートで各 broker に**再接続**する。

[cdk/ekscdk/constructs/_manifest.py](../ekscdk/constructs/_manifest.py) の採番ルールにより、broker_count=3 の現構成では次のポート設計になっている。

| 役割 | NLB listener |
|---|---|
| Bootstrap | 9094 |
| Broker 0 | 9095 |
| Broker 1 | 9096 |
| Broker 2 | 9097 |

さらに advertised host は **NLB の internal DNS 名**（[cdk/ekscdk/constructs/kafka.py](../ekscdk/constructs/kafka.py) の `advertised_host=nlb_dns_name`）で、コンテナ内ではこの名前は解決できないし、解決できても internal NLB に届かない。

したがって必要な工作は以下の 2 つ。

- **4 ポートぶん（9094-9097）を SSM で並行トンネル**して `localhost:9094-9097` ↔ EC2 経由 ↔ NLB を繋ぐ
- **コンテナの `/etc/hosts` で NLB DNS 名を `127.0.0.1` に向ける**ことで、advertised.listeners で返るホスト名をループバックに誘導

## 1. コンテナ内ツールのインストール

Debian / Ubuntu 系コンテナの例（Amazon Linux ベースならパッケージ名が異なる）。

```bash
# AWS CLI v2（既に入っていればスキップ）
curl -fsSL "https://awscli.amazonaws.com/awscli-exe-linux-$(uname -m).zip" -o /tmp/awscliv2.zip
unzip -q /tmp/awscliv2.zip -d /tmp && sudo /tmp/aws/install

# Session Manager Plugin
curl -fsSL "https://s3.amazonaws.com/session-manager-downloads/plugin/latest/ubuntu_64bit/session-manager-plugin.deb" -o /tmp/smp.deb
sudo dpkg -i /tmp/smp.deb

# kcat（kafka-console-* より軽い）
sudo apt-get update && sudo apt-get install -y kcat
```

## 2. 値の取得

```bash
export AWS_REGION=ap-northeast-1

INSTANCE_ID=$(aws cloudformation describe-stacks \
  --stack-name TestEc2Stack \
  --query 'Stacks[0].Outputs[?OutputKey==`TestEc2InstanceId`].OutputValue' \
  --output text)

NLB_DNS=$(aws cloudformation describe-stacks \
  --stack-name EksCdkStack \
  --query 'Stacks[0].Outputs[?OutputKey==`KafkaNlbDnsName`].OutputValue' \
  --output text)

echo "INSTANCE_ID=${INSTANCE_ID}"
echo "NLB_DNS=${NLB_DNS}"
```

## 3. `/etc/hosts` に NLB DNS を追加

advertised.listeners で返ってくる NLB DNS 名をコンテナ内で `127.0.0.1` に解決させる。

```bash
echo "127.0.0.1 ${NLB_DNS}" | sudo tee -a /etc/hosts
```

## 4. 4 本のトンネルを同時起動

`AWS-StartPortForwardingSessionToRemoteHost` は **1 セッション = 1 ポート**なので、Bootstrap + Broker0/1/2 で計 4 本必要。

```bash
# kafka-tunnel-up.sh
set -eu
PIDS=()
for PORT in 9094 9095 9096 9097; do
  aws ssm start-session \
    --target "${INSTANCE_ID}" \
    --document-name AWS-StartPortForwardingSessionToRemoteHost \
    --parameters "{\"host\":[\"${NLB_DNS}\"],\"portNumber\":[\"${PORT}\"],\"localPortNumber\":[\"${PORT}\"]}" \
    > "/tmp/ssm-${PORT}.log" 2>&1 &
  PIDS+=($!)
done
echo "${PIDS[@]}" > /tmp/ssm-tunnel.pids
echo "tunnels up: ${PIDS[@]}"
```

停止用。

```bash
# kafka-tunnel-down.sh
kill $(cat /tmp/ssm-tunnel.pids) 2>/dev/null || true
rm -f /tmp/ssm-tunnel.pids
```

## 5. 動作確認

```bash
sleep 3
kcat -b "${NLB_DNS}:9094" -L
```

メタデータ一覧で broker が 3 つ表示されればトンネル成立。プロデュース・コンシュームも同じ `-b ${NLB_DNS}:9094` で接続可能。

## 6. 後片付け

```bash
# トンネル停止
bash kafka-tunnel-down.sh

# /etc/hosts エントリ削除（NLB DNS が再作成されると古い行はゴミになる）
sudo sed -i "/${NLB_DNS}/d" /etc/hosts
```

## ハマりどころ

- **SG ingress**: 踏み台 EC2 は private subnet にいて VPC CIDR 内なので、NLB の SG ingress（[cdk/ekscdk/constructs/network.py](../ekscdk/constructs/network.py) の `Peer.ipv4(vpc_cidr_block)`）でそのまま通る。NLB 側を変更する必要はない。
- **NLB DNS の解決**: 踏み台 EC2 上で `nslookup ${NLB_DNS}` が VPC 内 IP を返すこと。SSM は EC2 側で `host:port` に TCP 接続するため、ここで失敗すると `no route` や `connection refused` になる。
- **SSM idle timeout**: SSM セッションは既定 20 分の idle で切れる。長時間テストでは Session Manager の `idleSessionTimeout` を上げるか、再起動スクリプトでカバー。
- **コンテナ再ビルド**: devcontainer を作り直すと `/etc/hosts` も Session Manager Plugin も消える。`postCreateCommand` か手動で再投入。
- **TLS 未対応の現構成**: external listener は `tls: false` で認証も設定されていない（[cdk/manifests/kafka/kafka-cluster.yaml](../manifests/kafka/kafka-cluster.yaml)）。トンネル経由でも平文 PLAINTEXT で接続する。TLS / SASL を有効化した場合は `security.protocol` 等の client config を別途用意する。

## 代替: 踏み台 EC2 上で直接 Kafka クライアントを動かす

複数ポートのトンネル管理が面倒な場合、EC2 上で kafka クライアントを動かす方が楽。EC2 は VPC 内なので advertised.listeners をそのまま解決でき、ポート転送も `/etc/hosts` 細工も不要。

```bash
aws ssm start-session --target "${INSTANCE_ID}"

# EC2 内で
sudo dnf install -y java-21-amazon-corretto-headless
curl -fsSL https://archive.apache.org/dist/kafka/3.8.0/kafka_2.13-3.8.0.tgz | tar -xz
./kafka_2.13-3.8.0/bin/kafka-topics.sh --bootstrap-server "${NLB_DNS}:9094" --list
```

「コンテナ内のコードから接続したい」場合はトンネル方式、「対話的に topic / produce / consume を叩きたいだけ」なら EC2 直接実行が早い。
