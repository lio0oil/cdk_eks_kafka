# kafka2/producer

[kafka2/proto/event.proto](../proto/event.proto) の `Envelope` を組み立て、zlib 圧縮した上で Kafka topic `sample-events-event` に送信するサンプル producer。

## メッセージ構造

```
Envelope
├─ common  (id, datetime)
├─ user    (userid, username)
└─ content
    └─ historyrecords[]  (historyid, datetime, memo, classification)
```

`constants.HISTORY_RECORD_COUNT` 件の `HistoryRecord` を 1 つの `Envelope` に詰める。各 `HistoryRecord.classification` は `classification-<index>` (index は `0..HISTORY_RECORD_COUNT-1`) になる。

## 送信データ

- `key`: iteration 番号の文字列
- `value`: `Envelope.SerializeToString()` を `zlib.compress()` した bytes
- Kafka header は付与しない (consumer 側もヘッダーを読まない)

## 実行例

```bash
uv sync
uv run python producer.py --count 10 --interval 1.0
uv run python producer.py --infinite   # Ctrl+C で停止するまで送り続ける
```

`BOOTSTRAP_SERVERS` / `TOPIC` は [constants.py](constants.py) の固定値。実環境では `BOOTSTRAP_SERVERS` を NLB / VPC Endpoint Service の DNS に置き換える。

## 性能テスト (Locust)

[locustfile.py](locustfile.py) が [Locust](https://locust.io/) の負荷定義。`make_envelope_payload` / `build_producer` を producer.py からそのまま import しているので、送信データの構造は producer.py と同じ。

```bash
uv sync --group dev

# Web UI を起動 (http://localhost:8089 をブラウザで開き、User数・起動レートを指定して開始/停止)
uv run locust -f locustfile.py

# ヘッドレス実行 (User数 10、起動レート 10/s、30 秒間実行して終了)
uv run locust -f locustfile.py --headless -u 10 -r 10 --run-time 30s

# broker に接続せず locustfile が正しく読み込めるかだけを確認する
uv run locust -f locustfile.py --list
```

`-u` (User数) と `-r` (起動レート) は役割が異なる。`-u` は同時に動く仮想ユーザー数で定常スループットを決める主要因、`-r` は `-u` の人数まで何人/秒で立ち上げるかというランプアップ速度で、定常スループットそのものには影響しない。理論上の全体スループットは `-u` × `locustfile.py` の `TARGET_RATE_PER_USER` (1 User あたりの目標 task 実行回数/秒) で近似できる。`TARGET_RATE_PER_USER` はコード側の固定値で CLI オプションでは変えられないため、レートを変えたい場合は `-u` を調整する。

接続先は環境変数で上書きする (未設定時の既定は `localhost:9094` / `test-topic`)。

```bash
KAFKA_BOOTSTRAP_SERVERS=kafka-nlb.example.internal:9094 KAFKA_TOPIC=test-topic \
  uv run locust -f locustfile.py --headless -u 10 -r 10 --run-time 30s
```

### 分散実行 (master/worker)

Locust は master 1 プロセス + worker N プロセスの構成で水平にスケールできる。**Kafka への produce を実際に行うのは worker のみ**で、master はテストの開始/停止指示と統計の集約に専念する (master 自身は produce しない)。`KAFKA_BOOTSTRAP_SERVERS` / `KAFKA_TOPIC` は produce する側、つまり worker 起動時に指定する (master には不要)。

同一ホストで試す場合、ターミナルを分けて起動する。

```bash
# ターミナル 1: master (Web UI: http://localhost:8089)
uv run locust -f locustfile.py --master

# ターミナル 2, 3, ...: worker (起動した数だけ worker が増える)
KAFKA_BOOTSTRAP_SERVERS=localhost:9094 KAFKA_TOPIC=test-topic \
  uv run locust -f locustfile.py --worker
```

worker が接続すると master の Web UI の "Workers" タブに台数が表示される。以降は Web UI から User 数・起動レートを指定してテストを開始する。

別マシンに分ける場合は worker 側に `--master-host` で master のアドレスを指定する (master/worker 間は TCP 既定ポート 5557 で通信する)。

```bash
KAFKA_BOOTSTRAP_SERVERS=... KAFKA_TOPIC=... \
  uv run locust -f locustfile.py --worker --master-host=<master のホスト名または IP>
```

ヘッドレスで worker の接続台数を待ってから自動開始することもできる (`--expect-workers` で必要台数を指定)。

```bash
uv run locust -f locustfile.py --master --headless --expect-workers 3 -u 30 -r 30 --run-time 30s
```

Python は GIL の制約で 1 プロセスが CPU 1 コアまでしか使い切れないため、複数コアを使う場合は同一ホストでも worker プロセスをコア数分立てる。`--processes N` で 1 コマンドから N プロセス起動できる。

1 プロセス (1 コア) が実際にこなせるスループットには上限があり、そのプロセス内の `TARGET_RATE_PER_USER × User数` がこの上限を超えると `constant_throughput` の待ち時間が 0 に張り付いて目標レートを達成できなくなる (Locust 側がボトルネックになっているサイン)。[.claude/PERFTEST.md](../../.claude/PERFTEST.md) の実測では payload 生成コストが 53.7 usec/msg で、1 プロセスあたり約 18,600 msg/s が上限の目安。目標スループットを上げたいときは `TARGET_RATE_PER_USER` ではなく worker プロセス数 (≒ CPU コア数) を増やして水平にスケールする。

`-u` (User数) は Locust の User が OS スレッドではなく gevent の greenlet であるため、CPU コア数と一致させる必要はない (HTTP のような I/O バウンドな負荷試験では 1 プロセスで User 数千を扱うことも珍しくない)。ただし `produce_envelope` はネットワーク I/O 待ちがほぼなく (`produce()`/`poll(0)` はノンブロッキングで実際の送受信は librdkafka の内部スレッドが行う)、payload 生成という CPU バウンドな処理が支配的コストになっている。そのため `-u` を増やしても上記の 1 プロセスあたりスループット上限は超えられず、上限に達したら `-u` をさらに増やすより `--processes` で worker プロセス数を増やす方が効果的。

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9094 KAFKA_TOPIC=test-topic \
  uv run locust -f locustfile.py --worker --processes 4
```

worker 間で index が重複しないよう `locustfile.py` の `compute_index` が `worker_index` を使って採番している (`worker_index` は master が worker 接続時に配布する連番)。EC2 フリート等クラスタ外の実行基盤を組む場合の背景は [.claude/PERFTEST.md](../../.claude/PERFTEST.md) を参照。

## 単発送信 (動作確認用)

コンシューマー側の動作確認などで Kafka に 1 件だけ送りたい場合は `produce_one()` を使う。1 件送信して `flush()` で配送を待つ。

```python
from producer import produce_one

produce_one("localhost:9094", "test-topic", index=0)
```

実 broker に対する統合テストとしても実行できる (`KAFKA_BOOTSTRAP_SERVERS` 未設定時は自動 skip)。

```bash
KAFKA_BOOTSTRAP_SERVERS=localhost:9094 KAFKA_TOPIC=test-topic uv run pytest tests/test_producer.py -k Integration
```

## テスト

```bash
uv sync --group dev
uv run pytest
```

## ProtoBuf 生成コード

[event_pb2.py](event_pb2.py) は [kafka2/proto/event.proto](../proto/event.proto) から protoc で生成した Python コード。`.proto` を変更したら再生成する。

```bash
protoc -I ../proto --python_out=. ../proto/event.proto
```

生成に使う protoc は、`pyproject.toml` の `protobuf>=6,<7` ランタイムと同じメジャーバージョン系列のものを使うこと (生成コード先頭の `ValidateProtobufRuntimeVersion` がメジャーバージョン不一致で import エラーになるため)。
