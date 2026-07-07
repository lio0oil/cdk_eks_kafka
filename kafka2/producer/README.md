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

## ProtoBuf 生成コード

[event_pb2.py](event_pb2.py) は [kafka2/proto/event.proto](../proto/event.proto) から protoc で生成した Python コード。`.proto` を変更したら再生成する。

```bash
protoc -I ../proto --python_out=. ../proto/event.proto
```

生成に使う protoc は、`pyproject.toml` の `protobuf>=6,<7` ランタイムと同じメジャーバージョン系列のものを使うこと (生成コード先頭の `ValidateProtobufRuntimeVersion` がメジャーバージョン不一致で import エラーになるため)。
