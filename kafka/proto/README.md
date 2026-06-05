# kafka/proto

Kafka メッセージの Protocol Buffer スキーマ。producer (`kafka/producer/`) と consumer (`kafka/consumer/`) の両方から参照する。

1 つの topic `sample-events-event` に **構造の異なる 2 つの定義** が相乗りで流れる:

- `event.proto` の `Envelope`: Event を共通フィールドとして持ち、`oneof extra` で追加情報 (OrderEvent / UserEvent / ...) を 1 つだけ含む。新しい extra を追加する場合は `Envelope.extra` の oneof に case を増やして再生成するだけで、consumer のコード変更は不要 (consumer 側は events.desc から oneof case を動的列挙する)。
- `metric.proto` の `Metric`: Envelope とは別構造のフラットな型 (Event ネストも oneof も持たない)。consumer は Kafka header の proto-schema でこの型を識別し、`metric_id / observed_at / kind` を Envelope と同じ `event_id / event_datetime / extra_type` カラムへ写像して同一テーブルに書く。

両者とも 1 つの統合 `events.desc` に束ねて consumer に渡す。`from_protobuf` は `messageName` 引数 (`ekscdk.kafka.Envelope` / `ekscdk.kafka.Metric`) で型を選び分ける。

## ファイル配置

| ファイル | 場所 | 役割 |
|---|---|---|
| `event.proto` | `kafka/proto/event.proto` | Envelope スキーマ定義 (編集対象) |
| `metric.proto` | `kafka/proto/metric.proto` | Metric スキーマ定義 (編集対象) |
| `event_pb2.py` / `metric_pb2.py` | `kafka/producer/` | producer が import する Python クラス (生成物) |
| `events.desc` | `kafka/consumer/events.desc` | Spark の `from_protobuf` および consumer の oneof 列挙が読む統合 FileDescriptorSet (両 .proto を束ねた生成物) |

生成物は `.proto` から自動生成され、再生成のたびに producer / consumer 各ディレクトリへ配置する。`kafka/proto/` 自身には生成物を置かない。

## 再生成

`protoc` が手元にあれば:

```bash
cd kafka/proto
protoc --python_out=. --descriptor_set_out=events.desc -I. event.proto metric.proto
mv event_pb2.py ../producer/event_pb2.py
mv metric_pb2.py ../producer/metric_pb2.py
mv events.desc ../consumer/events.desc
```

`protoc` が無い環境では `grpcio-tools` を一時的に使う (uv で隔離環境):

```bash
cd kafka/proto
uvx --from grpcio-tools python -m grpc_tools.protoc \
  --python_out=. --descriptor_set_out=events.desc -I. event.proto metric.proto
mv event_pb2.py ../producer/event_pb2.py
mv metric_pb2.py ../producer/metric_pb2.py
mv events.desc ../consumer/events.desc
```

## EMR Spark への配布

`events.desc` だけ S3 にアップロードすればよい。`*_pb2.py` は producer 専用なので EMR に置く必要はない。

```bash
aws s3 cp kafka/consumer/events.desc s3://<アーティファクトバケット>/proto/events.desc
```
