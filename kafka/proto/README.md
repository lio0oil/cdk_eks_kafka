# kafka/proto

Kafka メッセージの Protocol Buffer スキーマと生成物を集約するディレクトリ。producer (`kafka/producer/`) と consumer (`kafka/consumer/`) の両方から参照する。

サンプル実装では **Event 1 種類のみ** を定義しているが、`.proto` を追加して `events.desc` を再生成するだけで複数 message に拡張可能。consumer 側は events.desc から `SCHEMAS` を自動展開する。

## ファイル

| ファイル | 役割 |
|---|---|
| `event.proto` | Event スキーマ (id, datetime, name) ─ 編集対象 |
| `event_pb2.py` | Event 用の Python クラス (生成物・producer が import) |
| `events.desc` | 全 message を含む統合 FileDescriptorSet (生成物・Spark の `from_protobuf` 用) |

生成物 (`*_pb2.py`, `events.desc`) は `.proto` から自動生成される。リポジトリには両方コミットする (CI や EMR デプロイで再生成不要にするため)。

## 再生成

`protoc` が手元にあれば:

```bash
cd kafka/proto
protoc --python_out=. --descriptor_set_out=events.desc -I. event.proto
```

`protoc` が無い環境では `grpcio-tools` を一時的に使う (uv で隔離環境):

```bash
cd kafka/proto
uvx --from grpcio-tools python -m grpc_tools.protoc \
  --python_out=. --descriptor_set_out=events.desc -I. event.proto
```

複数の `.proto` を一度に渡すと **1 つの `events.desc` にまとめて出力**される。後で型を追加する場合は `event_b.proto` などを作って同じコマンドの引数に並べるだけ。

## EMR Spark への配布

`events.desc` だけ S3 にアップロードすればよい。`*_pb2.py` は producer 専用なので EMR に置く必要はない。

```bash
aws s3 cp events.desc s3://<アーティファクトバケット>/proto/events.desc
```
