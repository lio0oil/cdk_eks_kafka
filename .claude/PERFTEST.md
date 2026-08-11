# Kafka + Lambda ESM 性能測定アーキテクチャ調査

Kafka クラスタ + Lambda ESM consumer の性能測定に使う負荷生成ツール・実行基盤の選定調査。TODO.md の「broker/controller の `resources` / `storage.size` / `jvmOptions` を stg/prd 向けにチューニングする」の判断材料を得ることが目的。本ドキュメントは調査結果であり、実装はまだ行っていない。

## 背景・要件

測定対象は **Kafka クラスタ + Lambda ESM consumer** の 1 経路。producer の動きは [../kafka2/producer](../kafka2/producer) が正（Envelope 組み立て → protobuf serialize → zlib 圧縮 → Kafka 送信）で、これは変更しない。Lambda は zlib 解凍 → ProtocolBuffers デシリアライズ → S3 Tables 登録を行う予定だが**現状コードは無い**（[../cdk/lambda/kafka_consumer/index.py](../cdk/lambda/kafka_consumer/index.py) は受信内容をログするだけのダミー）。

要件として **「protobuf の中の値をメッセージごとに変更する」** ことが確定している。これは負荷生成ランタイムが実行時に protobuf を組み立て直し zlib で圧縮し直せることを要求する。事前生成した payload プールを使い回す案はこの要件を満たさないため採らない。

「ツール利用優先」は「コードを一切書かない」という意味ではない。機能を満たすためのコード記述は許容する。したがって「payload 生成ロジックを別言語で書き直すことになる」ことは候補を落とす決定打にはしない。

検討対象は **Locust + confluent-kafka / k6 + xk6-kafka（Go 拡張） / Gatling + galax-io プラグイン** の 3 候補。k6 は純 JS バンドル変種を採らず Go 拡張前提とする。負荷試験の実行そのものはユーザーが行う（`cdk deploy` と同じ整理）。

## 実測データ

判断の根拠として、3 ランタイムすべてで実測した。

payload の同一性はすべてで確認済み。producer.py が送る生 protobuf（1307 B）を同一入力として、JVM `Deflater` と JS `fflate` はどちらも **232 B・ヘッダ `78 9c`** を出力し、Python の `zlib.compress` と同一の zlib 形式（RFC 1950）であることを確認した。**どの候補でも payload の再現自体は問題にならない。**

**zlib 圧縮コスト**（同一の 1307 B に対する実測、これが payload 生成の支配的コスト）

| ランタイム | 実装 | usec/msg |
|---|---|---|
| JVM | `java.util.zip.Deflater`（native） | **11.4** |
| Node (V8) | `node:zlib`（C） | 16.9 |
| Python | `zlib.compress`（C） | 25.2 |
| Node (V8) | `fflate zlibSync`（pure JS） | **45.8** |

**payload 生成の全体コスト**

| ランタイム | 内容 | usec/msg | プロセスあたり上限 |
|---|---|---|---|
| Python | `make_envelope_payload`（オブジェクト構築 ~27 + serialize 1.2 + zlib 25.2） | **53.7** | 約 18,600 msg/s |
| Node (V8) | protobufjs encode 12.7 + fflate zlib | **62.3** | 約 16,000 msg/s |
| JVM | zlib のみ実測。protobuf encode / オブジェクト構築は未計測 | 11.4〜 | 未計測 |

最下段の `fflate`（pure JS）は、k6 の純 JS 変種を採った場合の性能である。k6 の JS ランタイムは [Sobek（Goja フォーク）](https://deepwiki.com/grafana/k6/4-javascript-runtime-and-modules)で**JIT を持たないインタープリタ**のため、V8 の JIT 込みで測った 62.3 usec すら Sobek が超えられない下限にすぎない。**この不利が、k6 を Go 拡張前提とする根拠になっている**（Go 拡張ならコンパイル済みコードで動くため、この penalty は発生しない）。

なお Go の `compress/zlib` は cgo ではなく純 Go 実装であり、C の zlib と完全に同等とは限らない。ただし [klauspost/compress](https://github.com/klauspost/compress)（Go で最も最適化された実装）でも標準ライブラリ比 5〜10% の差にとどまるため、標準ライブラリで大きく外すことはない。本環境に Go ツールチェーンが無いため未計測だが、pure JS のような桁違いの不利は生じない。

## 候補比較

| | Locust + confluent-kafka | k6 + xk6-kafka（Go 拡張） | Gatling + galax-io |
|---|---|---|---|
| payload 再現 | 実コード（`make_envelope_payload`）を import | Go で再実装 | Scala/Java で再実装 |
| 生成性能 | 53.7 usec（実測） | 未計測（コンパイル済み Go、pure JS の不利なし） | zlib 11.4 usec（実測、最速） |
| Kafka クライアント | **librdkafka（producer.py と同一）** | kafka-go | kafka-clients（Java 公式） |
| ビルド | 不要 | **xk6 build（Go ツールチェーン + カスタムバイナリ配布）** | Maven / Gradle |
| 分散実行 | master/worker（OSS 標準機能） | `--execution-segment` による手動分散（K8s 不要）または k6-operator（K8s 必須、instance 識別は自前実装が必要） | **OSS にはなし**（Enterprise 限定、DIY するなら手動） |
| レポート | 標準（Web UI / CSV） | 強い | 最も強い（単一プロセスの場合） |
| リポジトリ適合 | Python 一色に合致 | Go + JS + xk6 ビルド基盤を追加 | JVM + ビルドツールを追加 |
| 最大の残リスク | gevent との相性 | ビルド運用の継続コスト | プラグインの byte[] API が未確認・分散は Enterprise 限定 |

### Locust + confluent-kafka

[../kafka2/producer/constants.py](../kafka2/producer/constants.py) の `make_envelope_payload` を import してそのまま呼べる唯一の候補で、payload 定義が 1 箇所に保たれる。この関数は既に index ごとに `common.id` / `user.userid` / `username` / `historyid` / `memo` を変えるため「値を変える」要件をそのまま満たす。実 producer と同じ librdkafka を使うので、クライアント挙動の乖離を考えなくてよい。

**残リスク（gevent）**: Locust は gevent ベースで、confluent-kafka の C レベルのブロッキング呼び出しは monkey-patch できない（[DoorDash の解説](https://careersatdoordash.com/blog/how-to-make-kafka-consumer-compatible-with-gevent-in-python/)）。ただしこれは consumer 用途の落とし穴であり、producer 用途では影響が限定される。`produce()` は librdkafka の C バッファに積むだけの非ブロッキング呼び出し、`poll(0)` も非ブロッキングで、キュー満杯時は `BufferError` が上がる（ブロックしない）。ブロックするのは終了時の `flush()` のみで許容できる。

レート制御は `constant_throughput` wait time がツール標準機能として使える。分散実行時の worker 識別・index 一意性の詳細は後述の「分散実行の詳細」を参照。

### k6 + xk6-kafka（Go 拡張）

xk6-kafka の protobuf serde は使えない。standalone モードでも Confluent wire format のフレーミング（magic byte + schema ID + message indexes）を必ず付与し、生の protobuf バイト列を出す設定は存在しない。producer.py は生 protobuf を zlib 圧縮して送るため、フレーミングが混入すると payload が別物になる。したがって payload はカスタム Go 拡張で作り、`SCHEMA_TYPE_BYTES` で送ることになる。

**構成**: カスタム拡張に `make(index) -> []byte`（Envelope 組み立て → protobuf serialize → zlib 圧縮）を実装し、JS 側は `writer.produce()` に渡すだけにする。zlib だけを Go 拡張にして protobuf 組み立てを JS に残す設計は採らない。protobufjs の encode（V8 で 12.7 usec）が Sobek でそのまま penalty を受け、Go 拡張にした意味が薄れるためである。重い処理はすべて Go 側に置く。

**Go↔JS 境界のコストは問題にならない見込み**。[pkg/kafka/bytearray.go](https://github.com/mostafa/xk6-kafka/blob/main/pkg/kafka/bytearray.go) の `ByteArraySerde.Serialize` は Go の `[]byte` を受け取った場合そのまま返す実装で、`[]any`（JS Array）の場合のみ要素ごとに `float64` → `byte` へキャストする。カスタム拡張が `[]byte` を返せばこの高速パスに乗るため、232 バイトを要素ごとに boxing する事態は避けられる。ただし Sobek が拡張間で `[]byte` を保持したまま受け渡すかは PoC で要確認。

**最大の課題はビルド運用の継続コスト**。`xk6 build` で k6 + xk6-kafka + カスタム拡張を結合したバイナリを作り、Docker イメージとして配布する必要がある。k6 / xk6-kafka のバージョン更新時、および `.proto` 変更時に、Go ツールチェーンでの再ビルドと再配布が毎回発生する。

payload 生成をすべて Go に置くと JS スクリプトはほぼ実体を持たなくなる（`make()` を呼んで produce するだけ）。この構成で k6 から得られる価値は、スクリプティングではなくハーネス（`constant-arrival-rate` 等の executor、メトリクス / threshold、`--execution-segment` による分散、Grafana 連携）に限られる。それが十分な価値かが採否の分かれ目になる。

### Gatling + galax-io プラグイン

[gatling-kafka-plugin](https://github.com/galax-io/gatling-kafka-plugin) は Protobuf / Avro / Schema Registry 対応で、Scala / Java / Kotlin が使える。zlib が 11.4 usec と 3 ランタイム中最速で、protobuf-java も揃うため性能面では最も有利。レポートは 3 候補中で最も強く、CI/CD 連携のプラグイン群も揃っている。

**残リスク**: README の例は String キー / String 値のみで、`Array[Byte]` を送る例と、Session から動的に payload を生成する例が示されていない。Scala 版が `send[K, V]` のジェネリクスを持ち、`producerSettings(Map(...))` で producer プロパティを渡せることから `ByteArraySerializer` + `Array[Byte]` は通ると見込まれるが未確認。またコミュニティプラグインであり Gatling 公式ではない（[公式ガイド](https://docs.gatling.io/guides/use-cases/kafka/)もこのプラグインを参照している）。

このリポジトリは Python + CDK で統一されており、JVM とビルドツール（Maven/Gradle）の導入が運用上の新規要素になる。加えて **OSS 版には分散実行機能そのものが無い**（詳細は次節）。

## 推奨と、判断が動く条件

**第 1 候補は Locust + confluent-kafka。** 理由は性能ではなくリスクの低さと整合性にある。3 候補とも payload は再現できることが実測で確定した以上、差がつくのは「未検証の要素がどれだけ残るか」で、Locust は payload 生成が実 producer コードそのもの・Kafka クライアントも同一・リポジトリの言語とも一致し、残リスクが producer 用途では限定的な gevent の 1 点に絞られる。

**broker の resources は固定値ではない点に注意する。** dev の broker `cpu_limit` 500m × 3 台は固定の天井ではなく、TODO.md の最優先課題そのものが「broker/controller の resources を stg/prd 向けにチューニングする」ことである。この測定の目的はその天井を今後変えていくための判断材料を得ることにあるため、「現在の天井に対して 1 インスタンスで足りるか」ではなく、**天井が未確定で今後上がっていく前提で、負荷生成側がそれに追従してスケールできるか**が候補選定の必須条件になる。

これは 3 候補の評価に直接効く。

- **Locust**: worker を追加するだけで水平にスケールする。追加の基盤（K8s クラスタ等）を要さず、EC2 台数や ECS タスク数を増やすだけで済む。**集計方式が正しい**。worker は個々のレイテンシ値ではなく応答時間のヒストグラム（丸め済み）を master に送り、master はバケットごとに件数を足し算してマージしてから percentile を計算する（[locust/stats.py](https://github.com/locustio/locust/blob/master/locust/stats.py) の `StatsEntry.extend()` で確認）。「各 worker の p95 を平均する」方式とは異なり数学的に正しい全体 p95 を導出でき、Web UI が直近 10 秒のローリングウィンドウで実行中もライブ更新される。外部の Prometheus / Grafana を新たに配線しなくても、これが標準機能だけで揃う。ただし worker 数が非常に多い場合（実例で 250 worker 時に master CPU 100% の報告あり）は master 自体がボトルネックになり得るが、本検討の規模（dev の broker `cpu_limit` 500m × 3 台）では worker 数がその閾値に達する見込みは無い。
- **k6 + Go 拡張**: `--execution-segment` による手動分散（後述）であれば Locust と同じ EC2/ECS 基盤で K8s なしにスケールできる。ただしメトリクス集約（Prometheus remote write を自分で配線）と segment 分割の計算は自分で持つ必要があり、Locust ほど手離れが良くない。加えて Go 拡張のビルド運用コストは分散の有無に関わらず乗る。
- **Gatling OSS**: スケールする手段が無い（複数マシンへのオーケストレーションは Enterprise 限定）。DIY で複数プロセスを立てても結果の自動集約が無く、broker の天井が上がるたびに運用コストが増す。スケールが必須条件になった今、この弱点は無視できない。

判断が動く条件を明記しておく。

- **Gatling の `Array[Byte]` + Session 動的生成が PoC で確認でき、かつ「スケールが必要になった時点で Enterprise ライセンスか DIY 運用を追加で背負う」ことが許容できる場合** → Gatling に切り替える価値がある。ただし現時点でこの弱点は解消されない。
- **組織として k6 に標準化しており揃える必要がある場合** → k6 + Go 拡張を採る（分散は `--execution-segment` による手動分散で K8s なしに可能。判断材料は Go 拡張のビルド運用コストを引き受けられるかに絞られる）。
- **加えて k6-operator / Grafana 連携を宣言的なハーネスとして使いたい場合** → k6-operator を採る。この場合のみ、専用 K8s クラスタの新設コストが上乗せされる。

各候補の PoC は独立して小さく、いずれも 1 日程度で判定できる。

## 分散実行の詳細

broker の resources は今後チューニングされていくものであり、現時点で確定した天井ではない。この測定自体が「天井をどこまで上げられるか／どこで頭打ちになるか」を明らかにするために行うものなので、負荷生成側が broker の変化に追従してスケールできることは将来ではなく現時点で満たすべき条件になる。3 候補の分散実行機構を仕組みのレベルで比較する。

### Locust: master/worker

Locust は分散実行が組み込みの標準機能で、追加ツールが要らない。

- master と worker は TCP で通信する（既定ポート 5557。プロトコルの実装詳細は非公開情報のため未確認）。`--master` で起動した master がテストの開始/停止を worker に指示し、worker が生成した Users からの統計を送り返す。master 自身は負荷を生成しない（Web UI とテスト制御に専念する）。
- worker は 1 プロセス = 1 CPU コア対応が前提。Python は GIL の制約で 1 プロセスが複数コアを使い切れないため、コア数分の worker プロセスを立てる必要がある（同一ホストでも複数プロセスに分ければよい。`--processes` フラグでも代替可）。
- headless 実行では `--expect-workers N` で N 台の worker 接続を待ってからテストを開始できる。台数管理が明示的にできる。
- 統計は master 側にリアルタイムで集約され、Web UI と CSV エクスポートがそのまま使える。外部の集計基盤（Prometheus 等）を別途組む必要がない。
- 実行基盤としては EC2 複数台、ECS タスク複数、または deliveryhero の Helm chart で EKS 上に組める。いずれも master 1 + worker N の構成は変わらない。

**index 一意性の実装**: [locust/runners.py](https://github.com/locustio/locust/blob/master/locust/runners.py) で確認済みの通り、`LocalRunner.worker_index = 0` / `WorkerRunner.worker_index`（master から `ack` メッセージで配布される連番）がツール標準で手に入る。`make_envelope_payload(worker_index * STRIDE + local_counter)` のような stride 採番で重複を避けられる。追加の配線が不要という意味で 3 候補中もっとも簡単。

### k6: 2 つの分散経路がある

k6 自体は単なるバイナリ/コンテナで、**K8s に依存しない分散実行手段が公式に用意されている**。k6-operator という特定の選択肢に限った制約を k6 全体の制約と混同しないこと。

**経路 (a) 手動分散（`--execution-segment`、K8s 不要）**: k6 は `--execution-segment` / `--execution-segment-sequence` という CLI フラグを持ち、独立した複数プロセスに VU / 反復回数を重複なく分割して割り当てられる（[k6 options reference](https://grafana.com/docs/k6/latest/using-k6/k6-options/reference/)）。

```
k6 run --execution-segment '0:1/4'   --execution-segment-sequence '0,1/4,1/2,3/4,1' script.js  # instance 1
k6 run --execution-segment '1/4:1/2' --execution-segment-sequence '0,1/4,1/2,3/4,1' script.js  # instance 2
```

これは EC2 フリートや ECS Fargate の複数タスクに、Locust の worker と同じ発想でそのまま乗る。新規の K8s クラスタは不要で、Locust 用に用意する実行基盤（EC2 フリート / ECS Fargate）をそのまま共用できる。

instance 識別も、k6-operator を経由せず自分たちの CDK で ECS タスク定義 / EC2 起動テンプレートに直接環境変数を注入するため、後述する k6-operator 固有の環境変数注入バグ（[#207](https://github.com/grafana/k6-operator/issues/207)、[コミュニティフォーラム #106860](https://community.grafana.com/t/not-able-to-access-environment-variables-in-init-phase-k6-operator/106860)）や instance ID 未実装（[#709](https://github.com/grafana/k6-operator/issues/709)）はそもそも関係しない。これらは k6-operator の `TestRun` CRD が Pod に値を配る仕組みに起因する問題であり、自前で ECS/EC2 に環境変数を渡す経路には当てはまらない。

残るコストは、メトリクス集約に外部出力（Prometheus remote write 等）を自分で組む必要がある点。**これは省略できる選択肢ではない。** xk6-kafka は `kafka_writer_message_count` / `kafka_writer_error_count` / `kafka_writer_write_seconds`（Trend、p50/p95/p99）等の Kafka 固有メトリクスを組み込みで持つが（[pkg/kafka/stats.go](https://github.com/mostafa/xk6-kafka/blob/main/pkg/kafka/stats.go)）、外部出力なしの既定動作では各プロセスが自分の実行終了後にテキスト要約をコンソールへ出すだけで、次の 3 つの制約がある。

1. N 台分の値を合算する仕組みが無い
2. Trend は既に縮約済みの値のためインスタンス横断の正しい p95 を後から算出できない（N 台の p95 を平均しても真の全体 p95 にはならない）
3. 実行中は何も見えず、安全柵（投入総量やレートの監視）が効かない

`-o experimental-prometheus-rw` で本リポジトリの `kube-prometheus-stack`（[../cdk/manifests/monitoring](../cdk/manifests/monitoring)）に push すれば、`sum(rate(k6_kafka_writer_message_count_total[1m]))` のようなクエリで複数インスタンス分を合算したスループットを実行中にリアルタイムで見られ、同じ Grafana 上で broker 側の `MessagesInPerSec` と突き合わせられる。EKS クラスタ外の ECS/EC2 から remote write エンドポイントに到達させる経路は別途組む必要がある。

**経路 (b) k6-operator（K8s 必須、ターンキーだが重い）**: `TestRun` CRD の `parallelism: N` を指定すると、k6-operator が内部で経路 (a) と同じ `--execution-segment` の割り当てを自動化し、`separate: true` で各 Job を別ノードに配置する（[k6-operator ドキュメント](https://grafana.com/docs/k6/latest/set-up/set-up-distributed-k6/usage/configure-testrun-crd/)）。分割の自動化と引き換えに、instance ID の自動露出が無い（[#709](https://github.com/grafana/k6-operator/issues/709)、要望止まりで未実装）・環境変数注入に既知の不具合がある（[#207](https://github.com/grafana/k6-operator/issues/207)、[#106860](https://community.grafana.com/t/not-able-to-access-environment-variables-in-init-phase-k6-operator/106860)）という経路 (a) には無い制約が乗り、かつ専用の K8s クラスタ運用（後述の制約により `EksCdkStack` は使えないため新設）が要る。

K8s 運用を増やしたくないなら経路 (a) で足りる。k6-operator は K8s 上のハーネス（Grafana 連携・宣言的なテスト実行管理）に価値を見出す場合にのみ選ぶ。

### Gatling: OSS には無い

Gatling OSS（community edition）には分散実行のオーケストレーション機能が存在しない。複数マシンへの負荷分散、IP 配分、injector の死活監視、結果の自動集約は [Gatling Enterprise（有償）の機能](https://gatling.io/community-vs-enterprise)であり、OSS 版でこれをやるには「複数マシンで別々に Gatling プロセスを起動し、結果をユーザー側で手動突合する」DIY が必要になる（自動マージの仕組みは無い）。

broker の resources は今後上がっていく前提のため、分散実行が必要になるタイミングは「将来」ではなく「この測定を通じて broker を増強し始めた時点」であり、比較的早く訪れる。Gatling を選ぶ場合、その時点で Enterprise ライセンスか DIY 運用のどちらかを追加で背負うことになる。この弱点は本検討における Gatling の評価を大きく下げる要素である。

## 実行基盤

### 制約: EksCdkStack のクラスタ本体には一切手を加えない

負荷生成環境は `EksCdkStack` が管理する EKS クラスタとは別に用意する。`EksCdkStack` は本番と同じ構成を保つ必要があり、検証専用のノードグループ・taint・labels をこのクラスタに追加することは認められない。

具体的には [../cdk/ekscdk/constructs/eks_cluster.py](../cdk/ekscdk/constructs/eks_cluster.py) の `SystemNodeGroup` / `KafkaBrokerNodeGroup` / `KafkaControllerNodeGroup` の定義に、負荷生成用のノードグループやワークロードを追加しない。以下の選択肢はいずれもこの制約を満たすが、根拠を明示しておく。

- **EC2**: [../cdk/ekscdk/test_ec2_stack.py](../cdk/ekscdk/test_ec2_stack.py) は `TestEc2Stack` という別スタックが作る素の EC2 インスタンスで、EKS のノードグループではない。VPC は共有するが、EKS クラスタの定義自体には触れない。制約に適合する。
- **ECS Fargate**: EKS と無関係な別サービスなので自明に適合する。
- **EKS 別クラスタ**: 「別」を厳密に取る必要がある。`EksCdkStack` の `add_nodegroup_capacity` を増やす形（既存クラスタに検証用ノードグループを足す）は制約違反になる。採用する場合は新規の EKS クラスタリソースそのもの（別の CDK スタック、別のコントロールプレーン）として作る。

現時点の dev（broker `cpu_limit` 500m × 3 台）に対しては 1 インスタンスで到達できる見込みだが、broker 側は今後増強されていくため、負荷生成側は追加コストなく worker を増やしてスケールできる構成であることを最初から満たす必要がある。**単発の EC2 1 台だけを作って終わりにはしない。**

| 選択肢 | 評価 |
|---|---|
| **EC2 フリート**（推奨） | [../cdk/ekscdk/test_ec2_stack.py](../cdk/ekscdk/test_ec2_stack.py) を master 1 台 + worker N 台（台数はパラメータ化）に拡張する。同一 VPC の `PRIVATE_WITH_EGRESS` サブネット・`allow_all_outbound=True` で NLB に到達できる。`t4g.small` は負荷生成には小さいのでタイプ引き上げが必要。broker が増強されて負荷生成側が先に飽和するようになったら、台数パラメータを増やすだけで追従できる（K8s 等の新規基盤は不要）。 |
| **ECS Fargate**（代替） | クラスタ管理・AMI 管理が不要で、worker のスケールは Fargate タスクの desired count を増やすだけで済む。EC2 フリートより ASG 相当の配線が要らない分シンプルだが、master 1 タスクのサービスディスカバリ配線が新規に要る。 |
| **EKS 別クラスタ** | [deliveryhero の Locust Helm chart](https://github.com/deliveryhero/helm-charts/tree/master/stable/locust) が master/worker の配線を丸ごと持つ。ただし新規クラスタ 1 面のコントロールプレーンコストが常時乗るため、EC2 フリート／ECS Fargate で足りる間は過剰。 |

いずれもクラスタ外からの実行になるため internal listener（9092）経由は測定できず、NLB 経由（9094）のみになる。これは本番クライアントと同じ経路なので、測りたい対象としてはむしろ正しい。

**接続経路（同一 VPC 内到達 vs 別 VPC + VPC Endpoint Service 経由の PrivateLink）は今回の測定目的には効かない。** [SPEC.md#L66](SPEC.md#L66) の通りこのプロジェクトは VPC Endpoint Service + PrivateLink への到達可能性を認可境界にしているため経路の違いは認可モデル上の意味を持つが、本検討のスコープ（broker/controller resources のチューニング判断材料を得ること）で効くボトルネックは broker `cpu_limit` 500m・gp3 IOPS・t4g バーストクレジットであり、PrivateLink 1 ホップ分のオーバーヘッドはそれらに比べて無視できる。したがって**同一 VPC 内到達（既存 TestEc2Stack と同じ方式）を既定とする**。認可境界の妥当性そのものを検証したい、またはクライアント観測レイテンシを本番同等に精緻化したい場合にのみ、別 VPC + PrivateLink 経由への切り替えを再検討する。

**[../cdk/scripts/kafka-bastion.sh](../cdk/scripts/kafka-bastion.sh) の `kubectl port-forward` 経由では絶対に測らない。** 全トラフィックが EKS API server 経由の単一多重化ストリーム + socat 1 段を通るため、結果が Kafka ではなく port-forward の性能になる。

## 測定の正をどこに置くか

**produce レートの正は broker 側メトリクス**（`BytesInPerSec` / `MessagesInPerSec`。既存 Prometheus で取得済み）から取る。ツールを変えても比較可能性が保たれる。

Lambda 側は consumer group `<cluster_name>-lambda-esm` の lag（kafka-exporter → 既存 Grafana。`topicRegex: ".*"` / `groupRegex: ".*"` で有効）と Lambda の CloudWatch メトリクスで見る。

**Lambda ESM の並列度上限 = topic の partition 数**（[AWS Compute Blog](https://aws.amazon.com/blogs/compute/scaling-improvements-when-processing-apache-kafka-with-aws-lambda/)：「Total processors for all pollers can only scale up to the total number of partitions in the topic」）。現状 `partitions: 3` なので Lambda 側の天井は 3 並列。レバーは KafkaTopic CR の `partitions` 増加（増加のみ可、`cdk deploy` はユーザー作業）と `ProvisionedPollersConfig`（現在未設定）。

## フェーズ

**Phase 1: ESM 配管のベースライン測定**（Lambda 実装を待たずに着手できる）

現行のログ出力 Lambda に対して投入し、ESM の poller スケーリング挙動・partition 3 の天井・lag の伸び方を把握する。

**Phase 2: E2E 測定**（Lambda 実装後）

zlib 解凍 → protobuf → S3 Tables の Lambda が実装された後、同じ負荷構成で E2E を測る。Phase 1 との差分がアプリ処理と S3 Tables 書き込みのコストになる。Lambda 自体の設計は本調査の範囲外。

## 未決事項

**topic 名が食い違っている。** [../kafka2/producer/constants.py](../kafka2/producer/constants.py) の `TOPIC` は `sample-events-event`、ESM の購読先は `test-topic`（[../cdk/ekscdk/kafka_consumer_app_stack.py#L57](../cdk/ekscdk/kafka_consumer_app_stack.py#L57)）、S3 Tables のテーブル名は `sample_events_event`（[../cdk/ekscdk/s3tables_stack.py#L93](../cdk/ekscdk/s3tables_stack.py#L93)）。本実装の Lambda は `sample-events-event` を購読するのが筋に見える。**負荷側では解決せず topic をパラメータ化**し、既定は現に ESM が購読している `test-topic` とする。

## 安全柵

- **1 run の投入総量に上限を決める。** RF=3 / broker 3 台なので各 broker がほぼ全量を保持し、broker あたり storage は 20Gi しかない。実効書き込みは概算で「投入件数 × 230 B」が各 broker に載る。`test-topic` は KafkaTopic CR 管理下にあり `kafka-configs.sh` での retention 変更は Topic Operator に巻き戻されるため、総量を決めることが唯一の即効性ある柵。[DISK-ALERT.md](DISK-ALERT.md) の閾値も併せて確認する。
- **現行 Lambda は受信レコードを全件 INFO ログする。** Phase 1 の実コストとボトルネックは CloudWatch Logs の ingestion 側に出る可能性が高い。Phase 1 の投入総量は特に保守的に決める。
- **dev の broker `cpu_limit` は 500m**（[../cdk/ekscdk/config.py#L242](../cdk/ekscdk/config.py#L242)）。測れるのは「dev の絞られた設定の天井」であって「クラスタの天井」ではない。TODO.md のチューニングサイクル（測る → resources を上げる → 再測定）の初回測定として正しい位置づけ。
- **t4g は burstable。** sustained 負荷で CPU クレジットを使い切ると結果が時間とともに劣化する。broker 側・負荷生成側の両方で注意する。
- **gp3 は 125 MB/s / 3000 IOPS 固定**（StorageClass 固定値、サイズ拡張では変わらない）。RF=3 のため broker あたり EBS スループットが先に天井になる可能性がある。
- 負荷生成インスタンス自身の CPU が飽和していないことを毎回確認する。

## 実装時の見取り図（未着手）

- 新規: locustfile（`make_envelope_payload` を import する User クラス、`constant_throughput` によるレート制御、`worker_index` 起点の index 採番）
- 変更: [../cdk/ekscdk/test_ec2_stack.py](../cdk/ekscdk/test_ec2_stack.py) — インスタンスタイプ引き上げ、負荷ツール導入の `user_data`、master 1 台 + worker N 台（台数パラメータ化）へのフリート化

Test-First に従い、ロジックを持つ部分（index 採番）にはテストを書く。[../kafka2/consumer/pyproject.toml](../kafka2/consumer/pyproject.toml) の構成を踏襲する（`dependency-groups.dev` に `pytest` / `pytest-mock`、`testpaths = ["tests"]`、`pythonpath = ["."]`）。モックは `mocker` フィクスチャ経由。

**検証観点**:

- index 採番のテストが `uv run pytest` で通ること。先に red を確認してから実装に進む。
- 生成した payload を `zlib.decompress` → protobuf パースで復元し、値がメッセージごとに変わっていることを確認する（要件そのものの検証）。
- 小レート（例 100 msg/s × 数十秒）で実行し、broker メトリクスの `MessagesInPerSec` が目標レートに一致すること、エラーが 0 であることを確認する。
- consumer group `<cluster_name>-lambda-esm` の lag が Grafana に出ること、Lambda が起動して CloudWatch Logs にレコードが出ることを確認する。
- その後レートを段階的に上げ、broker CPU / EBS / lag のどれが先に飽和するかを記録する。
