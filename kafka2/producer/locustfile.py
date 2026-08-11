# Locust の負荷定義ファイル。ファイル名は locustfile.py 固定 (慣習ではなく Locust の既定探索名で、
# `-f` で明示しない限りこの名前のファイルをカレントディレクトリから探しに行く)。
# `locust -f locustfile.py` で起動すると、このファイル内の User サブクラス (下記 KafkaProducerUser)
# を Locust が自動検出し、Web UI またはヘッドレスモードで「仮想ユーザー」として並列実行する。
import itertools
import os
import time

from locust import User, constant_throughput, task

from constants import make_envelope_payload
from producer import build_producer

# event.proto の id/userid/historyid は int64 なので STRIDE=10**9 でもオーバーフローしない。
STRIDE = 10**9

BOOTSTRAP_SERVERS = os.environ.get("KAFKA_BOOTSTRAP_SERVERS", "localhost:9094")
# ESM が実際に購読している test-topic を既定にする。constants.TOPIC (sample-events-event) は
# 名称が食い違っているため使わない (.claude/PERFTEST.md 未決事項)。
TOPIC = os.environ.get("KAFKA_TOPIC", "test-topic")
# constant_throughput は「1 User あたり 1 秒間に何回 @task を実行するか」を指定する値。
# 全体の目標レートは これ × 起動する User 数 (`locust` コマンドの `-u` オプション) になる。
# 例: TARGET_RATE_PER_USER=10 で `-u 50` なら理論上 500 msg/s。
TARGET_RATE_PER_USER = 10.0

# Locust は 1 worker プロセス内に複数の User インスタンスを同時に生成する。
# カウンタを User インスタンス側 (self) に持つと同一 worker 内の複数 User が index を
# 衝突させてしまうため、プロセスに 1 つだけのモジュールスコープ変数として持つ。
_local_counter = itertools.count()


def compute_index(worker_index: int, local_counter: int) -> int:
    """worker 間で重複しない index を採番する (stride 採番)。

    worker_index は分散実行時の worker 識別子 (単体実行では常に 0)。
    worker ごとに STRIDE 分の index 空間をずらして割り当てることで、
    複数 worker が同時に送信しても index が重ならないようにする。
    """
    return worker_index * STRIDE + local_counter


class KafkaProducerUser(User):
    """Locust が検出する「仮想ユーザー」の定義。

    Locust は起動時にこのクラスのインスタンスを User 数分生成し、それぞれが
    独立した実行単位 (greenlet) として並行に動く。1 インスタンス = 1 仮想ユーザー。
    """

    # wait_time は「@task を 1 回実行したあと次の実行までどれだけ待つか」を Locust に
    # 教える callable。constant_throughput は待ち時間を動的に調整して目標スループットを
    # 維持しようとする戦略 (単純な固定 sleep ではない)。
    wait_time = constant_throughput(TARGET_RATE_PER_USER)

    def on_start(self) -> None:
        """この仮想ユーザーが動き始める直前に Locust が 1 回だけ呼ぶフック。

        User ごとに Producer を 1 つ持たせる (Kafka Producer はスレッドセーフだが、
        User 間で共有するとどの仮想ユーザーの送信か区別しづらくなるため分ける)。
        """
        self._producer = build_producer(BOOTSTRAP_SERVERS)
        # self.environment.runner は実行形態によって型が変わる:
        # 単体実行 (LocalRunner) では worker_index が存在せず実質 0 扱い、
        # 分散実行の worker プロセス (WorkerRunner) では master から配布された
        # 連番が入る。getattr で両対応する。
        self._worker_index = getattr(self.environment.runner, "worker_index", 0)

    def on_stop(self) -> None:
        """この仮想ユーザーが停止する直前に Locust が 1 回だけ呼ぶフック。"""
        self._producer.flush(timeout=10)

    @task
    def produce_envelope(self) -> None:
        """Locust が wait_time の間隔で繰り返し呼び出すタスク本体。

        @task を付けたメソッドが「1 回の負荷」の単位になる。同じクラスに複数
        @task があれば Locust がランダムに選んで実行するが、ここでは 1 種類のみ。
        """
        index = compute_index(self._worker_index, next(_local_counter))
        payload = make_envelope_payload(index)
        start = time.perf_counter()

        def _on_delivery(err, msg) -> None:
            # confluent-kafka の produce() は非同期で、実際に broker への配送が
            # 完了 (成功/失敗問わず) した時点でこのコールバックが呼ばれる。
            # そのため応答時間は produce() 呼び出し時刻からここまでの経過time。
            # events.request.fire() が Locust の集計対象になる唯一の入口で、
            # ここで fire しないと Web UI / CSV / percentile 計算に一切反映されない。
            self.environment.events.request.fire(
                request_type="produce",
                name=TOPIC,
                response_time=(time.perf_counter() - start) * 1000,
                response_length=0 if err is not None else len(payload),
                exception=Exception(str(err)) if err is not None else None,
            )

        try:
            self._producer.produce(TOPIC, key=str(index).encode("utf-8"), value=payload, on_delivery=_on_delivery)
        except BufferError as exc:
            # librdkafka 内部の送信待ちキューが満杯のときに produce() が同期的に
            # 送出する例外 (broker 側のエラーではない)。高レートで負荷をかけたときに
            # 実際に踏み込みたい飽和状態そのものなので、失敗として記録して処理を続ける。
            self.environment.events.request.fire(
                request_type="produce",
                name=TOPIC,
                response_time=(time.perf_counter() - start) * 1000,
                response_length=0,
                exception=exc,
            )
        finally:
            # poll() は delivery callback (_on_delivery) を実際に呼び出すための
            # librdkafka 側キュー処理。BufferError 時も poll() しないとキューが
            # 一切 drain されず、以降の produce() が永久に失敗し続けてしまう。
            self._producer.poll(0)
