import os

import yaml

# ekscdk/constructs/ から見た manifests/ のルート
_MANIFESTS_ROOT = os.path.join(os.path.dirname(__file__), "..", "..", "manifests")

# external listener の broker N に割り当てる port の起点。
# - nodePort: 30095, 30096, ... (kube-proxy の許可帯域 30000-32767 内)
# - advertisedPort: 9095, 9096, ... (クライアントが接続する NLB 側 listener port)
# 既存ブローカーの値は破壊的変更（クライアント接続が壊れる）になるため変更しない。
# broker_count を増やすときは末尾に追加採番される。
_KAFKA_BROKER_NODE_PORT_START = 30095
_KAFKA_BROKER_LISTENER_PORT_START = 9095


def manifest_dir(*parts: str) -> str:
    """manifests/ 以下のディレクトリパスを返す。例: manifest_dir("kafka")"""
    return os.path.join(_MANIFESTS_ROOT, *parts)


def load_with_subs(base_dir: str, filename: str, **subs: str) -> dict:
    return yaml.safe_load(load_text_with_subs(base_dir, filename, **subs))


def load_text_with_subs(base_dir: str, filename: str, **subs: str) -> str:
    """テンプレート変数を置換した raw text を返す（YAML パースしない）。

    AMP Scraper の configurationBlob のように、YAML 文字列のまま base64 encode する
    用途で使う。YAML として読みたい場合は load / load_with_subs を使う。
    """
    with open(os.path.join(base_dir, filename)) as f:
        text = f.read()
    for placeholder, value in subs.items():
        text = text.replace(f"<{placeholder}>", value)
    return text


def load(base_dir: str, filename: str) -> dict:
    return load_with_subs(base_dir, filename)


def load_all(base_dir: str, filename: str) -> list[dict]:
    with open(os.path.join(base_dir, filename)) as f:
        return [doc for doc in yaml.safe_load_all(f) if doc is not None]


def _get_external_listener(base_dir: str) -> dict:
    manifest = load(base_dir, "kafka-cluster.yaml")
    try:
        listeners = manifest["spec"]["kafka"]["listeners"]
    except KeyError as e:
        raise ValueError(
            f"kafka-cluster.yaml に必須フィールドがありません: {e}。"
            "spec.kafka.listeners が定義されているか確認してください。"
        ) from e

    external = next((listener for listener in listeners if listener["name"] == "external"), None)
    if external is None:
        raise ValueError(
            "kafka-cluster.yaml に 'external' リスナーが見つかりません。"
            f"定義済みリスナー: {[listener['name'] for listener in listeners]}"
        )
    return external


def parse_kafka_external_listener(base_dir: str) -> tuple[str, int]:
    """kafka-cluster.yaml の external listener から (name, port) を返す。

    Strimzi が生成する per-broker NodePort Service の Service port は、
    すべて external listener の `port` と同じ値になる（NodePort は別）。
    また Service の port name は `tcp-<listener_name>` 形式で生成される。
    TargetGroupBinding の `serviceRef.port` には port name を使う方が、
    Kubernetes 慣習にも合うため、name を併せて返す。
    """
    external = _get_external_listener(base_dir)
    if "port" not in external:
        raise ValueError("kafka-cluster.yaml の external リスナーに 'port' が定義されていません。")
    return external["name"], external["port"]


def build_kafka_nlb_ports(base_dir: str, broker_count: int) -> list[tuple[str, int, int]]:
    """external listener の bootstrap 情報 + broker_count から
    (name, listener_port, node_port) のタプル列を構築する。

    - Bootstrap は kafka-cluster.yaml の external.port / bootstrap.nodePort から取得
    - Broker N は (_KAFKA_BROKER_LISTENER_PORT_START + N, _KAFKA_BROKER_NODE_PORT_START + N) を採番
    """
    external = _get_external_listener(base_dir)
    cfg = external.get("configuration")
    if cfg is None:
        raise ValueError("kafka-cluster.yaml の external リスナーに configuration が定義されていません。")
    if "bootstrap" not in cfg:
        raise ValueError("kafka-cluster.yaml の external.configuration に 'bootstrap' が定義されていません。")
    if "nodePort" not in cfg["bootstrap"]:
        raise ValueError("kafka-cluster.yaml の external.configuration.bootstrap に 'nodePort' が定義されていません。")
    if "port" not in external:
        raise ValueError("kafka-cluster.yaml の external リスナーに 'port' が定義されていません。")

    ports: list[tuple[str, int, int]] = [("Bootstrap", external["port"], cfg["bootstrap"]["nodePort"])]
    for i in range(broker_count):
        ports.append(
            (
                f"Broker{i}",
                _KAFKA_BROKER_LISTENER_PORT_START + i,
                _KAFKA_BROKER_NODE_PORT_START + i,
            )
        )
    return ports


def build_kafka_broker_configs(broker_count: int, advertised_host: str) -> list[dict]:
    """kafka-cluster.yaml の external.configuration.brokers[] に注入する dict 列を返す。

    キー挿入順（broker / advertisedHost / nodePort / advertisedPort）は従来 YAML 表記と同一にし、
    CDK が manifest を JSON 化するときの並びを保つ（CFN 差分・Strimzi 側の noop 判定を維持）。
    """
    return [
        {
            "broker": i,
            "advertisedHost": advertised_host,
            "nodePort": _KAFKA_BROKER_NODE_PORT_START + i,
            "advertisedPort": _KAFKA_BROKER_LISTENER_PORT_START + i,
        }
        for i in range(broker_count)
    ]
