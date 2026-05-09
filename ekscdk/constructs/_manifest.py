import os

import yaml

# ekscdk/constructs/ から見た manifests/ のルート
_MANIFESTS_ROOT = os.path.join(os.path.dirname(__file__), "..", "..", "manifests")


def manifest_dir(*parts: str) -> str:
    """manifests/ 以下のディレクトリパスを返す。例: manifest_dir("kafka")"""
    return os.path.join(_MANIFESTS_ROOT, *parts)


def load_with_subs(base_dir: str, filename: str, **subs: str) -> dict:
    with open(os.path.join(base_dir, filename)) as f:
        text = f.read()
    for placeholder, value in subs.items():
        text = text.replace(f"<{placeholder}>", value)
    return yaml.safe_load(text)


def load(base_dir: str, filename: str) -> dict:
    return load_with_subs(base_dir, filename)


def load_all(base_dir: str, filename: str) -> list[dict]:
    with open(os.path.join(base_dir, filename)) as f:
        return [doc for doc in yaml.safe_load_all(f) if doc is not None]


def parse_kafka_external_listener_port(base_dir: str) -> int:
    """kafka-cluster.yaml の external listener.port を返す。

    Strimzi が生成する per-broker NodePort Service の Service port は、
    すべて external listener の `port` と同じ値になる（NodePort は別）。
    TargetGroupBinding の `serviceRef.port` で使用する。
    """
    manifest = load(base_dir, "kafka-cluster.yaml")
    try:
        listeners = manifest["spec"]["kafka"]["listeners"]
    except KeyError as e:
        raise ValueError(
            f"kafka-cluster.yaml に必須フィールドがありません: {e}"
        ) from e
    external = next((listener for listener in listeners if listener["name"] == "external"), None)
    if external is None:
        raise ValueError(
            "kafka-cluster.yaml に 'external' リスナーが見つかりません。"
        )
    if "port" not in external:
        raise ValueError(
            "kafka-cluster.yaml の external リスナーに 'port' が定義されていません。"
        )
    return external["port"]


def parse_kafka_nlb_ports(base_dir: str) -> list[tuple[str, int, int]]:
    """kafka-cluster.yaml の external listener から (name, listener_port, node_port) を返す。

    host プレースホルダーが残っていても yaml.safe_load は文字列として読めるため問題ない。
    """
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
            f"定義済みリスナー: {[l['name'] for l in listeners]}"
        )

    cfg = external.get("configuration")
    if cfg is None:
        raise ValueError("kafka-cluster.yaml の external リスナーに configuration が定義されていません。")

    for required in ("bootstrap", "brokers"):
        if required not in cfg:
            raise ValueError(
                f"kafka-cluster.yaml の external.configuration に '{required}' が定義されていません。"
            )
    if "nodePort" not in cfg["bootstrap"]:
        raise ValueError(
            "kafka-cluster.yaml の external.configuration.bootstrap に 'nodePort' が定義されていません。"
        )

    ports: list[tuple[str, int, int]] = [("Bootstrap", external["port"], cfg["bootstrap"]["nodePort"])]
    for i, broker in enumerate(cfg["brokers"]):
        for field in ("broker", "advertisedPort", "nodePort"):
            if field not in broker:
                raise ValueError(
                    f"kafka-cluster.yaml の external.configuration.brokers[{i}] に '{field}' がありません。"
                )
        ports.append((f"Broker{broker['broker']}", broker["advertisedPort"], broker["nodePort"]))

    return ports
