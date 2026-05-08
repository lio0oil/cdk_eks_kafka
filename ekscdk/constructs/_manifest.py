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


def parse_kafka_nlb_ports(base_dir: str) -> list[tuple[str, int, int]]:
    """kafka-cluster.yaml の external listener から (name, listener_port, node_port) を返す。

    host プレースホルダーが残っていても yaml.safe_load は文字列として読めるため問題ない。
    """
    manifest = load(base_dir, "kafka-cluster.yaml")
    listeners = manifest["spec"]["kafka"]["listeners"]
    external = next((listener for listener in listeners if listener["name"] == "external"), None)
    if external is None:
        raise ValueError(
            "kafka-cluster.yaml に 'external' リスナーが見つかりません。"
            f"定義済みリスナー: {[l['name'] for l in listeners]}"
        )
    cfg = external.get("configuration")
    if cfg is None:
        raise ValueError("kafka-cluster.yaml の external リスナーに configuration が定義されていません。")
    return [("Bootstrap", external["port"], cfg["bootstrap"]["nodePort"])] + [
        (f"Broker{b['broker']}", b["advertisedPort"], b["nodePort"])
        for b in cfg["brokers"]
    ]
