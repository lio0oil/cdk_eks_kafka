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
