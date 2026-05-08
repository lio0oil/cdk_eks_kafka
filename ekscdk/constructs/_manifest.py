import yaml


def load_with_subs(path: str, **subs: str) -> dict:
    with open(path) as f:
        text = f.read()
    for placeholder, value in subs.items():
        text = text.replace(f"<{placeholder}>", value)
    return yaml.safe_load(text)


def load(path: str) -> dict:
    return load_with_subs(path)


def load_all(path: str) -> list[dict]:
    with open(path) as f:
        return [doc for doc in yaml.safe_load_all(f) if doc is not None]
