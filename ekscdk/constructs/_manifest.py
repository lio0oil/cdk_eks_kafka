import yaml


def load_with_subs(path: str, **subs: str) -> dict:
    with open(path) as f:
        text = f.read()
    for placeholder, value in subs.items():
        text = text.replace(f"<{placeholder}>", value)
    return yaml.safe_load(text)


def load(path: str) -> dict:
    return load_with_subs(path)
