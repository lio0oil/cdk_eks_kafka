import json
from pathlib import Path

import pyarrow as pa

ARROW_TYPES = {
    "string": pa.string(),
    "double": pa.float64(),
    "timestamp_us_utc": pa.timestamp("us", tz="UTC"),
}

DLQ_SCHEMA = pa.schema(
    [
        ("topic", pa.string()),
        ("partition", pa.int32()),
        ("offset", pa.int64()),
        ("reason", pa.string()),
        ("payload", pa.binary()),
    ]
)


def load_schema(path: Path) -> pa.Schema:
    columns = json.loads(path.read_text())["columns"]
    return pa.schema([(c["name"], ARROW_TYPES[c["type"]]) for c in columns])
