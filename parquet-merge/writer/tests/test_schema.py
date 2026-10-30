import json
from pathlib import Path

import pyarrow as pa
import pytest

from schema import load_schema

CONTRACT_PATH = Path(__file__).resolve().parents[2] / "contract" / "staging_schema.json"


def test_contract_types_map_to_arrow_types(tmp_path: Path):
    path = tmp_path / "schema.json"
    path.write_text(
        json.dumps(
            {
                "columns": [
                    {"name": "a", "type": "string"},
                    {"name": "b", "type": "double"},
                    {"name": "c", "type": "timestamp_us_utc"},
                ]
            }
        )
    )

    assert load_schema(path) == pa.schema(
        [("a", pa.string()), ("b", pa.float64()), ("c", pa.timestamp("us", tz="UTC"))]
    )


def test_unknown_contract_type_is_rejected(tmp_path: Path):
    path = tmp_path / "schema.json"
    path.write_text(json.dumps({"columns": [{"name": "a", "type": "int128"}]}))

    with pytest.raises(KeyError):
        load_schema(path)


def test_real_contract_loads():
    assert load_schema(CONTRACT_PATH).names[0] == "device_id"
