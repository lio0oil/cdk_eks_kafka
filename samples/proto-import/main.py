"""import を含む生成コードの round-trip 動作確認。

gen/ を sys.path に載せることで、event_pb2.py 内の
`from common import types_pb2` が解決できる。
"""

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).parent / "gen"))

from google.protobuf.timestamp_pb2 import Timestamp

from common import types_pb2
from service import event_pb2

created_at = Timestamp()
created_at.FromJsonString("2026-06-12T00:00:00Z")

event = event_pb2.SampleEvent(
    id=1,
    name="sample",
    metadata=types_pb2.Metadata(source="demo", created_at=created_at),
    severity=types_pb2.WARN,
)

data = event.SerializeToString()
restored = event_pb2.SampleEvent.FromString(data)

print(f"serialized: {len(data)} bytes")
print(f"restored.metadata.source = {restored.metadata.source}")
print(f"restored.severity = {types_pb2.Severity.Name(restored.severity)}")
assert restored == event
print("round-trip OK")
