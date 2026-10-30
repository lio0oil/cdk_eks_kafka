import zlib

from readings_pb2 import Reading, ReadingsMessage


def test_zlib_compressed_message_round_trips():
    message = ReadingsMessage(
        device_id="dev-1",
        device_code="code-a",
        message_id="3f2b8c1e-0000-4000-8000-000000000001",
        sent_at=1_788_000_000_123_456,
        readings=[
            Reading(metric="temperature", value=21.5, measured_at=1_787_999_999_000_001),
            Reading(metric="humidity", value=40.25, measured_at=1_787_999_999_000_002),
        ],
    )

    value = zlib.compress(message.SerializeToString())
    decoded = ReadingsMessage.FromString(zlib.decompress(value))

    assert decoded == message
    assert decoded.sent_at == 1_788_000_000_123_456
    assert [r.measured_at for r in decoded.readings] == [1_787_999_999_000_001, 1_787_999_999_000_002]
