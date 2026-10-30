import tracemalloc
import zlib

import pytest
from conftest import kafka_value, make_message

from decode import InvalidRecord, decode_message
from readings_pb2 import ReadingsMessage

MIB = 1024 * 1024


def test_message_at_exactly_the_limit_is_accepted():
    message = make_message()

    decoded = decode_message(kafka_value(message), len(message.SerializeToString()))

    assert decoded == message


def test_message_one_byte_over_the_limit_is_too_large():
    message = make_message()

    with pytest.raises(InvalidRecord) as error:
        decode_message(kafka_value(message), len(message.SerializeToString()) - 1)

    assert error.value.reason == "too_large"


def test_highly_compressed_payload_is_rejected_without_expanding_it():
    bomb = zlib.compress(b"\0" * (64 * MIB))
    assert len(bomb) < 1 * MIB

    tracemalloc.start()
    try:
        with pytest.raises(InvalidRecord) as error:
            decode_message(bomb, 1024)
        _, peak = tracemalloc.get_traced_memory()
    finally:
        tracemalloc.stop()

    assert error.value.reason == "too_large"
    assert peak < 4 * MIB


@pytest.mark.parametrize(
    "message", [make_message(device_id=""), make_message(message_id="")], ids=["device_id", "message_id"]
)
def test_empty_identity_field_is_invalid(message: ReadingsMessage):

    with pytest.raises(InvalidRecord) as error:
        decode_message(kafka_value(message), 1_000_000)

    assert error.value.reason == "invalid_field"
