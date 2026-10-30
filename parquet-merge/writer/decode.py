import zlib

from google.protobuf.message import DecodeError

from readings_pb2 import ReadingsMessage

# Spark と Iceberg の timestamp が扱える範囲（0001-01-01 〜 9999-12-31）を Unix time のマイクロ秒で表したもの
MIN_MICROS = -62_135_596_800_000_000
MAX_MICROS = 253_402_300_799_999_999


class InvalidRecord(Exception):
    def __init__(self, reason: str):
        super().__init__(reason)
        self.reason = reason


def decode_message(data: bytes, max_decompressed_bytes: int) -> ReadingsMessage:
    decompressor = zlib.decompressobj()
    try:
        # 1 バイト余分に取り出して、上限ちょうどのサイズと上限超過を区別する
        raw = decompressor.decompress(data, max_decompressed_bytes + 1)
    except zlib.error as e:
        raise InvalidRecord("zlib_error") from e
    if len(raw) > max_decompressed_bytes:
        raise InvalidRecord("too_large")
    if not decompressor.eof:
        raise InvalidRecord("zlib_error")

    try:
        message = ReadingsMessage.FromString(raw)
    except DecodeError as e:
        raise InvalidRecord("decode_error") from e

    timestamps = [message.sent_at, *(r.measured_at for r in message.readings)]
    required = (message.device_id, message.message_id, message.device_code)
    if not all(required) or any(not MIN_MICROS <= t <= MAX_MICROS for t in timestamps):
        raise InvalidRecord("invalid_field")
    return message
