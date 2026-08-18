import json
import zlib
from pathlib import Path

import pytest
import yaml
from confluent_kafka import Producer
from pytest_mock import MockerFixture

import send_from_file
from event_pb2 import Envelope  # pyright: ignore[reportAttributeAccessIssue]

ENVELOPE_DICT = {
    "common": {"id": 1, "datetime": "2026-08-18T00:00:00+00:00"},
    "user": {"userid": 1, "username": "user-1"},
    "content": {
        "historyrecords": [
            {
                "historyid": 1,
                "datetime": "2026-08-18T00:00:00+00:00",
                "memo": "memo-1",
                "classification": "classification-0",
            }
        ]
    },
}


class TestLoadEnvelopeDict:
    def test_reads_json(self, tmp_path: Path) -> None:
        path = tmp_path / "envelope.json"
        path.write_text(json.dumps(ENVELOPE_DICT), encoding="utf-8")

        assert send_from_file.load_envelope_dict(path) == ENVELOPE_DICT

    def test_reads_yaml(self, tmp_path: Path) -> None:
        path = tmp_path / "envelope.yaml"
        path.write_text(yaml.safe_dump(ENVELOPE_DICT), encoding="utf-8")

        assert send_from_file.load_envelope_dict(path) == ENVELOPE_DICT

    def test_raises_for_unsupported_extension(self, tmp_path: Path) -> None:
        path = tmp_path / "envelope.txt"
        path.write_text("{}", encoding="utf-8")

        with pytest.raises(ValueError, match="拡張子"):
            send_from_file.load_envelope_dict(path)


class TestBuildPayloadFromDict:
    def test_binds_dict_fields_into_envelope(self) -> None:
        payload = send_from_file.build_payload_from_dict(ENVELOPE_DICT)

        envelope = Envelope.FromString(zlib.decompress(payload))
        assert envelope.common.id == 1
        assert envelope.user.username == "user-1"
        assert envelope.content.historyrecords[0].memo == "memo-1"
        assert envelope.content.historyrecords[0].classification == "classification-0"

    def test_raises_for_unknown_field(self) -> None:
        with pytest.raises(Exception):  # noqa: B017  # google.protobuf.json_format.ParseError
            send_from_file.build_payload_from_dict({"common": {"unknown_field": 1}})


class TestSendFromFile:
    def test_produces_and_flushes(self, tmp_path: Path, mocker: MockerFixture) -> None:
        path = tmp_path / "envelope.json"
        path.write_text(json.dumps(ENVELOPE_DICT), encoding="utf-8")
        mock_producer = mocker.Mock(spec=Producer)
        mock_producer.flush.return_value = 0
        mocker.patch.object(send_from_file, "build_producer", return_value=mock_producer)

        send_from_file.send_from_file("localhost:9094", "test-topic", path, "0")

        args, kwargs = mock_producer.produce.call_args
        assert args[0] == "test-topic"
        assert kwargs["key"] == b"0"
        mock_producer.flush.assert_called_once_with(timeout=10)

    def test_raises_when_flush_leaves_messages_undelivered(self, tmp_path: Path, mocker: MockerFixture) -> None:
        path = tmp_path / "envelope.json"
        path.write_text(json.dumps(ENVELOPE_DICT), encoding="utf-8")
        mock_producer = mocker.Mock(spec=Producer)
        mock_producer.flush.return_value = 1
        mocker.patch.object(send_from_file, "build_producer", return_value=mock_producer)

        with pytest.raises(RuntimeError):
            send_from_file.send_from_file("localhost:9094", "test-topic", path, "0")
