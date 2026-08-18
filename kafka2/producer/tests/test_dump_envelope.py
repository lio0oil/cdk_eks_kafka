import json
import zlib
from pathlib import Path

import pytest
import yaml

import dump_envelope
import send_from_file
from constants import make_envelope_payload
from event_pb2 import Envelope  # pyright: ignore[reportAttributeAccessIssue]


class TestDecodePayload:
    def test_decodes_compressed_envelope(self) -> None:
        payload = make_envelope_payload(0)

        data = dump_envelope.decode_payload(payload)

        assert data["common"]["id"] == "0"
        assert data["user"]["username"] == "user-0"
        assert data["content"]["historyrecords"][0]["memo"] == "memo-0-0"

    def test_keeps_zero_value_fields(self) -> None:
        """proto3 はデフォルト値 (0) のフィールドを暗黙で省略するため、明示的に残す。"""
        payload = make_envelope_payload(0)

        data = dump_envelope.decode_payload(payload)

        assert "id" in data["common"]
        assert "userid" in data["user"]
        assert "historyid" in data["content"]["historyrecords"][0]


class TestPayloadToYaml:
    def test_roundtrips_through_send_from_file(self) -> None:
        payload = make_envelope_payload(1)

        yaml_text = dump_envelope.payload_to_yaml(payload)
        data = yaml.safe_load(yaml_text)
        rebuilt_payload = send_from_file.build_payload_from_dict(data)

        original = Envelope.FromString(zlib.decompress(payload))
        rebuilt = Envelope.FromString(zlib.decompress(rebuilt_payload))
        assert original == rebuilt


class TestPayloadToJson:
    def test_roundtrips_through_send_from_file(self) -> None:
        payload = make_envelope_payload(1)

        json_text = dump_envelope.payload_to_json(payload)
        data = json.loads(json_text)
        rebuilt_payload = send_from_file.build_payload_from_dict(data)

        original = Envelope.FromString(zlib.decompress(payload))
        rebuilt = Envelope.FromString(zlib.decompress(rebuilt_payload))
        assert original == rebuilt

    def test_int64_fields_are_numbers_including_zero(self) -> None:
        """MessageToDict は int64 系フィールドを str 化するが、JSON は数値のまま出す。"""
        payload = make_envelope_payload(0)

        json_text = dump_envelope.payload_to_json(payload)
        data = json.loads(json_text)

        assert data["common"]["id"] == 0
        assert isinstance(data["common"]["id"], int)
        assert data["user"]["userid"] == 0
        assert data["content"]["historyrecords"][0]["historyid"] == 0
        assert '"id": "0"' not in json_text


class TestDumpFile:
    def test_writes_yaml_to_stdout_when_out_omitted(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        path = tmp_path / "envelope.bin"
        path.write_bytes(make_envelope_payload(0))

        dump_envelope.dump_file(path)

        out = capsys.readouterr().out
        data = yaml.safe_load(out)
        assert data["user"]["username"] == "user-0"

    def test_writes_yaml_to_file_when_out_given(self, tmp_path: Path, capsys: pytest.CaptureFixture[str]) -> None:
        path = tmp_path / "envelope.bin"
        path.write_bytes(make_envelope_payload(0))
        out_path = tmp_path / "envelope.yaml"

        dump_envelope.dump_file(path, out_path)

        data = yaml.safe_load(out_path.read_text(encoding="utf-8"))
        assert data["user"]["username"] == "user-0"
        assert capsys.readouterr().out == ""
