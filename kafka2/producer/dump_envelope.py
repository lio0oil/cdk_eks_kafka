"""zlib 圧縮された Envelope バイナリを YAML として表示する動作確認用スクリプト。

send_from_file.py の逆方向。make_envelope_payload の出力や Kafka から取得したバイナリを
ファイルに保存しておき、その中身を目視確認したいときに使う。preserving_proto_field_name=True
で出力するため、ここで出た YAML はそのまま send_from_file.py の入力として使える。
"""

import argparse
import json
import logging
import zlib
from pathlib import Path
from typing import Any

import yaml
from google.protobuf.json_format import MessageToDict

from event_pb2 import Envelope  # pyright: ignore[reportAttributeAccessIssue]

logger = logging.getLogger("dump_envelope")

# proto3 の JSON 正準マッピングは int64 系フィールドを str 化する仕様のため
# (JS の Number 精度制限を避けるため MessageToDict/MessageToJson 側にも回避オプションはない)、
# JSON 出力でだけ元の int64 フィールド名を数値に戻す。YAML 側は str のままで問題ないため対象外。
_INT64_FIELD_NAMES = frozenset({"id", "userid", "historyid"})


def decode_payload(payload: bytes) -> dict[str, Any]:
    """zlib 展開して Envelope をデコードし、dict にする。

    always_print_fields_with_no_presence=True にしないと、int64 フィールドが 0 のとき
    proto3 の仕様で出力から省略されてしまう (id=0 と未設定の区別が YAML 上でつかなくなる)。
    """
    envelope = Envelope.FromString(zlib.decompress(payload))
    return MessageToDict(
        envelope,
        preserving_proto_field_name=True,
        always_print_fields_with_no_presence=True,
    )


def payload_to_yaml(payload: bytes) -> str:
    return yaml.safe_dump(decode_payload(payload), allow_unicode=True, sort_keys=False)


def _restore_int64_fields(value: Any) -> Any:
    if isinstance(value, dict):
        return {
            key: int(v) if key in _INT64_FIELD_NAMES else _restore_int64_fields(v) for key, v in value.items()
        }
    if isinstance(value, list):
        return [_restore_int64_fields(item) for item in value]
    return value


def payload_to_json(payload: bytes) -> str:
    data = _restore_int64_fields(decode_payload(payload))
    return json.dumps(data, ensure_ascii=False, indent=2)


def dump_file(path: Path, out: Path | None = None) -> None:
    """path のバイナリを YAML に変換する。out 指定時はファイルへ書き込み、未指定なら標準出力に表示する。"""
    yaml_text = payload_to_yaml(path.read_bytes())
    if out is not None:
        out.write_text(yaml_text, encoding="utf-8")
    else:
        print(yaml_text, end="")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="zlib 圧縮された Envelope バイナリを YAML に変換する",
    )
    parser.add_argument("file", type=Path, help="make_envelope_payload 等で生成したバイナリファイルのパス")
    parser.add_argument("--out", type=Path, default=None, help="書き込み先ファイルパス (省略時は標準出力)")
    return parser.parse_args(argv)


def main() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = parse_args()
    dump_file(args.file, args.out)


if __name__ == "__main__":
    main()
