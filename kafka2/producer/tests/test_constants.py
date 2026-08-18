import zlib

from constants import HISTORY_RECORD_COUNT, build_envelope, make_envelope_payload
from event_pb2 import Envelope  # pyright: ignore[reportAttributeAccessIssue]


class TestBuildEnvelope:
    def test_fills_common_user_and_history_records(self) -> None:
        envelope = build_envelope(1)

        assert envelope.common.id == 1
        assert envelope.user.userid == 1
        assert envelope.user.username == "user-1"
        assert len(envelope.content.historyrecords) == HISTORY_RECORD_COUNT
        assert envelope.content.historyrecords[0].historyid == HISTORY_RECORD_COUNT
        assert envelope.content.historyrecords[0].memo == "memo-1-0"


class TestMakeEnvelopePayload:
    def test_values_change_between_indexes(self) -> None:
        payload_1 = make_envelope_payload(1)
        payload_2 = make_envelope_payload(2)

        envelope_1 = Envelope.FromString(zlib.decompress(payload_1))
        envelope_2 = Envelope.FromString(zlib.decompress(payload_2))

        assert envelope_1.common.id != envelope_2.common.id
        assert envelope_1.user.userid != envelope_2.user.userid
        assert envelope_1.user.username != envelope_2.user.username
        assert envelope_1.content.historyrecords[0].historyid != envelope_2.content.historyrecords[0].historyid
        assert envelope_1.content.historyrecords[0].memo != envelope_2.content.historyrecords[0].memo
