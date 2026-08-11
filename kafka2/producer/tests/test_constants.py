import zlib

from constants import make_envelope_payload
from event_pb2 import Envelope  # pyright: ignore[reportAttributeAccessIssue]


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
