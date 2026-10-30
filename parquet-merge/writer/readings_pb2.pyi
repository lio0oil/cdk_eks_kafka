from google.protobuf.internal import containers as _containers
from google.protobuf import descriptor as _descriptor
from google.protobuf import message as _message
from collections.abc import Iterable as _Iterable, Mapping as _Mapping
from typing import ClassVar as _ClassVar, Optional as _Optional, Union as _Union

DESCRIPTOR: _descriptor.FileDescriptor

class ReadingsMessage(_message.Message):
    __slots__ = ("device_id", "device_code", "message_id", "sent_at", "readings")
    DEVICE_ID_FIELD_NUMBER: _ClassVar[int]
    DEVICE_CODE_FIELD_NUMBER: _ClassVar[int]
    MESSAGE_ID_FIELD_NUMBER: _ClassVar[int]
    SENT_AT_FIELD_NUMBER: _ClassVar[int]
    READINGS_FIELD_NUMBER: _ClassVar[int]
    device_id: str
    device_code: str
    message_id: str
    sent_at: int
    readings: _containers.RepeatedCompositeFieldContainer[Reading]
    def __init__(self, device_id: _Optional[str] = ..., device_code: _Optional[str] = ..., message_id: _Optional[str] = ..., sent_at: _Optional[int] = ..., readings: _Optional[_Iterable[_Union[Reading, _Mapping]]] = ...) -> None: ...

class Reading(_message.Message):
    __slots__ = ("metric", "value", "measured_at")
    METRIC_FIELD_NUMBER: _ClassVar[int]
    VALUE_FIELD_NUMBER: _ClassVar[int]
    MEASURED_AT_FIELD_NUMBER: _ClassVar[int]
    metric: str
    value: float
    measured_at: int
    def __init__(self, metric: _Optional[str] = ..., value: _Optional[float] = ..., measured_at: _Optional[int] = ...) -> None: ...
