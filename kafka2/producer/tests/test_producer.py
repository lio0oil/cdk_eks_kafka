import os

import pytest
from confluent_kafka import Producer
from pytest_mock import MockerFixture

import producer


class TestProduceOne:
    def test_produces_and_flushes(self, mocker: MockerFixture) -> None:
        mock_producer = mocker.Mock(spec=Producer)
        mock_producer.flush.return_value = 0
        mocker.patch.object(producer, "build_producer", return_value=mock_producer)

        producer.produce_one("localhost:9094", "test-topic", 0)

        args, kwargs = mock_producer.produce.call_args
        assert args[0] == "test-topic"
        assert kwargs["key"] == b"0"
        mock_producer.flush.assert_called_once_with(timeout=10)

    def test_raises_when_flush_leaves_messages_undelivered(self, mocker: MockerFixture) -> None:
        mock_producer = mocker.Mock(spec=Producer)
        mock_producer.flush.return_value = 1
        mocker.patch.object(producer, "build_producer", return_value=mock_producer)

        with pytest.raises(RuntimeError):
            producer.produce_one("localhost:9094", "test-topic", 0)


@pytest.mark.skipif("KAFKA_BOOTSTRAP_SERVERS" not in os.environ, reason="requires a reachable broker")
class TestProduceOneIntegration:
    def test_sends_one_message_without_raising(self) -> None:
        producer.produce_one(
            os.environ["KAFKA_BOOTSTRAP_SERVERS"],
            os.environ.get("KAFKA_TOPIC", "test-topic"),
            0,
        )
