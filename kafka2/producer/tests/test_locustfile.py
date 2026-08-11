from confluent_kafka import Producer
from pytest_mock import MockerFixture

import locustfile


class TestComputeIndex:
    def test_worker_zero_uses_local_counter_directly(self) -> None:
        assert locustfile.compute_index(0, 0) == 0
        assert locustfile.compute_index(0, 5) == 5

    def test_different_workers_produce_disjoint_ranges(self) -> None:
        assert locustfile.compute_index(1, 0) == locustfile.compute_index(0, 0) + locustfile.STRIDE

    def test_worker_range_does_not_overlap_next_worker(self) -> None:
        assert locustfile.compute_index(0, locustfile.STRIDE - 1) < locustfile.compute_index(1, 0)


class TestKafkaProducerUser:
    def _make_environment(self, mocker: MockerFixture, worker_index: int | None):
        runner = (
            mocker.Mock(spec=[])
            if worker_index is None
            else mocker.Mock(spec=["worker_index"], worker_index=worker_index)
        )
        return mocker.Mock(runner=runner, events=mocker.Mock())

    def _make_started_user(self, mocker: MockerFixture, worker_index: int = 0):
        mock_producer = mocker.Mock(spec=Producer)
        mocker.patch.object(locustfile, "build_producer", return_value=mock_producer)
        environment = self._make_environment(mocker, worker_index=worker_index)
        user = locustfile.KafkaProducerUser(environment)
        user.on_start()
        return user, mock_producer, environment

    def test_on_start_builds_producer_and_resolves_worker_index(self, mocker: MockerFixture) -> None:
        user, mock_producer, _ = self._make_started_user(mocker, worker_index=3)

        assert user._producer is mock_producer
        assert user._worker_index == 3

    def test_on_start_defaults_worker_index_to_zero_when_runner_lacks_attribute(self, mocker: MockerFixture) -> None:
        user, _, _ = self._make_started_user(mocker, worker_index=None)  # type: ignore[arg-type]

        assert user._worker_index == 0

    def test_produce_envelope_fires_success_request_event(self, mocker: MockerFixture) -> None:
        user, mock_producer, environment = self._make_started_user(mocker)

        user.produce_envelope()
        _, kwargs = mock_producer.produce.call_args
        kwargs["on_delivery"](None, mocker.Mock())

        environment.events.request.fire.assert_called_once()
        assert environment.events.request.fire.call_args.kwargs["exception"] is None
        mock_producer.poll.assert_called_once_with(0)

    def test_produce_envelope_fires_failure_request_event_on_buffer_error(self, mocker: MockerFixture) -> None:
        user, mock_producer, environment = self._make_started_user(mocker)
        mock_producer.produce.side_effect = BufferError("queue full")

        user.produce_envelope()

        environment.events.request.fire.assert_called_once()
        assert environment.events.request.fire.call_args.kwargs["exception"] is not None

    def test_produce_envelope_polls_even_when_buffer_error_raised(self, mocker: MockerFixture) -> None:
        user, mock_producer, _ = self._make_started_user(mocker)
        mock_producer.produce.side_effect = BufferError("queue full")

        user.produce_envelope()

        mock_producer.poll.assert_called_once_with(0)

    def test_on_stop_flushes_producer(self, mocker: MockerFixture) -> None:
        user, mock_producer, _ = self._make_started_user(mocker)

        user.on_stop()

        mock_producer.flush.assert_called_once_with(timeout=10)
