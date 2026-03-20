import json
from unittest.mock import MagicMock

import pytest
from polling.polling_lambda import poll_and_enqueue_response, send_to_sqs
from shared.boston_311_api.service_request import ServiceRequest
from shared.boston_311_api.service_request_response import ServiceRequestResponse

from tests.helpers import make_context, make_response

QUEUE_URL = "https://sqs.us-east-1.amazonaws.com/123456789/test-queue"
TOPIC_ARN = "arn:aws:sns:us-east-1:123456789:test-topic"


# -- helpers ------------------------------------------------------------------


def _make_response_with_ids(*ids: str) -> ServiceRequestResponse:
    return ServiceRequestResponse([ServiceRequest(service_request_id=sid, status="open") for sid in ids])


def _poll(polling_client, sqs, sns, **kwargs):
    return poll_and_enqueue_response(
        polling_client,
        sqs,
        sns,
        make_context(),
        sqs_queue_url=kwargs.get("sqs_queue_url", QUEUE_URL),
        sns_topic_arn=kwargs.get("sns_topic_arn", TOPIC_ARN),
    )


# -- fixtures -----------------------------------------------------------------


@pytest.fixture
def polling_client():
    mock = MagicMock()
    mock.get_service_requests.return_value = make_response(3)
    return mock


@pytest.fixture
def sqs():
    mock = MagicMock()
    mock.send_message_batch.return_value = {"Successful": [], "Failed": []}
    return mock


@pytest.fixture
def sns():
    return MagicMock()


# -- send_to_sqs --------------------------------------------------------------


def test_sends_single_batch_when_under_10(sqs):
    result = send_to_sqs(sqs, QUEUE_URL, make_response(5))

    sqs.send_message_batch.assert_called_once()
    assert sqs.send_message_batch.call_args.kwargs["QueueUrl"] == QUEUE_URL
    assert len(sqs.send_message_batch.call_args.kwargs["Entries"]) == 5
    assert result == 0


def test_chunks_into_multiple_batches_when_over_10(sqs):
    send_to_sqs(sqs, QUEUE_URL, make_response(25))

    assert sqs.send_message_batch.call_count == 3
    batch_sizes = [len(call.kwargs["Entries"]) for call in sqs.send_message_batch.call_args_list]
    assert batch_sizes == [10, 10, 5]


def test_exactly_10_requests_sends_one_batch(sqs):
    send_to_sqs(sqs, QUEUE_URL, make_response(10))

    sqs.send_message_batch.assert_called_once()
    assert len(sqs.send_message_batch.call_args.kwargs["Entries"]) == 10


def test_returns_zero_when_no_failures(sqs):
    assert send_to_sqs(sqs, QUEUE_URL, make_response(3)) == 0


def test_returns_failure_count_from_single_batch():
    sqs = MagicMock()
    sqs.send_message_batch.return_value = {
        "Successful": [],
        "Failed": [
            {"Id": "1", "SenderFault": True, "Code": "InvalidParameterValue"},
            {"Id": "2", "SenderFault": True, "Code": "InvalidParameterValue"},
        ],
    }

    assert send_to_sqs(sqs, QUEUE_URL, make_response(5)) == 2


def test_accumulates_failure_counts_across_batches():
    sqs = MagicMock()
    sqs.send_message_batch.side_effect = [
        {"Successful": [], "Failed": [{"Id": "0", "SenderFault": True, "Code": "InvalidParameterValue"}]},
        {
            "Successful": [],
            "Failed": [
                {"Id": "10", "SenderFault": True, "Code": "InvalidParameterValue"},
                {"Id": "11", "SenderFault": True, "Code": "InvalidParameterValue"},
            ],
        },
    ]

    assert send_to_sqs(sqs, QUEUE_URL, make_response(15)) == 3


def test_empty_response_sends_no_batches():
    sqs = MagicMock()

    result = send_to_sqs(sqs, QUEUE_URL, make_response(0))

    sqs.send_message_batch.assert_not_called()
    assert result == 0


def test_entry_ids_match_service_request_ids(sqs):
    send_to_sqs(sqs, QUEUE_URL, _make_response_with_ids("abc-123", "def-456"))

    entries = sqs.send_message_batch.call_args.kwargs["Entries"]
    assert [e["Id"] for e in entries] == ["abc-123", "def-456"]


# -- poll_and_enqueue_response ------------------------------------------------


def test_happy_path_publishes_completed_event_at_end(polling_client, sqs, sns):
    _poll(polling_client, sqs, sns)

    sns.publish.assert_called_once()
    message = json.loads(sns.publish.call_args.kwargs["Message"])
    assert sns.publish.call_args.kwargs["TopicArn"] == TOPIC_ARN
    assert message["event_type"] == "polling.completed"
    assert message["payload"]["polled_count"] == 3
    assert message["payload"]["enqueued_count"] == 3
    assert message["payload"]["failed_enqueued_count"] == 0


def test_happy_path_sns_called_after_sqs(polling_client, sqs, sns):
    call_order = []
    sqs.send_message_batch.side_effect = lambda **_: call_order.append("sqs") or {"Successful": [], "Failed": []}
    sns.publish.side_effect = lambda **_: call_order.append("sns")

    _poll(polling_client, sqs, sns)

    assert call_order == ["sqs", "sns"]


def test_polling_client_failure_raises(sqs, sns):
    polling_client = MagicMock()
    polling_client.get_service_requests.side_effect = RuntimeError("API unavailable")

    with pytest.raises(RuntimeError, match="API unavailable"):
        _poll(polling_client, sqs, sns)

    sns.publish.assert_not_called()


def test_sqs_failure_raises(polling_client, sns):
    sqs = MagicMock()
    sqs.send_message_batch.side_effect = RuntimeError("SQS unavailable")

    with pytest.raises(RuntimeError, match="SQS unavailable"):
        _poll(polling_client, sqs, sns)

    sns.publish.assert_not_called()


def test_sns_failure_raises(polling_client, sqs):
    sns = MagicMock()
    sns.publish.side_effect = RuntimeError("SNS unavailable")

    with pytest.raises(RuntimeError, match="SNS unavailable"):
        _poll(polling_client, sqs, sns)


def test_failed_enqueue_count_reflected_in_completed_event(polling_client, sns):
    sqs = MagicMock()
    sqs.send_message_batch.return_value = {
        "Successful": [],
        "Failed": [{"Id": "0", "SenderFault": True, "Code": "InvalidParameterValue"}],
    }

    _poll(polling_client, sqs, sns)

    message = json.loads(sns.publish.call_args.kwargs["Message"])
    assert message["payload"]["polled_count"] == 3
    assert message["payload"]["enqueued_count"] == 2
    assert message["payload"]["failed_enqueued_count"] == 1
