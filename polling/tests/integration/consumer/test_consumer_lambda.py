import json
import os
from unittest.mock import patch

import boto3
import pytest
from aws_lambda_powertools.utilities.data_classes import SQSEvent
from consumer.consumer_lambda import map_service_requests_to_internal_objects
from moto import mock_aws
from shared.constants import APP_EVENTS_TOPIC_ARN

from tests.helpers import make_context

REGION = "us-east-1"


# -- helpers ------------------------------------------------------------------


def _make_sqs_record(message_id: str, body: dict | str) -> dict:
    return {
        "messageId": message_id,
        "receiptHandle": f"receipt-{message_id}",
        "body": json.dumps(body) if isinstance(body, dict) else body,
        "attributes": {
            "ApproximateReceiveCount": "1",
            "SentTimestamp": "1609459200000",
            "SenderId": "123456789012",
            "ApproximateFirstReceiveTimestamp": "1609459200001",
        },
        "messageAttributes": {},
        "md5OfBody": "test-md5",
        "eventSource": "aws:sqs",
        "eventSourceARN": "arn:aws:sqs:us-east-1:123456789012:test-queue",
        "awsRegion": REGION,
    }


def _make_sqs_event(records: list[dict]) -> SQSEvent:
    return SQSEvent({"Records": records})


def _make_service_request_body(**kwargs) -> dict:
    defaults = {"service_request_id": "sr-1", "status": "open"}
    return {**defaults, **kwargs}


def _drain_and_parse_sns_messages(sqs, capture_queue_url: str) -> list[dict]:
    """Receive all messages from the SNS capture queue and parse the inner AppEvent."""
    captured = sqs.receive_message(QueueUrl=capture_queue_url, MaxNumberOfMessages=10).get("Messages", [])
    return [json.loads(json.loads(m["Body"])["Message"]) for m in captured]


# -- tests --------------------------------------------------------------------


@pytest.mark.integration
@mock_aws
def test_completed_event_published_for_valid_batch():
    sns = boto3.client("sns", region_name=REGION)
    sqs = boto3.client("sqs", region_name=REGION)
    topic_arn = sns.create_topic(Name="app-events")["TopicArn"]

    capture_queue_url = sqs.create_queue(QueueName="capture")["QueueUrl"]
    capture_queue_arn = sqs.get_queue_attributes(QueueUrl=capture_queue_url, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=capture_queue_arn)

    records = [
        _make_sqs_record("msg-1", _make_service_request_body(service_request_id="sr-1")),
        _make_sqs_record("msg-2", _make_service_request_body(service_request_id="sr-2")),
        _make_sqs_record("msg-3", _make_service_request_body(service_request_id="sr-3")),
    ]
    event = _make_sqs_event(records)

    with patch.dict(os.environ, {APP_EVENTS_TOPIC_ARN: topic_arn}):
        result = map_service_requests_to_internal_objects(event, make_context(), sns)

    assert result == {"batchItemFailures": []}

    events = _drain_and_parse_sns_messages(sqs, capture_queue_url)
    assert len(events) == 1
    assert events[0]["event_type"] == "consumer.completed"
    assert events[0]["source"] == "boston311.consumer"
    assert events[0]["payload"]["batch_size"] == 3
    assert events[0]["payload"]["failure_count"] == 0


@pytest.mark.integration
@mock_aws
def test_batch_failures_returned_and_counted_in_completed_event():
    sns = boto3.client("sns", region_name=REGION)
    sqs = boto3.client("sqs", region_name=REGION)
    topic_arn = sns.create_topic(Name="app-events")["TopicArn"]

    capture_queue_url = sqs.create_queue(QueueName="capture")["QueueUrl"]
    capture_queue_arn = sqs.get_queue_attributes(QueueUrl=capture_queue_url, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=capture_queue_arn)

    records = [
        _make_sqs_record("msg-ok", _make_service_request_body(service_request_id="sr-ok")),
        _make_sqs_record("msg-bad-1", "not valid json{{{"),
        _make_sqs_record("msg-bad-2", {"missing_required_fields": True}),
    ]
    event = _make_sqs_event(records)

    with patch.dict(os.environ, {APP_EVENTS_TOPIC_ARN: topic_arn}):
        result = map_service_requests_to_internal_objects(event, make_context(), sns)

    assert result == {
        "batchItemFailures": [
            {"itemIdentifier": "msg-bad-1"},
            {"itemIdentifier": "msg-bad-2"},
        ]
    }

    events = _drain_and_parse_sns_messages(sqs, capture_queue_url)
    assert len(events) == 1
    assert events[0]["event_type"] == "consumer.completed"
    assert events[0]["payload"]["batch_size"] == 3
    assert events[0]["payload"]["failure_count"] == 2


@pytest.mark.integration
@mock_aws
def test_all_records_fail_still_publishes_completed_event():
    sns = boto3.client("sns", region_name=REGION)
    sqs = boto3.client("sqs", region_name=REGION)
    topic_arn = sns.create_topic(Name="app-events")["TopicArn"]

    capture_queue_url = sqs.create_queue(QueueName="capture")["QueueUrl"]
    capture_queue_arn = sqs.get_queue_attributes(QueueUrl=capture_queue_url, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=capture_queue_arn)

    records = [
        _make_sqs_record("msg-1", "bad json"),
        _make_sqs_record("msg-2", "also bad"),
    ]
    event = _make_sqs_event(records)

    with patch.dict(os.environ, {APP_EVENTS_TOPIC_ARN: topic_arn}):
        result = map_service_requests_to_internal_objects(event, make_context(), sns)

    assert len(result["batchItemFailures"]) == 2

    events = _drain_and_parse_sns_messages(sqs, capture_queue_url)
    assert len(events) == 1
    assert events[0]["event_type"] == "consumer.completed"
    assert events[0]["payload"]["batch_size"] == 2
    assert events[0]["payload"]["failure_count"] == 2


@pytest.mark.integration
@mock_aws
def test_failed_event_published_and_exception_reraised_on_unexpected_error():
    sns = boto3.client("sns", region_name=REGION)
    sqs = boto3.client("sqs", region_name=REGION)
    topic_arn = sns.create_topic(Name="app-events")["TopicArn"]

    capture_queue_url = sqs.create_queue(QueueName="capture")["QueueUrl"]
    capture_queue_arn = sqs.get_queue_attributes(QueueUrl=capture_queue_url, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=capture_queue_arn)

    records = [_make_sqs_record("msg-1", _make_service_request_body())]
    event = _make_sqs_event(records)

    with (
        patch.dict(os.environ, {APP_EVENTS_TOPIC_ARN: topic_arn}),
        patch("consumer.consumer_lambda.collect_batch_failures", side_effect=RuntimeError("unexpected boom")),
        pytest.raises(RuntimeError, match="unexpected boom"),
    ):
        map_service_requests_to_internal_objects(event, make_context(), sns)

    events = _drain_and_parse_sns_messages(sqs, capture_queue_url)
    assert len(events) == 1
    assert events[0]["event_type"] == "consumer.failed"
    assert events[0]["source"] == "boston311.consumer"
    assert events[0]["payload"]["exception"]["type"] == "RuntimeError"
    assert events[0]["payload"]["exception"]["message"] == "unexpected boom"


@pytest.mark.integration
@mock_aws
def test_lambda_context_included_in_completed_event():
    sns = boto3.client("sns", region_name=REGION)
    sqs = boto3.client("sqs", region_name=REGION)
    topic_arn = sns.create_topic(Name="app-events")["TopicArn"]

    capture_queue_url = sqs.create_queue(QueueName="capture")["QueueUrl"]
    capture_queue_arn = sqs.get_queue_attributes(QueueUrl=capture_queue_url, AttributeNames=["QueueArn"])["Attributes"][
        "QueueArn"
    ]
    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=capture_queue_arn)

    records = [_make_sqs_record("msg-1", _make_service_request_body())]
    event = _make_sqs_event(records)
    context = make_context()

    with patch.dict(os.environ, {APP_EVENTS_TOPIC_ARN: topic_arn}):
        map_service_requests_to_internal_objects(event, context, sns)

    events = _drain_and_parse_sns_messages(sqs, capture_queue_url)
    lambda_context = events[0]["payload"]["lambda_context"]
    assert lambda_context["aws_request_id"] == context.aws_request_id
    assert lambda_context["function_name"] == context.function_name
    assert lambda_context["function_version"] == context.function_version
    assert lambda_context["invoked_function_arn"] == context.invoked_function_arn
