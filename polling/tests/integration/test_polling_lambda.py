import json
from unittest.mock import MagicMock

import boto3
import pytest
from moto import mock_aws
from polling_lambda import poll_and_enqueue_response

from shared.boston_311_api.service_request import ServiceRequest
from shared.boston_311_api.service_request_response import ServiceRequestResponse

REGION = "us-east-1"


def _make_response(count: int) -> ServiceRequestResponse:
    return ServiceRequestResponse([ServiceRequest(service_request_id=str(i), status="open") for i in range(count)])


def _make_context() -> MagicMock:
    context = MagicMock()
    context.aws_request_id = "test-request-id"
    context.function_name = "test-function"
    context.function_version = "$LATEST"
    context.invoked_function_arn = "arn:aws:lambda:us-east-1:123456789:function:test"
    context.log_group_name = "/aws/lambda/test"
    context.log_stream_name = "2026/03/15/[$LATEST]abc123"
    return context


@pytest.fixture
def polling_client():
    mock = MagicMock()
    mock.get_service_requests.return_value = _make_response(3)
    return mock


@pytest.mark.integration
@mock_aws
def test_messages_enqueued_to_sqs(polling_client):
    sqs = boto3.client("sqs", region_name=REGION)
    sns = boto3.client("sns", region_name=REGION)

    queue_url = sqs.create_queue(QueueName="test-queue")["QueueUrl"]
    topic_arn = sns.create_topic(Name="test-topic")["TopicArn"]

    poll_and_enqueue_response(
        polling_client, sqs, sns, _make_context(), sqs_queue_url=queue_url, sns_topic_arn=topic_arn
    )

    messages = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10)["Messages"]
    assert len(messages) == 3
    bodies = [json.loads(m["Body"]) for m in messages]
    service_request_ids = {b["service_request_id"] for b in bodies}
    assert service_request_ids == {"0", "1", "2"}


@pytest.mark.integration
@mock_aws
def test_completed_event_published_to_sns(polling_client):
    sqs = boto3.client("sqs", region_name=REGION)
    sns = boto3.client("sns", region_name=REGION)

    queue_url = sqs.create_queue(QueueName="test-queue")["QueueUrl"]
    topic_arn = sns.create_topic(Name="test-topic")["TopicArn"]

    # Subscribe an SQS queue to the SNS topic to capture published messages
    capture_queue_url = sqs.create_queue(QueueName="sns-capture-queue")["QueueUrl"]
    capture_queue_arn = sqs.get_queue_attributes(
        QueueUrl=capture_queue_url,
        AttributeNames=["QueueArn"],
    )["Attributes"]["QueueArn"]
    sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=capture_queue_arn)

    poll_and_enqueue_response(
        polling_client, sqs, sns, _make_context(), sqs_queue_url=queue_url, sns_topic_arn=topic_arn
    )

    captured = sqs.receive_message(QueueUrl=capture_queue_url, MaxNumberOfMessages=1)["Messages"]
    assert len(captured) == 1
    envelope = json.loads(captured[0]["Body"])
    event = json.loads(envelope["Message"])
    assert event["event_type"] == "polling.completed"
    assert event["payload"]["polled_count"] == 3
    assert event["payload"]["enqueued_count"] == 3
    assert event["payload"]["failed_enqueued_count"] == 0
