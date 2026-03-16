import json
from unittest.mock import MagicMock
from uuid import uuid4

import boto3
import pytest
from botocore.exceptions import ClientError
from moto import mock_aws

from polling.polling_lambda import poll_and_enqueue_response
from tests.helpers import make_context, make_response

REGION = "us-east-1"
STACK_NAME = "Boston311Polling-dev"


@pytest.fixture
def polling_client():
    mock = MagicMock()
    mock.get_service_requests.return_value = make_response(3)
    return mock


# ---------------------------------------------------------------------------
# Moto tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
@mock_aws
def test_messages_enqueued_to_sqs(polling_client):
    sqs = boto3.client("sqs", region_name=REGION)
    sns = boto3.client("sns", region_name=REGION)

    queue_url = sqs.create_queue(QueueName="test-queue")["QueueUrl"]
    topic_arn = sns.create_topic(Name="test-topic")["TopicArn"]

    poll_and_enqueue_response(
        polling_client, sqs, sns, make_context(), sqs_queue_url=queue_url, sns_topic_arn=topic_arn
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
        polling_client, sqs, sns, make_context(), sqs_queue_url=queue_url, sns_topic_arn=topic_arn
    )

    captured = sqs.receive_message(QueueUrl=capture_queue_url, MaxNumberOfMessages=1)["Messages"]
    assert len(captured) == 1
    envelope = json.loads(captured[0]["Body"])
    event = json.loads(envelope["Message"])
    assert event["event_type"] == "polling.completed"
    assert event["payload"]["polled_count"] == 3
    assert event["payload"]["enqueued_count"] == 3
    assert event["payload"]["failed_enqueued_count"] == 0


# ---------------------------------------------------------------------------
# Live AWS helpers and fixtures
# ---------------------------------------------------------------------------


def _drain_queue(sqs, queue_url: str) -> None:
    """Receive and delete all messages currently in the queue."""
    while True:
        resp = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=0)
        messages = resp.get("Messages", [])
        if not messages:
            break
        for msg in messages:
            sqs.delete_message(QueueUrl=queue_url, ReceiptHandle=msg["ReceiptHandle"])


@pytest.fixture(scope="module")
def dev_lambda():
    cfn = boto3.client("cloudformation", region_name=REGION)
    try:
        resources = cfn.list_stack_resources(StackName=STACK_NAME)["StackResourceSummaries"]
    except ClientError as e:
        if e.response["Error"]["Code"] == "ValidationError":
            pytest.skip(f"{STACK_NAME} stack not deployed")
        raise

    lambda_resource = next(
        (r for r in resources if r["ResourceType"] == "AWS::Lambda::Function"),
        None,
    )
    if lambda_resource is None:
        pytest.skip(f"No Lambda function found in {STACK_NAME}")

    function_name = lambda_resource["PhysicalResourceId"]
    lambda_client = boto3.client("lambda", region_name=REGION)
    config = lambda_client.get_function_configuration(FunctionName=function_name)
    env_vars = config["Environment"]["Variables"]

    return {
        "function_name": function_name,
        "queue_url": env_vars["SERVICE_REQUESTS_QUEUE_URL"],
        "topic_arn": env_vars["APP_EVENTS_TOPIC_ARN"],
    }


@pytest.fixture
def sns_capture_queue(dev_lambda):
    sqs = boto3.client("sqs", region_name=REGION)
    sns = boto3.client("sns", region_name=REGION)
    topic_arn = dev_lambda["topic_arn"]

    queue_name = f"boston311-test-{uuid4().hex[:8]}"
    queue_url = sqs.create_queue(QueueName=queue_name)["QueueUrl"]
    queue_arn = sqs.get_queue_attributes(QueueUrl=queue_url, AttributeNames=["QueueArn"])["Attributes"]["QueueArn"]

    sqs.set_queue_attributes(
        QueueUrl=queue_url,
        Attributes={
            "Policy": json.dumps(
                {
                    "Statement": [
                        {
                            "Effect": "Allow",
                            "Principal": {"Service": "sns.amazonaws.com"},
                            "Action": "sqs:SendMessage",
                            "Resource": queue_arn,
                            "Condition": {"ArnEquals": {"aws:SourceArn": topic_arn}},
                        }
                    ]
                }
            )
        },
    )

    subscription_arn = sns.subscribe(TopicArn=topic_arn, Protocol="sqs", Endpoint=queue_arn)["SubscriptionArn"]

    yield queue_url

    sns.unsubscribe(SubscriptionArn=subscription_arn)
    sqs.delete_queue(QueueUrl=queue_url)


# ---------------------------------------------------------------------------
# Live AWS tests
# ---------------------------------------------------------------------------


@pytest.mark.integration
def test_dev_lambda_polls_and_enqueues(dev_lambda, sns_capture_queue):
    sqs = boto3.client("sqs", region_name=REGION)
    lambda_client = boto3.client("lambda", region_name=REGION)

    queue_url = dev_lambda["queue_url"]
    function_name = dev_lambda["function_name"]

    _drain_queue(sqs, queue_url)

    response = lambda_client.invoke(FunctionName=function_name, InvocationType="RequestResponse")
    assert response["StatusCode"] == 200
    assert "FunctionError" not in response, response.get("FunctionError")

    # Collect all SQS messages (poll until a full empty pass)
    sqs_messages = []
    empty_passes = 0
    while empty_passes < 2:
        result = sqs.receive_message(QueueUrl=queue_url, MaxNumberOfMessages=10, WaitTimeSeconds=2)
        batch = result.get("Messages", [])
        if batch:
            sqs_messages.extend(batch)
            empty_passes = 0
        else:
            empty_passes += 1

    # Read SNS capture queue for the AppEvent
    captured = sqs.receive_message(QueueUrl=sns_capture_queue, MaxNumberOfMessages=1, WaitTimeSeconds=10)
    sns_messages = captured.get("Messages", [])
    assert len(sns_messages) == 1, "Expected exactly one polling.completed event on SNS"

    envelope = json.loads(sns_messages[0]["Body"])
    event = json.loads(envelope["Message"])
    assert event["event_type"] == "polling.completed"
    assert event["payload"]["failed_enqueued_count"] == 0
    enqueued_count = event["payload"]["enqueued_count"]

    assert len(sqs_messages) == enqueued_count

    _drain_queue(sqs, queue_url)
