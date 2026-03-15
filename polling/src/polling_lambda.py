import os
from datetime import datetime, timedelta, timezone
from itertools import batched

import boto3
from aws_lambda_powertools.logging import Logger
from aws_lambda_powertools.utilities.typing import LambdaContext
from client import ThreeOneOneClient, ThreeOneOneRequest
from counters import requests_ingested_counter
from mypy_boto3_sns import SNSClient
from mypy_boto3_sqs import SQSClient
from mypy_boto3_sqs.type_defs import SendMessageBatchResultTypeDef

from shared.boston_311_api.service_request_response import ServiceRequestResponse
from shared.constants import APP_EVENTS_TOPIC_ARN, POLLING_LOOKBACK_MINUTES, SERVICE_REQUESTS_QUEUE_URL
from shared.notifications import AppEvent

logger: Logger = Logger(service="polling")

BASE_URL = "https://311.boston.gov/open311/v2/requests.json"


client: ThreeOneOneClient = ThreeOneOneClient(BASE_URL)
sqs_client: SQSClient = boto3.client("sqs")
sns_client: SNSClient = boto3.client("sns")
LOOKBACK_MINUTES: int = int(os.environ.get(POLLING_LOOKBACK_MINUTES, 20))


def poll_and_enqueue_response(
    polling_client: ThreeOneOneClient,
    sqs: SQSClient,
    sns: SNSClient,
    context: LambdaContext,
    sqs_queue_url: str,
    sns_topic_arn: str,
    lookback_minutes: int = LOOKBACK_MINUTES,
) -> dict:
    logger.info("Starting polling function")
    requests_starting_at_datetime: datetime = datetime.now(timezone.utc) - timedelta(minutes=lookback_minutes)
    request: ThreeOneOneRequest = ThreeOneOneRequest(start_date=requests_starting_at_datetime)
    logger.info("Fetching service requests")
    response: ServiceRequestResponse = polling_client.get_service_requests(request)
    count: int = len(response.root)
    requests_ingested_counter.add(count)
    logger.info("Response from 311", count=count, response=response)
    failed_to_enqueue_count: int = send_to_sqs(sqs, sqs_queue_url, response)
    complete_event: AppEvent = _create_polling_complete_event(
        payload={
            "polled_count": count,
            "enqueued_count": count - failed_to_enqueue_count,
            "failed_enqueued_count": failed_to_enqueue_count,
        },
        context=context,
    )
    sns.publish(TopicArn=sns_topic_arn, Message=complete_event.model_dump_json())
    logger.info("Exiting polling function")
    return {"data": response.model_dump_json()}


def _create_polling_complete_event(payload: dict, context: LambdaContext) -> AppEvent:
    return AppEvent(
        source="boston311.polling",
        event_type="polling.completed",
        payload={
            **payload,
            "lambda_context": {
                "aws_request_id": context.aws_request_id,
                "function_name": context.function_name,
                "function_version": context.function_version,
                "invoked_function_arn": context.invoked_function_arn,
                "log_group_name": context.log_group_name,
                "log_stream_name": context.log_stream_name,
            },
        },
    )


def _create_polling_failed_event(exception: Exception, context: LambdaContext) -> AppEvent:
    return AppEvent(
        source="boston311.polling",
        event_type="polling.failed",
        payload={
            "exception": {
                "type": type(exception).__name__,
                "message": str(exception),
            },
            "lambda_context": {
                "aws_request_id": context.aws_request_id,
                "function_name": context.function_name,
                "function_version": context.function_version,
                "invoked_function_arn": context.invoked_function_arn,
                "log_group_name": context.log_group_name,
                "log_stream_name": context.log_stream_name,
            },
        },
    )


def send_to_sqs(sqs_client: SQSClient, queue_url: str, response: ServiceRequestResponse) -> int:
    entries = [{"Id": req.service_request_id, "MessageBody": req.model_dump_json()} for req in response.root]
    failed_count: int = 0
    for chunk in batched(entries, 10):
        result: SendMessageBatchResultTypeDef = sqs_client.send_message_batch(QueueUrl=queue_url, Entries=list(chunk))
        if result.get("Failed"):
            failed_count += len(result["Failed"])
            logger.error(
                "Failed to send messages to SQS",
                failed_ids=[e["Id"] for e in result["Failed"]],
            )
    return failed_count


@logger.inject_lambda_context
def handler(_event: dict, context: LambdaContext) -> dict:
    try:
        return poll_and_enqueue_response(
            client,
            sqs_client,
            sns_client,
            context,
            sqs_queue_url=os.environ[SERVICE_REQUESTS_QUEUE_URL],
            sns_topic_arn=os.environ[APP_EVENTS_TOPIC_ARN],
        )
    except Exception as e:
        logger.exception("Exception in polling function")
        failed_event: AppEvent = _create_polling_failed_event(e, context)
        sns_client.publish(TopicArn=os.environ[APP_EVENTS_TOPIC_ARN], Message=failed_event.model_dump_json())
        raise
