import os

import boto3
from aws_lambda_powertools.logging import Logger
from aws_lambda_powertools.utilities.data_classes import SQSEvent, event_source
from aws_lambda_powertools.utilities.typing import LambdaContext
from db.connection import create_connection
from db.service_request_dao import ServiceRequestDAO, ServiceRequestSaveException, WriteResult
from mypy_boto3_sns import SNSClient
from shared.boston_311_api.service_request import ServiceRequest
from shared.constants import APP_EVENTS_TOPIC_ARN
from shared.internal.internal_service_request import InternalServiceRequest
from shared.notifications import AppEvent

from consumer.mapper import map_batch_item_to_service_request, map_service_request_to_internal_service_request

logger = Logger()

sns_client: SNSClient = boto3.client("sns")
dao = ServiceRequestDAO(create_connection())


def collect_batch_failures(records: list[tuple[str, InternalServiceRequest | None]]) -> dict:
    return {"batchItemFailures": [{"itemIdentifier": record_id} for record_id, record in records if record is None]}


def summarize_write_results(results: list[WriteResult]) -> dict[str, int]:
    return {
        "rows_not_changed": sum(1 for r in results if r == WriteResult.NO_CHANGE),
        "rows_inserted": sum(1 for r in results if r == WriteResult.INSERTED),
        "rows_updated": sum(1 for r in results if r == WriteResult.UPDATED),
    }


def _create_mapper_complete_event(payload: dict, context: LambdaContext) -> AppEvent:
    return AppEvent(
        source="boston311.consumer",
        event_type="consumer.completed",
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


def _create_save_failed_event(exception: ServiceRequestSaveException, context: LambdaContext) -> AppEvent:
    return AppEvent(
        source="boston311.consumer",
        event_type="consumer.save_failed",
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


def _create_mapper_failed_event(exception: Exception, context: LambdaContext) -> AppEvent:
    return AppEvent(
        source="boston311.consumer",
        event_type="consumer.failed",
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


def map_service_requests_to_internal_objects(
    event: SQSEvent, context: LambdaContext, sns_client: SNSClient, dao: ServiceRequestDAO
) -> dict:
    try:
        logger.info("Beginning mapping service requests")
        id_to_service_requests: list[tuple[str, ServiceRequest | None]] = [
            map_batch_item_to_service_request(record) for record in event.records
        ]
        id_to_internal_service_requests: list[tuple[str, InternalServiceRequest | None]] = [
            (record_id, map_service_request_to_internal_service_request(service_request))
            for record_id, service_request in id_to_service_requests
        ]
        logger.info("Finished mapping service requests")
        write_results: list[WriteResult] = dao.upsert_service_requests(
            [sr for _, sr in id_to_internal_service_requests if sr is not None]
        )
        write_summary: dict[str, int] = summarize_write_results(write_results)
        logger.info("Persisted service requests", batch_size=len(id_to_internal_service_requests), **write_summary)
        batch_failures = collect_batch_failures(id_to_internal_service_requests)
        complete_event = _create_mapper_complete_event(
            payload={
                "batch_size": len(id_to_internal_service_requests),
                "failure_count": len(batch_failures["batchItemFailures"]),
                **write_summary,
            },
            context=context,
        )

        sns_client.publish(TopicArn=os.environ[APP_EVENTS_TOPIC_ARN], Message=complete_event.model_dump_json())
        return batch_failures
    except ServiceRequestSaveException as e:
        logger.exception("Failed to save service requests to the database")
        save_failed_event = _create_save_failed_event(e, context)
        sns_client.publish(TopicArn=os.environ[APP_EVENTS_TOPIC_ARN], Message=save_failed_event.model_dump_json())
        raise
    except Exception as e:
        logger.exception("Exception in consumer function")
        failed_event = _create_mapper_failed_event(e, context)
        sns_client.publish(TopicArn=os.environ[APP_EVENTS_TOPIC_ARN], Message=failed_event.model_dump_json())
        raise


@logger.inject_lambda_context
@event_source(data_class=SQSEvent)
def handler(event: SQSEvent, context: LambdaContext) -> dict:
    dao.ping()
    return map_service_requests_to_internal_objects(event, context, sns_client, dao)
