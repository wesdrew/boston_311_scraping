import hashlib
import json
import os

import boto3
from aws_lambda_powertools.logging import Logger
from aws_lambda_powertools.utilities.data_classes import SQSEvent, event_source
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from aws_lambda_powertools.utilities.typing import LambdaContext
from db.service_request_dao import ServiceRequestDAO, ServiceRequestSaveException, WriteResult
from mypy_boto3_sns import SNSClient
from pydantic import ValidationError
from shared.boston_311_api.attribute import Attribute
from shared.boston_311_api.extended_attributes import ExtendedAttributes
from shared.boston_311_api.note import Note
from shared.boston_311_api.photo import Photo
from shared.boston_311_api.service_request import ServiceRequest
from shared.constants import APP_EVENTS_TOPIC_ARN
from shared.internal.internal_attribute import InternalAttribute
from shared.internal.internal_extended_attributes import InternalExtendedAttributes
from shared.internal.internal_note import InternalNote
from shared.internal.internal_photo import InternalPhoto
from shared.internal.internal_service_request import InternalServiceRequest
from shared.notifications import AppEvent

logger = Logger()

sns_client: SNSClient = boto3.client("sns")


def map_batch_item_to_service_request(record: SQSRecord) -> tuple[str, ServiceRequest | None]:
    try:
        body = json.loads(record.body)
        return record.message_id, ServiceRequest.model_validate(body)
    except (json.JSONDecodeError, ValidationError):
        logger.exception(
            "Failed to map SQS record to ServiceRequest", message_id=record.message_id, record_body=record.body
        )
        return record.message_id, None


def collect_batch_failures(records: list[tuple[str, InternalServiceRequest | None]]) -> dict:
    return {"batchItemFailures": [{"itemIdentifier": record_id} for record_id, record in records if record is None]}


def map_photo_to_internal(photo: Photo | None) -> InternalPhoto | None:
    if photo is None:
        return None
    return InternalPhoto(
        media_url=photo.media_url,
        title=photo.title,
        created_at=photo.created_at,
    )


def map_attribute_to_internal(attribute: Attribute | None) -> InternalAttribute | None:
    if attribute is None:
        return None
    return InternalAttribute(
        label=attribute.label,
        value=attribute.value,
        name=attribute.name,
        code=attribute.code,
    )


def map_extended_attributes_to_internal(ext: ExtendedAttributes | None) -> InternalExtendedAttributes | None:
    if ext is None:
        return None
    return InternalExtendedAttributes(
        x=ext.x,
        y=ext.y,
        name=ext.name,
        first_name=ext.first_name,
        last_name=ext.last_name,
        email=ext.email,
        phone=ext.phone,
        photos=[map_photo_to_internal(p) for p in ext.photos],
    )


def map_note_to_internal(note: Note | None) -> InternalNote | None:
    if note is None:
        return None
    return InternalNote(
        datetime=note.datetime,
        description=note.description,
    )


def map_to_internal(service_request: ServiceRequest | None) -> InternalServiceRequest | None:
    if service_request is None:
        return None

    try:
        serialized = service_request.model_dump_json(exclude_none=False)
        event_hash = hashlib.sha256(serialized.encode()).hexdigest()
        return InternalServiceRequest(
            service_request_id=service_request.service_request_id,
            status=service_request.status,
            status_notes=service_request.status_notes,
            service_name=service_request.service_name,
            service_code=service_request.service_code,
            description=service_request.description,
            requested_datetime=service_request.requested_datetime,
            updated_datetime=service_request.updated_datetime,
            expected_datetime=service_request.expected_datetime,
            address=service_request.address,
            address_id=service_request.address_id,
            zipcode=service_request.zipcode,
            lat=service_request.lat,
            long=service_request.long,
            media_url=service_request.media_url,
            token=service_request.token,
            details=service_request.details,
            attributes=[map_attribute_to_internal(a) for a in service_request.attributes],
            extended_attributes=(
                [map_extended_attributes_to_internal(e) for e in service_request.extended_attributes]
                if isinstance(service_request.extended_attributes, list)
                else map_extended_attributes_to_internal(service_request.extended_attributes)
                if service_request.extended_attributes is not None
                else None
            ),
            notes=[map_note_to_internal(n) for n in service_request.notes],
            event_hash=event_hash,
        )
    except Exception:
        logger.exception(
            "Failed to map ServiceRequest to InternalServiceRequest",
            service_request_id=service_request.service_request_id,
        )
        return None


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


def _summarize_write_results(results: list[WriteResult]) -> dict:
    return {
        "rows_not_changed": sum(1 for r in results if r == WriteResult.NO_CHANGE),
        "rows_inserted": sum(1 for r in results if r == WriteResult.INSERTED),
        "rows_updated": sum(1 for r in results if r == WriteResult.UPDATED),
    }


def map_service_requests_to_internal_objects(
    event: SQSEvent, context: LambdaContext, sns_client: SNSClient, dao: ServiceRequestDAO
) -> dict:
    try:
        logger.info("Beginning mapping service requests")
        id_to_service_requests: list[tuple[str, ServiceRequest | None]] = [
            map_batch_item_to_service_request(record) for record in event.records
        ]
        id_to_internal_service_requests: list[tuple[str, InternalServiceRequest | None]] = [
            (record_id, map_to_internal(service_request)) for record_id, service_request in id_to_service_requests
        ]
        logger.info("Finished mapping service requests")
        write_results: list[WriteResult] = dao.upsert_service_requests(
            [sr for _, sr in id_to_internal_service_requests if sr is not None]
        )
        write_summary: dict[str, int] = _summarize_write_results(write_results)
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
    return map_service_requests_to_internal_objects(event, context, sns_client)
