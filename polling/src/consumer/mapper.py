import hashlib
import json

from aws_lambda_powertools.logging import Logger
from aws_lambda_powertools.utilities.data_classes.sqs_event import SQSRecord
from pydantic import ValidationError
from shared.boston_311_api.attribute import Attribute
from shared.boston_311_api.extended_attributes import ExtendedAttributes
from shared.boston_311_api.note import Note
from shared.boston_311_api.photo import Photo
from shared.boston_311_api.service_request import ServiceRequest
from shared.internal.internal_attribute import InternalAttribute
from shared.internal.internal_extended_attributes import InternalExtendedAttributes
from shared.internal.internal_note import InternalNote
from shared.internal.internal_photo import InternalPhoto
from shared.internal.internal_service_request import InternalServiceRequest

logger = Logger()


def _hash(*parts: str | None) -> str:
    value = "|".join(p or "" for p in parts)
    return hashlib.sha256(value.encode()).hexdigest()


def map_batch_item_to_service_request(record: SQSRecord) -> tuple[str, ServiceRequest | None]:
    try:
        body = json.loads(record.body)
        return record.message_id, ServiceRequest.model_validate(body)
    except (json.JSONDecodeError, ValidationError):
        logger.exception(
            "Failed to map SQS record to ServiceRequest", message_id=record.message_id, record_body=record.body
        )
        return record.message_id, None


def map_photo_to_internal(photo: Photo | None) -> InternalPhoto | None:
    if photo is None:
        return None
    return InternalPhoto(
        media_url=photo.media_url,
        title=photo.title,
        created_at=photo.created_at,
    )


def map_attribute_to_internal(attribute: Attribute | None, service_request_id: str) -> InternalAttribute | None:
    if attribute is None:
        return None
    return InternalAttribute(
        label=attribute.label,
        value=attribute.value,
        name=attribute.name,
        code=attribute.code,
        row_hash=_hash(service_request_id, attribute.label, attribute.name, attribute.code, attribute.value),
    )


def map_extended_attributes_to_internal(
    ext: ExtendedAttributes | None, service_request_id: str
) -> InternalExtendedAttributes | None:
    if ext is None:
        return None
    x = str(ext.x) if ext.x is not None else None
    y = str(ext.y) if ext.y is not None else None
    return InternalExtendedAttributes(
        x=ext.x,
        y=ext.y,
        name=ext.name,
        first_name=ext.first_name,
        last_name=ext.last_name,
        email=ext.email,
        phone=ext.phone,
        photos=[map_photo_to_internal(p) for p in ext.photos],
        row_hash=_hash(service_request_id, x, y),
    )


def map_note_to_internal(note: Note | None) -> InternalNote | None:
    if note is None:
        return None
    return InternalNote(
        datetime=note.datetime,
        description=note.description,
    )


def map_service_request_to_internal_service_request(
    service_request: ServiceRequest | None,
) -> InternalServiceRequest | None:
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
            attributes=[
                map_attribute_to_internal(a, service_request.service_request_id) for a in service_request.attributes
            ],
            extended_attributes=(
                [
                    map_extended_attributes_to_internal(e, service_request.service_request_id)
                    for e in service_request.extended_attributes
                ]
                if isinstance(service_request.extended_attributes, list)
                else map_extended_attributes_to_internal(
                    service_request.extended_attributes, service_request.service_request_id
                )
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
