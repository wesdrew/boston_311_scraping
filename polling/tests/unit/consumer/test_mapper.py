import json
from dataclasses import dataclass

from consumer.mapper import (
    map_attribute_to_internal,
    map_batch_item_to_service_request,
    map_extended_attributes_to_internal,
    map_note_to_internal,
    map_photo_to_internal,
    map_service_request_to_internal_service_request,
)
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

# -- helpers ------------------------------------------------------------------


@dataclass
class FakeSQSRecord:
    message_id: str
    body: str


def _make_record(message_id: str, body: dict | str) -> FakeSQSRecord:
    return FakeSQSRecord(
        message_id=message_id,
        body=json.dumps(body) if isinstance(body, dict) else body,
    )


def _make_service_request(**kwargs) -> ServiceRequest:
    defaults = {"service_request_id": "abc-123", "status": "open"}
    return ServiceRequest(**{**defaults, **kwargs})


# -- map_batch_item_to_service_request ----------------------------------------


def test_maps_valid_record_to_service_request():
    record = _make_record("msg-1", {"service_request_id": "sr-1", "status": "open"})

    message_id, service_request = map_batch_item_to_service_request(record)

    assert message_id == "msg-1"
    assert isinstance(service_request, ServiceRequest)
    assert service_request.service_request_id == "sr-1"


def test_returns_none_for_malformed_json():
    record = _make_record("msg-2", "not valid json{{{")

    message_id, service_request = map_batch_item_to_service_request(record)

    assert message_id == "msg-2"
    assert service_request is None


def test_returns_none_when_required_fields_missing():
    record = _make_record("msg-3", {"description": "missing required fields"})

    message_id, service_request = map_batch_item_to_service_request(record)

    assert message_id == "msg-3"
    assert service_request is None


def test_returns_none_for_empty_body():
    record = _make_record("msg-4", "")

    message_id, service_request = map_batch_item_to_service_request(record)

    assert message_id == "msg-4"
    assert service_request is None


def test_preserves_message_id_on_failure():
    record = _make_record("my-specific-id", "bad json")

    message_id, _ = map_batch_item_to_service_request(record)

    assert message_id == "my-specific-id"


# -- map_photo_to_internal ----------------------------------------------------


def test_maps_photo_to_internal():
    photo = Photo(media_url="https://example.com/photo.jpg", title="pothole", created_at="2024-01-01T00:00:00")

    result = map_photo_to_internal(photo)

    assert isinstance(result, InternalPhoto)
    assert str(result.media_url) == "https://example.com/photo.jpg"
    assert result.title == "pothole"
    assert result.created_at is not None


def test_map_photo_returns_none_for_none():
    assert map_photo_to_internal(None) is None


# -- map_attribute_to_internal ------------------------------------------------


def test_maps_attribute_to_internal():
    attribute = Attribute(label="Type", value="Pothole", name="type", code="PTHL")

    result = map_attribute_to_internal(attribute, "sr-1")

    assert isinstance(result, InternalAttribute)
    assert result.label == "Type"
    assert result.value == "Pothole"
    assert result.name == "type"
    assert result.code == "PTHL"
    assert len(result.row_hash) == 64


def test_map_attribute_returns_none_for_none():
    assert map_attribute_to_internal(None, "sr-1") is None


# -- map_extended_attributes_to_internal --------------------------------------


def test_maps_extended_attributes_to_internal():
    ext = ExtendedAttributes(
        x=42.36,
        y=-71.06,
        name="John Doe",
        first_name="John",
        last_name="Doe",
        email="john@example.com",
        phone="555-1234",
        photos=[],
    )

    result = map_extended_attributes_to_internal(ext, "sr-1")

    assert isinstance(result, InternalExtendedAttributes)
    assert result.x == 42.36
    assert result.y == -71.06
    assert result.name == "John Doe"
    assert result.email == "john@example.com"
    assert result.photos == []
    assert len(result.row_hash) == 64


def test_maps_extended_attributes_with_nested_photos():
    photo = Photo(media_url="https://example.com/photo.jpg")
    ext = ExtendedAttributes(photos=[photo])

    result = map_extended_attributes_to_internal(ext, "sr-1")

    assert len(result.photos) == 1
    assert isinstance(result.photos[0], InternalPhoto)


def test_map_extended_attributes_returns_none_for_none():
    assert map_extended_attributes_to_internal(None, "sr-1") is None


# -- map_note_to_internal -----------------------------------------------------


def test_maps_note_to_internal():
    note = Note(datetime="2024-01-15T10:30:00", description="Inspected site")

    result = map_note_to_internal(note)

    assert isinstance(result, InternalNote)
    assert result.description == "Inspected site"
    assert result.datetime is not None


def test_map_note_returns_none_for_none():
    assert map_note_to_internal(None) is None


# -- map_service_request_to_internal_service_request ----------------------------------------------------------


def test_maps_service_request_to_internal():
    sr = _make_service_request()

    result = map_service_request_to_internal_service_request(sr)

    assert isinstance(result, InternalServiceRequest)
    assert result.service_request_id == sr.service_request_id
    assert result.status == sr.status


def test_generates_event_hash():
    sr = _make_service_request()

    result = map_service_request_to_internal_service_request(sr)

    assert result.event_hash is not None
    assert len(result.event_hash) == 64  # sha256 hex digest


def test_same_service_request_produces_same_hash():
    sr = _make_service_request()

    result_1 = map_service_request_to_internal_service_request(sr)
    result_2 = map_service_request_to_internal_service_request(sr)

    assert result_1.event_hash == result_2.event_hash


def test_different_service_requests_produce_different_hashes():
    sr_1 = _make_service_request(service_request_id="sr-1")
    sr_2 = _make_service_request(service_request_id="sr-2")

    result_1 = map_service_request_to_internal_service_request(sr_1)
    result_2 = map_service_request_to_internal_service_request(sr_2)

    assert result_1.event_hash != result_2.event_hash


def test_returns_none_when_service_request_is_none():
    assert map_service_request_to_internal_service_request(None) is None


def test_returns_none_when_internal_mapping_raises(monkeypatch):
    sr = _make_service_request()

    def _raise(*args, **kwargs):
        raise Exception("hashing failed")

    monkeypatch.setattr("consumer.mapper.hashlib.sha256", _raise)
    result = map_service_request_to_internal_service_request(sr)

    assert result is None
