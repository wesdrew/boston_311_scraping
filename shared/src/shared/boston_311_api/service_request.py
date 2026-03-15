from datetime import datetime
from typing import Any

from pydantic import BaseModel, ConfigDict, HttpUrl

from shared.boston_311_api.attribute import Attribute
from shared.boston_311_api.extended_attributes import ExtendedAttributes
from shared.boston_311_api.note import Note


class ServiceRequest(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    # Standard Open311 fields
    service_request_id: str
    status: str
    status_notes: str | None = None
    service_name: str | None = None
    service_code: str | None = None
    description: str | None = None
    requested_datetime: datetime | None = None
    updated_datetime: datetime | None = None
    expected_datetime: datetime | None = None
    address: str | None = None
    address_id: str | None = None
    zipcode: str | None = None
    lat: float | None = None
    long: float | None = None
    media_url: HttpUrl | None = None

    # Boston Extensions
    token: str | None = None
    details: dict[str, Any] = {}
    attributes: list[Attribute] = []
    extended_attributes: list[ExtendedAttributes] | ExtendedAttributes | None = None
    notes: list[Note] = []
