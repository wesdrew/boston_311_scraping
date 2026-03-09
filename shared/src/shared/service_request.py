from datetime import datetime
from typing import Any, Dict, List, Optional

from pydantic import BaseModel, ConfigDict, HttpUrl

from shared.attribute import Attribute
from shared.extended_attributes import ExtendedAttributes
from shared.note import Note


class ServiceRequest(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    # Standard Open311 fields
    service_request_id: str
    status: str
    status_notes: Optional[str] = None
    service_name: Optional[str] = None
    service_code: Optional[str] = None
    description: Optional[str] = None
    requested_datetime: Optional[datetime] = None
    updated_datetime: Optional[datetime] = None
    expected_datetime: Optional[datetime] = None
    address: Optional[str] = None
    address_id: Optional[str] = None
    zipcode: Optional[str] = None
    lat: Optional[float] = None
    long: Optional[float] = None
    media_url: Optional[HttpUrl] = None

    # Boston Extensions
    token: Optional[str] = None
    details: Optional[Dict[str, Any]] = None
    attributes: Optional[List[Attribute]] = None
    extended_attributes: Optional[ExtendedAttributes] = None
    notes: Optional[List[Note]] = None
