from datetime import UTC, datetime

import pytest
from pydantic import ValidationError
from shared.boston_311_api.extended_attributes import ExtendedAttributes
from shared.boston_311_api.service_request import ServiceRequest


def test_service_request_parse() -> None:
    d: dict = {
        "service_request_id": "101006564843",
        "status": "open",
        "service_name": "Requests for Street Cleaning",
        "service_code": "Public Works Department:Street Cleaning:Requests for Street Cleaning",
        "description": "Street clean-up requested for Lagrange St. Extremely dirty and not taken care of.",
        "requested_datetime": "2026-03-11T02:00:11Z",
        "updated_datetime": "2026-03-11T02:00:52Z",
        "address": "47 55 Lagrange St, Boston, Ma, 02116",
        "lat": 42.351749,
        "long": -71.063998,
        "token": "6fd66811-d400-4f58-bb42-f697383038dc",
        "extended_attributes": {"x": "774021.32154270", "y": "2953507.51592370"},
    }

    s: ServiceRequest = ServiceRequest.model_validate(d)
    assert True


def test_service_request_optional_fields_none() -> None:
    ServiceRequest.model_validate(
        {
            "service_request_id": "test-id",
            "status": "open",
            "status_notes": None,
            "service_name": None,
            "service_code": None,
            "description": None,
            "requested_datetime": None,
            "updated_datetime": None,
            "expected_datetime": None,
            "address": None,
            "address_id": None,
            "zipcode": None,
            "lat": None,
            "long": None,
            "media_url": None,
            "token": None,
            "extended_attributes": None,
        }
    )

    assert True


def test_service_request_throws_validation_error_on_required_fields() -> None:
    with pytest.raises(ValidationError):
        ServiceRequest.model_validate({})
