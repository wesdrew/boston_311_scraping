from datetime import datetime, timezone

from request import ThreeOneOneRequest


def test_datetime_fields_serialize_to_z_format():
    dt = datetime(2026, 2, 21, 1, 42, 12, tzinfo=timezone.utc)
    request = ThreeOneOneRequest(start_date=dt)
    assert request.model_dump()["start_date"] == "2026-02-21T01:42:12Z"


def test_datetime_fields_serialize_none():
    request = ThreeOneOneRequest(start_date=None)
    assert request.model_dump()["start_date"] is None


def test_request_defaults_to_none():
    request = ThreeOneOneRequest()
    assert request.model_dump() == {
        "extensions": True,
        "q": None,
        "updated_after": None,
        "updated_before": None,
        "page": None,
        "per_page": None,
        "service_request_id": None,
        "service_code": None,
        "start_date": None,
        "end_date": None,
    }
