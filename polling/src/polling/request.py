from datetime import datetime, timezone

from pydantic import BaseModel, field_serializer


class ThreeOneOneRequest(BaseModel):
    """
    Query parameters for the Boston 311 GET /requests endpoint.

    extensions:         extended metadata
    q:                  text search param
    updated_after:      returns requests updated after a specific timestamp
    updated_before:     returns requests updated before a specific timestamp
    page:               page number
    per_page:           per page
    service_request_id: specific request IDs
    service_code:       filter by the type of service
    start_date:         the earliest datetime to include
    end_date:           the latest datetime to include
    """

    extensions: bool | None = True
    q: str | None = None
    updated_after: datetime | None = None
    updated_before: datetime | None = None
    page: int | None = None
    per_page: int | None = None
    service_request_id: str | None = None
    service_code: str | None = None
    start_date: datetime | None = None
    end_date: datetime | None = None

    @field_serializer("updated_after", "updated_before", "start_date", "end_date")
    def serialize_datetime(self, value: datetime | None) -> str | None:
        if value is None:
            return None
        return value.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ")
