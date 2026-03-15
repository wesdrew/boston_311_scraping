from datetime import datetime, timezone

from pydantic import BaseModel, Field


class AppEvent(BaseModel):
    source: str
    event_type: str
    timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    payload: dict
