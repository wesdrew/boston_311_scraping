from datetime import datetime

from pydantic import BaseModel, ConfigDict


class ServiceExtendedAttributes(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    active: bool | None = None
    notice: str | None = None
    updated_at: datetime | None = None
