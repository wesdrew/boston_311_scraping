from datetime import datetime

from pydantic import BaseModel, ConfigDict, HttpUrl


class InternalPhoto(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    media_url: HttpUrl
    title: str | None = None
    created_at: datetime | None = None
