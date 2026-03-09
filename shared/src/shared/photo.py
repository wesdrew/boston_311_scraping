from datetime import datetime
from typing import Optional

from pydantic import BaseModel, ConfigDict, HttpUrl


class Photo(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    media_url: HttpUrl
    title: Optional[str] = None
    created_at: Optional[datetime] = None
