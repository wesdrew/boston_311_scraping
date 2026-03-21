from datetime import datetime

from pydantic import BaseModel, ConfigDict


class InternalNote(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    datetime: datetime
    description: str
