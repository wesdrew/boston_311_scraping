from datetime import datetime

from pydantic import BaseModel, ConfigDict


class Note(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    datetime: datetime
    description: str
