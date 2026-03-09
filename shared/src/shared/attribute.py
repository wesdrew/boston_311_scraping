from typing import Optional

from pydantic import BaseModel, ConfigDict


class Attribute(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    label: str
    value: str
    name: str
    code: Optional[str] = None
