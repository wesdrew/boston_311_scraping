from pydantic import BaseModel, ConfigDict


class InternalAttribute(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    label: str
    value: str
    name: str
    code: str | None = None
