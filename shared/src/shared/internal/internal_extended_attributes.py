from pydantic import BaseModel, ConfigDict

from shared.internal.internal_photo import InternalPhoto


class InternalExtendedAttributes(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    x: float | str | None = None
    y: float | str | None = None
    name: str | None = None
    first_name: str | None = None
    last_name: str | None = None
    email: str | None = None
    phone: str | None = None
    photos: list[InternalPhoto] = []
