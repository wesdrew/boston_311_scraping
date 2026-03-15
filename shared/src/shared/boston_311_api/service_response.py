from pydantic import RootModel

from shared.boston_311_api.service import Service


class ServiceResponse(RootModel):
    root: list[Service]
