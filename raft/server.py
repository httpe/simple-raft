from typing import TypeVar, Type

from pydantic import BaseModel

from .configs import PlantConfig
from .network import (
    NetworkInterface,
)

TResp = TypeVar("TResp", bound=BaseModel)


class Server:
    def __init__(
        self, plant: PlantConfig, network: NetworkInterface, name: str
    ) -> None:
        self.plant = plant
        self.name = name
        self.config = plant.get_server(name)
        self.network = network

    def call(
        self, endpoint: str, resp_class: Type[TResp], arg: BaseModel | None = None
    ) -> TResp:

        if arg is None:
            body = None
        else:
            body = arg.model_dump()

        resp = self.network.call(
            self.config.address,
            endpoint,
            body=body,
            timeout=self.config.timeout,
        )

        return resp_class.model_validate_json(resp)

    @property
    def siblings(self):
        siblings = [
            self.__class__(self.plant, self.network, x.name)
            for x in self.plant.servers
            if x.name != self.name
        ]
        return siblings

    def get_server(self, name: str):
        return self.__class__(self.plant, self.network, name)
