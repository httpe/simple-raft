from typing_extensions import Self

from pydantic import BaseModel, PositiveInt, model_validator
from .logger import LogLevel

from .network import NetworkAddress


class ServerConfig(BaseModel):
    name: str
    id: str
    host: str | None = None
    port: PositiveInt
    log_level: LogLevel

    @property
    def address(self):
        return NetworkAddress(name=self.name, host=self.host, port=self.port)


class PlantConfig(BaseModel):
    servers: list[ServerConfig]
    use_proxy: bool
    proxy: ServerConfig | None

    def get_server(self, name: str):
        if self.proxy is not None:
            if self.proxy.name == name:
                return self.proxy
        return next(x for x in self.servers if x.name == name)

    @model_validator(mode="after")
    def check_proxy(self) -> Self:
        if self.use_proxy and self.proxy is None:
            raise ValueError('if "use_proxy" is true, "proxy" must be defined')
        return self