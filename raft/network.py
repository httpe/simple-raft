from urllib.parse import urlunparse, urlencode
from collections import namedtuple
from abc import ABC, abstractmethod

import httpx
from pydantic import BaseModel, PositiveInt
from fastapi import status

from .logger import logger
from .singleton import singleton


############################################
## Network Exceptions
############################################


class NetworkException(Exception):
    pass


class NetworkTimeoutException(NetworkException):
    pass


class NetworkGeneralException(NetworkException):
    pass


############################################
## Network Interface
############################################


class NetworkAddress(BaseModel):
    name: str
    host: str | None
    port: PositiveInt

    def construct_base_url(self, path: str):
        if self.host is None:
            host = "127.0.0.1"
        else:
            host = self.host
        netloc = f"{host}:{self.port}"
        return components_to_url("http", netloc, path)


class NetworkRequest(BaseModel):
    origin: NetworkAddress
    destination: NetworkAddress
    endpoint: str
    params: dict | None


class NetworkInterface(ABC):
    @abstractmethod
    def call(
        self, destination: NetworkAddress, endpoint: str, params: dict | None = None
    ) -> str:
        pass


def components_to_url(
    scheme: str, netloc: str, path: str, query_params: dict | None = None
) -> str:
    components = namedtuple(
        typename="Components",
        field_names=["scheme", "netloc", "path", "params", "query", "fragment"],
    )
    url = urlunparse(
        components(
            scheme=scheme,
            netloc=netloc,
            query=urlencode(query_params) if query_params is not None else None,
            params="",
            path=path,
            fragment="",
        )
    )
    return str(url)


class HttpNetworkInterface(NetworkInterface):
    def __init__(self, self_addr: NetworkAddress) -> None:
        super().__init__()
        self.self_addr = self_addr

    def call(
        self, destination: NetworkAddress, endpoint: str, params: dict | None = None
    ) -> str:
        base_url = destination.construct_base_url(endpoint)
        logger.info(f"Calling {base_url} with params {params}")
        try:
            r = httpx.get(base_url, params=params, timeout=singleton.server.timeout)
        except httpx.TimeoutException as e:
            raise NetworkTimeoutException()
        except Exception as e:
            raise NetworkGeneralException() from e
        if r.status_code != status.HTTP_200_OK:
            raise NetworkGeneralException(r.status_code)
        return r.text
