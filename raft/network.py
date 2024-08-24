from typing import TypeVar

from urllib.parse import urlunparse, urlencode
from collections import namedtuple
from abc import ABC, abstractmethod

import httpx
from pydantic import BaseModel, PositiveInt, NonNegativeFloat
from fastapi import status

from .logger import logger
from .api import PROXY_ROUTE_ENDPOINT

TResp = TypeVar("TResp", bound=BaseModel)

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
## Abstract Network Interface
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


class NetworkInterface(ABC):
    @abstractmethod
    def call(
        self,
        destination: NetworkAddress,
        endpoint: str,
        body: dict | None = None,
        timeout: NonNegativeFloat = 0,
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


############################################
## Default Interface
############################################


class HttpNetworkInterface(NetworkInterface):
    def __init__(self, self_addr: NetworkAddress) -> None:
        super().__init__()
        self.self_addr = self_addr

    def call(
        self,
        destination: NetworkAddress,
        endpoint: str,
        body: dict | None = None,
        timeout: NonNegativeFloat = 0,
    ) -> str:
        base_url = destination.construct_base_url(endpoint)
        logger.info(f"Calling {base_url} with body {body}")
        try:
            r = httpx.post(base_url, json=body, timeout=timeout)
        except httpx.TimeoutException as e:
            raise NetworkTimeoutException()
        except Exception as e:
            raise NetworkGeneralException() from e
        if r.status_code != status.HTTP_200_OK:
            raise NetworkGeneralException(r.status_code)
        return r.text


############################################
## Proxy Interface
############################################


class NetworkRequest(BaseModel):
    origin: NetworkAddress
    destination: NetworkAddress
    endpoint: str
    body: dict | None


class HttpNetworkInterfaceWithProxy(HttpNetworkInterface):
    def __init__(self, self_addr: NetworkAddress, proxy_addr: NetworkAddress) -> None:
        super().__init__(self_addr=self_addr)
        self.proxy_addr = proxy_addr

    def call(
        self,
        destination: NetworkAddress,
        endpoint: str,
        body: dict | None = None,
        timeout: NonNegativeFloat = 0,
    ) -> str:
        base_proxy_url = self.proxy_addr.construct_base_url(PROXY_ROUTE_ENDPOINT)
        request = NetworkRequest(
            origin=self.self_addr,
            destination=destination,
            endpoint=endpoint,
            body=body,
        )
        data = request.model_dump()
        logger.info(f"Sending request via proxy {self.proxy_addr.name}: {data}")
        try:
            r = httpx.post(base_proxy_url, json=data, timeout=timeout)
            if r.status_code != status.HTTP_200_OK:
                raise NetworkGeneralException(r.status_code)
            return r.text
        except httpx.TimeoutException as e:
            raise NetworkTimeoutException() from e
        except NetworkGeneralException as e:
            raise e
        except Exception as e:
            raise NetworkGeneralException() from e
