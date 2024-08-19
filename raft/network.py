from urllib.parse import urlunparse, urlencode
from collections import namedtuple

from abc import ABC, abstractmethod

import httpx
from pydantic import BaseModel, PositiveInt
from fastapi import APIRouter, Response

from .logger import logger
from .singleton import singleton

############################################
## Globals
############################################

router = APIRouter()

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
        r = httpx.get(base_url, params=params)
        return r.text


############################################
## Proxy
############################################


class HttpNetworkInterfaceWithProxy(HttpNetworkInterface):
    def __init__(self, self_addr: NetworkAddress, proxy_addr: NetworkAddress) -> None:
        super().__init__(self_addr=self_addr)
        self.proxy_addr = proxy_addr

    def call(
        self, destination: NetworkAddress, endpoint: str, params: dict | None = None
    ) -> str:
        base_proxy_url = self.proxy_addr.construct_base_url("/route")
        request = NetworkRequest(
            origin=self.self_addr,
            destination=destination,
            endpoint=endpoint,
            params=params,
        )
        data = request.model_dump()
        logger.info(f"Sending request via proxy {self.proxy_addr.name}: {data}")
        r = httpx.post(base_proxy_url, json=data)
        return r.text


@router.post("/route")
async def proxy_route(request_to_forward: NetworkRequest) -> Response:
    logger.info(
        f"Routing request from {request_to_forward.origin.name} to {request_to_forward.destination.name} for endpoint {request_to_forward.endpoint} with params {request_to_forward.params}"
    )
    r = singleton.network.call(
        destination=request_to_forward.destination,
        endpoint=request_to_forward.endpoint,
        params=request_to_forward.params,
    )
    return Response(content=r, media_type="application/json")
