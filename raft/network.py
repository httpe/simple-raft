from urllib.parse import urlunparse, urlencode
from collections import namedtuple
from asyncio import sleep
from abc import ABC, abstractmethod

import httpx
from pydantic import BaseModel, PositiveInt
from fastapi import APIRouter, Response, HTTPException, status

from .logger import logger
from .singleton import singleton

############################################
## Globals
############################################

router = APIRouter()


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
        try:
            r = httpx.post(base_proxy_url, json=data)
        except httpx.TimeoutException as e:
            raise NetworkTimeoutException() from e
        except Exception as e:
            raise NetworkGeneralException() from e
        if r.status_code != status.HTTP_200_OK:
            raise NetworkGeneralException(r.status_code)
        return r.text


@router.post("/route")
async def proxy_route(request_to_forward: NetworkRequest) -> Response:
    logger.info(
        f"Routing request from {request_to_forward.origin.name} to {request_to_forward.destination.name} for endpoint {request_to_forward.endpoint} with params {request_to_forward.params}"
    )

    process_proxy_drop_rule(request_to_forward)
    await process_proxy_timeout_rule(request_to_forward)

    r = singleton.network.call(
        destination=request_to_forward.destination,
        endpoint=request_to_forward.endpoint,
        params=request_to_forward.params,
    )

    return Response(content=r, media_type="application/json")


def process_proxy_drop_rule(request: NetworkRequest):
    # if (
    #     request.origin.name == "B"
    #     and request.destination.name == "A"
    #     and request.endpoint == "/pong"
    # ):
    #     raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY)
    pass


INFINITE_SLEEP_PERIOD = 60 * 60 * 24 * 30


async def process_proxy_timeout_rule(request: NetworkRequest):
    # if (
    #     request.origin.name == "B"
    #     and request.destination.name == "A"
    #     and request.endpoint == "/pong"
    # ):
    #     await sleep(INFINITE_SLEEP_PERIOD)
    pass
