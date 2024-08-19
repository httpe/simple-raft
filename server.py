from __future__ import annotations

import logging
import json
import argparse
from urllib.parse import urlunparse, urlencode
from collections import namedtuple
from contextlib import asynccontextmanager
from abc import ABC, abstractmethod
from typing_extensions import Self

from pydantic import BaseModel, PositiveInt, model_validator
from enum import Enum

from uvicorn.config import LOGGING_CONFIG
from fastapi import FastAPI
from fastapi.responses import RedirectResponse, Response
import uvicorn

import httpx


############################################
## Globals
############################################

plant: PlantConfig
server: ServerConfig
network: NetworkInterface


@asynccontextmanager
async def lifespan(app: FastAPI):
    global plant, server, network
    # This cannot be moved into __main__ as FastAPI will load the script without __main__ for sub-workers
    plant, server = get_server_settings()
    # Use proxy if set so
    if plant.use_proxy and plant.proxy is not None and server != plant.proxy:
        logger.warning(f"Using proxy: {plant.proxy.address}")
        network = HttpNetworkInterfaceWithProxy(proxy_addr=plant.proxy.address)
    else:
        network = HttpNetworkInterface()
    yield
    # TODO: cleanup
    pass


app = FastAPI(lifespan=lifespan)

logger = logging.getLogger("uvicorn.error")


############################################
## Communication Layer
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
    def __init__(self) -> None:
        super().__init__()

    def call(
        self, destination: NetworkAddress, endpoint: str, params: dict | None = None
    ) -> str:
        base_url = destination.construct_base_url(endpoint)
        logger.info(f"Calling {base_url} with params {params}")
        r = httpx.get(base_url, params=params)
        return r.text


class NetworkRequest(BaseModel):
    origin: NetworkAddress
    destination: NetworkAddress
    endpoint: str
    params: dict | None


class HttpNetworkInterfaceWithProxy(HttpNetworkInterface):
    def __init__(self, proxy_addr: NetworkAddress) -> None:
        super().__init__()
        self.proxy_addr = proxy_addr

    def call(
        self, destination: NetworkAddress, endpoint: str, params: dict | None = None
    ) -> str:
        base_proxy_url = self.proxy_addr.construct_base_url("/route")
        request = NetworkRequest(
            origin=server.address,
            destination=destination,
            endpoint=endpoint,
            params=params,
        )
        data = request.model_dump()
        logger.info(f"Sending request via proxy {self.proxy_addr.name}: {data}")
        r = httpx.post(base_proxy_url, json=data)
        return r.text


############################################
## APIs
############################################


@app.get("/")
async def get_root():
    """Will redirect you to the /docs endpoint"""
    return RedirectResponse("/docs")


@app.post("/route")
async def proxy_route(request_to_forward: NetworkRequest) -> Response:
    logger.info(
        f"Routing request from {request_to_forward.origin.name} to {request_to_forward.destination.name} for endpoint {request_to_forward.endpoint} with params {request_to_forward.params}"
    )
    r = network.call(
        destination=request_to_forward.destination,
        endpoint=request_to_forward.endpoint,
        params=request_to_forward.params,
    )
    return Response(content=r, media_type="application/json")


class PongResponse(BaseModel):
    server_name: str
    server_id: str
    requester_name: str | None


@app.get("/pong")
async def pong(requester: str | None = None) -> PongResponse:
    logger.info(f"Pong requested by {requester}")
    resp = PongResponse(
        server_name=server.name,
        server_id=server.id,
        requester_name=requester,
    )
    return resp


class PingResponse(BaseModel):
    server_name: str
    server_id: str
    remote_pong_response: PongResponse


@app.get("/ping/{server_name}")
async def ping(server_name: str) -> PingResponse:
    logger.info(f"Ping-ing {server_name}")
    destination = plant.get_server(server_name).address
    result_json = network.call(destination, "/pong", {"requester": server.name})
    logger.info(f"Pong respond received from {destination.name}")
    remote_resp = PongResponse.model_validate_json(result_json)
    resp = PingResponse(
        server_name=server.name,
        server_id=server.id,
        remote_pong_response=remote_resp,
    )
    return resp


############################################
## Logging
############################################


class LogLevel(Enum):
    debug = "debug"
    info = "info"
    warning = "warning"
    error = "error"
    critical = "critical"
    fatal = "fatal"


def get_uvicorn_log_config():
    log_config = LOGGING_CONFIG
    # Add timestamp to logging
    log_config["formatters"]["default"][
        "fmt"
    ] = "%(asctime)s %(levelprefix)s %(message)s"
    log_config["formatters"]["access"][
        "fmt"
    ] = "%(asctime)s %(levelprefix)s %(message)s"
    return log_config


############################################
## Server Config & Initialization
############################################


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


def parse_cml_args():
    parser = argparse.ArgumentParser(
        prog="Demo Server",
        description="Demo Server",
        epilog="Run a demo server",
    )
    parser.add_argument(
        "plant_config", help="Path to the plant configuration JSON file"
    )
    parser.add_argument(
        "server_name", help="Start the server of this name in the plant config"
    )
    args = parser.parse_args()
    return args


def get_server_settings():
    args = parse_cml_args()
    with open(args.plant_config, "r") as f:
        config = json.load(f)
    plant = PlantConfig(**config)
    current_server = plant.get_server(args.server_name)
    return plant, current_server


def main():
    _, server = get_server_settings()

    log_config = get_uvicorn_log_config()

    uvicorn.run(
        "server:app",
        port=server.port,
        log_level=server.log_level.value,
        log_config=log_config,
        reload=True,
    )


if __name__ == "__main__":
    main()
