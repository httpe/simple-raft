from __future__ import annotations

import json
import argparse

from contextlib import asynccontextmanager

from fastapi import FastAPI
import uvicorn

from .logger import logger, get_uvicorn_log_config
from .network import (
    HttpNetworkInterface,
    HttpNetworkInterfaceWithProxy,
    router as network_router,
)
from .routers.ping import router as ping_router
from .configs import PlantConfig
from .singleton import singleton

############################################
## FastAPI App
############################################


@asynccontextmanager
async def lifespan(app: FastAPI):
    # This cannot be moved into __main__ as FastAPI will load the script without __main__ for sub-workers
    plant, server = get_server_settings()

    # Use proxy if set so
    if plant.use_proxy and plant.proxy is not None and server != plant.proxy:
        logger.warning(f"Using proxy: {plant.proxy.address}")
        network = HttpNetworkInterfaceWithProxy(
            self_addr=server.address, proxy_addr=plant.proxy.address
        )
    else:
        network = HttpNetworkInterface(self_addr=server.address)

    # set global variables
    singleton.init(plant, server, network)

    yield
    # TODO: cleanup
    pass


app = FastAPI(lifespan=lifespan)
app.include_router(network_router)
app.include_router(ping_router)

############################################
## Server Config & Initialization
############################################


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
        "raft.server:app",
        port=server.port,
        log_level=server.log_level.value,
        log_config=log_config,
        reload=True,
    )


if __name__ == "__main__":
    main()
