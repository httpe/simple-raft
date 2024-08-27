from __future__ import annotations

import os
import argparse
import json
from contextlib import asynccontextmanager

from fastapi import FastAPI
import uvicorn

from .configs import PlantConfig
from .logger import logger, get_uvicorn_log_config, attach_log_file
from .proxy import router as network_router
from .routers.ping import router as ping_router
from .routers.tpc import router as tpc_router
from .server import Server
from .network import (
    HttpNetworkInterface,
    HttpNetworkInterfaceWithProxy,
)

############################################
## Server Config & CMD Arg Parsing
############################################


def get_current_server(plant_config_path: str, server_name: str):
    with open(plant_config_path, "r") as f:
        config = json.load(f)
    plant = PlantConfig(**config)

    server = plant.get_server(server_name)
    logger.info(f"Start server: {server}")

    # Use proxy if set so
    if plant.use_proxy and plant.proxy is not None and server_name != plant.proxy.name:
        logger.warning(f"Using proxy: {plant.proxy.address}")
        network = HttpNetworkInterfaceWithProxy(
            self_addr=server.address, proxy_addr=plant.proxy.address
        )
    else:
        network = HttpNetworkInterface(self_addr=server.address)

    server = Server(plant, network, server_name)

    return server


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


# These must be here, uvicorn will launch the worker processes without __name__ == "__main__"
args = parse_cml_args()
server = get_current_server(args.plant_config, args.server_name)


############################################
## FastAPI App
############################################


@asynccontextmanager
async def lifespan(app: FastAPI):
    # global server context
    app.state.server = server

    # Setup logging
    log_path = os.path.join("logs", f"{server.name}.log")
    os.makedirs("logs", exist_ok=True)
    attach_log_file(log_path)
    logger.info(f"Will log to file: {log_path}")

    yield
    # TODO: cleanup
    pass


app = FastAPI(lifespan=lifespan)
app.include_router(network_router)
app.include_router(ping_router)
app.include_router(tpc_router)


############################################
## Main
############################################

if __name__ == "__main__":
    log_config = get_uvicorn_log_config()
    uvicorn.run(
        "raft.main:app",
        host="127.0.0.1",
        port=server.config.port,
        log_level=server.config.log_level.value,
        log_config=log_config,
        reload=True,
        workers=1,
    )
