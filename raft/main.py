from __future__ import annotations

import os
import argparse
import json
from contextlib import asynccontextmanager
import asyncio

from fastapi import FastAPI
import uvicorn
from httpx import AsyncClient

from .configs import PlantConfig
from .logger import logger, get_uvicorn_log_config, attach_log_file
from .proxy import router as network_router
from .routers.ping import router as ping_router
from .routers.abd import router as abd_router, ABDApi
from .routers.raft import router as raft_router, RaftApi
from .server import LocalServer
from .network import HttpNetworkInterface, HttpNetworkInterfaceWithProxy

############################################
## Server Config & CMD Arg Parsing
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


# These must be here, uvicorn will launch the worker processes without __name__ == "__main__"
args = parse_cml_args()

# Read plant config
with open(args.plant_config, "r") as f:
    config = json.load(f)
plant_config = PlantConfig(**config)

local_server_name = args.server_name

############################################
## FastAPI App
############################################


@asynccontextmanager
async def lifespan(app: FastAPI):
    # Setup logging
    log_path = os.path.join("logs", f"{local_server_name}.log")
    os.makedirs("logs", exist_ok=True)
    attach_log_file(log_path)
    logger.info(f"Will log to file: {log_path}")

    logger.info(f"Starting server: {local_server_name}")
    local_server_config = plant_config.get_server(local_server_name)

    # Initialize local network interface
    http_client = AsyncClient()
    # Use proxy if set so
    if (
        plant_config.use_proxy
        and plant_config.proxy is not None
        and local_server_name != plant_config.proxy.name
    ):
        logger.warning(f"Using proxy: {plant_config.proxy.address}")
        network = HttpNetworkInterfaceWithProxy(
            self_addr=local_server_config.address,
            proxy_addr=plant_config.proxy.address,
            http_client=http_client,
        )
    else:
        network = HttpNetworkInterface(
            self_addr=local_server_config.address, http_client=http_client
        )

    # global server context
    localhost = LocalServer(plant_config, local_server_name, network)
    app.state.localhost = localhost
    tasks = []

    # initialize consensus algorithms only in non-proxy nodes
    if not plant_config.use_proxy or (
        plant_config.proxy is not None and local_server_name != plant_config.proxy.name
    ):
        app.state.abd = ABDApi(localhost)
        app.state.raft = RaftApi(localhost)
        raft_task = asyncio.create_task(app.state.raft.start())
        tasks.append(raft_task)

    yield

    # clean up
    await http_client.aclose()
    for task in tasks:
        task.cancel()

    pass


app = FastAPI(lifespan=lifespan)
app.include_router(network_router)
app.include_router(ping_router)
app.include_router(abd_router)
app.include_router(raft_router)

############################################
## Main
############################################

if __name__ == "__main__":
    log_config = get_uvicorn_log_config()
    uvicorn.run(
        "raft.main:app",
        host="127.0.0.1",
        port=plant_config.get_server(local_server_name).port,
        log_level=plant_config.get_server(local_server_name).log_level.value,
        log_config=log_config,
        reload=True,
        workers=1,
        access_log=False,
    )
