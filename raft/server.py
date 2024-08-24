from __future__ import annotations

import os

from contextlib import asynccontextmanager

from fastapi import FastAPI
import uvicorn

from .logger import logger, get_uvicorn_log_config, attach_log_file
from .proxy import router as network_router
from .routers.ping import router as ping_router
from .routers.tpc import router as tpc_router
from .singleton import server

############################################
## FastAPI App
############################################


@asynccontextmanager
async def lifespan(app: FastAPI):
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
## Server Config & Initialization
############################################


def main():
    log_config = get_uvicorn_log_config()

    uvicorn.run(
        "raft.server:app",
        port=server.config.port,
        log_level=server.config.log_level.value,
        log_config=log_config,
        reload=True,
        workers=1,
    )


if __name__ == "__main__":
    main()
