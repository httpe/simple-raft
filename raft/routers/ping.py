from fastapi import APIRouter

from ..server import LocalHost
from ..logger import logger
from ..api import (
    PING_ENDPOINT,
    PONG_ENDPOINT,
    PongArg,
    PongResponse,
    PingArg,
    PingResponse,
)

router = APIRouter(tags=["Ping"])


@router.post(PONG_ENDPOINT)
async def pong(arg: PongArg, server: LocalHost) -> PongResponse:
    logger.info(f"Pong requested by {arg.requester}")
    resp = PongResponse(
        server_name=server.config.name,
        server_id=server.config.id,
        requester_name=arg.requester,
    )
    return resp


@router.post(PING_ENDPOINT)
async def ping(arg: PingArg, localhost: LocalHost) -> PingResponse:
    logger.info(f"Ping-ing {arg.server_name}")
    remote = localhost.plant.get_server(arg.server_name)
    remote_resp = await localhost.call(
        remote, "/pong", PongResponse, PongArg(requester=localhost.name)
    )
    logger.info(f"Pong respond received from {remote.name}")
    resp = PingResponse(
        server_name=localhost.config.name,
        server_id=localhost.config.id,
        remote_pong_response=remote_resp,
    )
    return resp
