from fastapi import APIRouter

from ..singleton import server
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
async def pong(arg: PongArg) -> PongResponse:
    logger.info(f"Pong requested by {arg.requester}")
    resp = PongResponse(
        server_name=server.config.name,
        server_id=server.config.id,
        requester_name=arg.requester,
    )
    return resp


@router.post(PING_ENDPOINT)
async def ping(arg: PingArg) -> PingResponse:
    logger.info(f"Ping-ing {arg.server_name}")
    remote = server.get_server(arg.server_name)
    resp = remote.call("/pong", PongResponse, PongArg(requester=server.name))
    logger.info(f"Pong respond received from {remote.name}")
    resp = PingResponse(
        server_name=server.config.name,
        server_id=server.config.id,
        remote_pong_response=resp,
    )
    return resp
