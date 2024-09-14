from fastapi import APIRouter

from ..server import LocalHost, implement_api
from ..logger import logger
from ..api import PONG, PongArg, PING, PingArg

router = APIRouter(tags=["Ping"])


@implement_api(router, PONG)
async def pong(arg: PongArg, localhost: LocalHost):
    logger.info(f"Pong requested by {arg.requester}")
    resp = PONG.ResponseClass(
        server_name=localhost.config.name,
        server_id=localhost.config.id,
        requester_name=arg.requester,
    )
    return resp


@implement_api(router, PING)
async def ping(arg: PingArg, localhost: LocalHost):
    logger.info(f"Ping-ing {arg.server_name}")
    remote = localhost.plant.get_server(arg.server_name)
    remote_resp = await localhost.call(
        remote,
        PONG,
        PONG.ArgumentClass(requester=localhost.name),
    )
    logger.info(f"Pong respond received from {remote.name}")
    resp = PING.ResponseClass(
        server_name=localhost.config.name,
        server_id=localhost.config.id,
        remote_pong_response=remote_resp,
    )
    return resp
