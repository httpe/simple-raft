from pydantic import BaseModel
from fastapi import APIRouter

from ..singleton import singleton
from ..logger import logger
from ..network import NetworkException

router = APIRouter(tags=["Ping"])


class PongResponse(BaseModel):
    server_name: str
    server_id: str
    requester_name: str | None


@router.get("/pong")
async def pong(requester: str | None = None) -> PongResponse:
    logger.info(f"Pong requested by {requester}")
    resp = PongResponse(
        server_name=singleton.server.name,
        server_id=singleton.server.id,
        requester_name=requester,
    )
    return resp


class PingResponse(BaseModel):
    server_name: str
    server_id: str
    remote_pong_response: PongResponse


@router.get("/ping/{server_name}")
async def ping(server_name: str) -> PingResponse:
    logger.info(f"Ping-ing {server_name}")
    destination = singleton.plant.get_server(server_name).address
    try:
        result_json = singleton.network.call(
            destination, "/pong", {"requester": singleton.server.name}
        )
    except NetworkException as e:
        logger.error(f"Ping encountered network exception: {e}")
        raise e
    logger.info(f"Pong respond received from {destination.name}")
    remote_resp = PongResponse.model_validate_json(result_json)
    resp = PingResponse(
        server_name=singleton.server.name,
        server_id=singleton.server.id,
        remote_pong_response=remote_resp,
    )
    return resp
