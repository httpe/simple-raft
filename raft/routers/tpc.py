from pydantic import BaseModel
from fastapi import APIRouter

from ..singleton import singleton
from ..logger import logger
from ..network import NetworkException

router = APIRouter(prefix="/tpc", tags=["Two Phase Commit"])

# state holder
database: dict[str, str] = {}


class DBGetResponse(BaseModel):
    server_name: str
    server_id: str
    id: str
    db_size: int
    data: str | None


@router.get("/get/{id}")
async def tpc_get(id: str) -> DBGetResponse:
    logger.info(f"Database GET {id}")
    resp = DBGetResponse(
        server_name=singleton.server.name,
        server_id=singleton.server.id,
        id=id,
        db_size=len(database),
        data=database.get(id),
    )
    return resp


class DBPropagateResponse(BaseModel):
    server_name: str
    server_id: str
    id: str
    db_size: int


class DBSetResponse(BaseModel):
    server_name: str
    server_id: str
    id: str
    db_size: int
    propagations: list[DBPropagateResponse]


@router.get("/propagate/{id}")
async def tpc_propagate(id: str, data: str) -> DBPropagateResponse:
    logger.info(f"Database PROPAGATE set {id} to {data}")
    # Set local state
    database[id] = data
    resp = DBPropagateResponse(
        server_name=singleton.server.name,
        server_id=singleton.server.id,
        db_size=len(database),
        id=id,
    )
    return resp


@router.get("/set/{id}")
async def tpc_set(id: str, data: str) -> DBSetResponse:
    logger.info(f"Database SET {id} to {data}")

    # Set local state
    database[id] = data

    # Propagate to all siblings
    siblings = [x for x in singleton.plant.servers if x != singleton.server]
    quorum: list[DBPropagateResponse] = []
    for remote in siblings:
        destination = remote.address
        logger.info(f"Calling TPC PROPAGATE to {destination}")
        try:
            result_json = singleton.network.call(
                destination, f"{router.prefix}/propagate/{id}", {"data": data}
            )
        except NetworkException as e:
            logger.error(f"TPC PROPAGATE call encountered network exception: {e}")
            raise e
        logger.info(f"Received TPC PROPAGATE response from {destination.name}")
        remote_resp = DBPropagateResponse.model_validate_json(result_json)
        quorum.append(remote_resp)

    resp = DBSetResponse(
        server_name=singleton.server.name,
        server_id=singleton.server.id,
        db_size=len(database),
        id=id,
        propagations=quorum,
    )
    return resp
