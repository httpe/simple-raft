from pydantic import BaseModel
from fastapi import APIRouter, Request

from ..server import Server
from ..persist import PersistedStorage
from ..logger import logger
from ..api import (
    DBGetArg,
    DBGetResponse,
    DBSetArg,
    DBSetResponse,
    DBPropagateResponse,
    TPC_PREFIX,
    TPC_GET_ENDPOINT,
    TPC_SET_ENDPOINT,
)

router = APIRouter(tags=["Two Phase Commit"])

TPC_PROPAGATE_ENDPOINT = f"{TPC_PREFIX}/propagate"

##################################
# Core Logics
##################################


def set_sync(
    storage: PersistedStorage, key: str, data: str | None, siblings: list[Server]
):
    entry = storage.set_persisted(key, data)

    # Propagate to all siblings
    arg = DBPropagationArg(key=key, data=data)
    propagations: list[DBPropagateResponse] = []
    for remote in siblings:
        logger.info(f"Calling TPC PROPAGATE to {remote.config.address}")
        resp = remote.call(TPC_PROPAGATE_ENDPOINT, DBPropagateResponse, arg)
        logger.info(f"Received TPC PROPAGATE response from {remote.config.name}")
        propagations.append(resp)

    return entry, propagations


##################################
# Web APIs
##################################


@router.post(TPC_GET_ENDPOINT)
async def tpc_get(arg: DBGetArg, request: Request) -> DBGetResponse:
    logger.info(f"Database GET {arg.key}")

    server: Server = request.app.state.server
    storage: PersistedStorage = request.app.state.storage

    entry = storage.get_persisted(arg.key)

    db_size = len(storage)

    resp = DBGetResponse(
        server_name=server.config.name,
        server_id=server.config.id,
        key=arg.key,
        db_size=db_size,
        entry=entry,
    )
    return resp


class DBPropagationArg(BaseModel):
    key: str
    data: str | None


@router.post(TPC_PROPAGATE_ENDPOINT)
async def tpc_propagate(arg: DBPropagationArg, request: Request) -> DBPropagateResponse:
    logger.info(f"Database PROPAGATE set {arg.key} to {arg.data}")

    server: Server = request.app.state.server
    storage: PersistedStorage = request.app.state.storage

    storage.set_persisted(arg.key, arg.data)

    db_size = len(storage)

    resp = DBPropagateResponse(
        server_name=server.config.name,
        server_id=server.config.id,
        db_size=db_size,
        key=arg.key,
    )
    return resp


@router.post(TPC_SET_ENDPOINT)
async def tpc_set(arg: DBSetArg, request: Request) -> DBSetResponse:
    logger.info(f"Database SET {arg.key} to {arg.data}")

    server: Server = request.app.state.server
    storage: PersistedStorage = request.app.state.storage

    _, propagations = set_sync(storage, arg.key, arg.data, server.siblings)
    db_size = len(storage)

    resp = DBSetResponse(
        server_name=server.config.name,
        server_id=server.config.id,
        db_size=db_size,
        key=arg.key,
        propagations=propagations,
    )
    return resp
