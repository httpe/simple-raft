from datetime import datetime

from pydantic import BaseModel
from fastapi import APIRouter

from ..singleton import server
from ..logger import logger
from ..api import (
    DataEntry,
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


# state holder
database: dict[str, DataEntry] = {}


def get_data(key: str):
    return database.get(key)


def get_db_size():
    return len(database)


def set_local(key: str, data: str | None):
    if data is None:
        entry = database[key]
        del database[key]
    else:
        entry = DataEntry(data=data, timestamp=datetime.now())
        database[key] = entry
    return entry


def set_sync(key: str, data: str | None):
    entry = set_local(key, data)

    # Propagate to all siblings
    siblings = server.siblings
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
async def tpc_get(arg: DBGetArg) -> DBGetResponse:
    logger.info(f"Database GET {arg.key}")
    entry = get_data(arg.key)
    db_size = get_db_size()
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
async def tpc_propagate(arg: DBPropagationArg) -> DBPropagateResponse:
    logger.info(f"Database PROPAGATE set {arg.key} to {arg.data}")
    set_local(arg.key, arg.data)
    db_size = get_db_size()
    resp = DBPropagateResponse(
        server_name=server.config.name,
        server_id=server.config.id,
        db_size=db_size,
        key=arg.key,
    )
    return resp


@router.post(TPC_SET_ENDPOINT)
async def tpc_set(arg: DBSetArg) -> DBSetResponse:
    logger.info(f"Database SET {arg.key} to {arg.data}")

    _, propagations = set_sync(arg.key, arg.data)
    db_size = get_db_size()

    resp = DBSetResponse(
        server_name=server.config.name,
        server_id=server.config.id,
        db_size=db_size,
        key=arg.key,
        propagations=propagations,
    )
    return resp
