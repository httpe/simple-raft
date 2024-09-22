# Attiya, Bar-Noy, Dolev (ABD) Quorum Get/Set Algorithm

from fastapi import APIRouter

from ..server import LocalHost, implement_api
from ..logger import logger
from ..api import (
    ABDDataEntry,
    ABDGetArg,
    ABDSetArg,
    ABDSetLocalArg,
    ABDGetLocalArg,
    ABD_GET,
    ABD_GET_LOCAL,
    ABD_SET,
    ABD_SET_LOCAL,
)

router = APIRouter(tags=["ABD"])

STORAGE_SECTION = "ABD"

##################################
# Utilities
##################################


def get_persisted(localhost: LocalHost, key: str) -> ABDDataEntry | None:
    persisted_entry = localhost.storage.get_persisted(STORAGE_SECTION, key)
    if persisted_entry is None:
        entry = None
    else:
        entry = ABDDataEntry.model_validate_json(persisted_entry.data)
    return entry


def set_persisted(localhost: LocalHost, key: str, entry: ABDDataEntry):
    localhost.storage.set_persisted(STORAGE_SECTION, key, entry.model_dump_json())


def get_local_logical_timestamp(localhost: LocalHost):
    ts = localhost.storage.get_persisted(STORAGE_SECTION, "LOCAL_LOGICAL_TIMESTAMP")
    if ts is None:
        return 0
    else:
        return int(ts.data)


def set_local_logical_timestamp(localhost: LocalHost, ts: int):
    localhost.storage.set_persisted(STORAGE_SECTION, "LOCAL_LOGICAL_TIMESTAMP", str(ts))


##################################
# Core Logics
##################################


async def set_quorum(localhost: LocalHost, key: str, data: str | None):
    # Bump logical ts for every set
    ts = get_local_logical_timestamp(localhost)
    ts += 1
    set_local_logical_timestamp(localhost, ts)

    # Propagate to all siblings
    entry = ABDDataEntry(data=data, key=key, logical_timestamp=ts)
    arg = ABDSetLocalArg(entry=entry)

    quorum_responses = await localhost.call_multiple(
        localhost.siblings,
        localhost.plant.quorum_count - 1,
        ABD_SET_LOCAL,
        arg,
    )

    # Set to local storage
    set_persisted(localhost, key, entry)

    return entry, quorum_responses


async def get_and_repair_quorum(localhost: LocalHost, key: str):
    # Quorum read
    get_arg = ABDGetLocalArg(key=key)
    siblings = localhost.siblings
    quorum_count = localhost.plant.quorum_count

    # ourself is always good
    quorum_get_responses = await localhost.call_multiple(
        siblings, quorum_count - 1, ABD_GET_LOCAL, get_arg
    )

    local_entry = get_persisted(localhost, key)
    latest_entry = local_entry
    max_logical_ts = -1 if latest_entry is None else latest_entry.logical_timestamp
    for r in quorum_get_responses:
        if r.entry is None:
            continue
        if r.entry.logical_timestamp > max_logical_ts:
            latest_entry = r.entry
            max_logical_ts = r.entry.logical_timestamp

    if latest_entry is None:
        return latest_entry
    assert local_entry is not None

    if local_entry.logical_timestamp < latest_entry.logical_timestamp:
        set_persisted(localhost, key, latest_entry)

    nodes_need_repair = [
        localhost.plant.get_server(x.server_name)
        for x in quorum_get_responses
        if x.entry is not None
        and x.entry.logical_timestamp < latest_entry.logical_timestamp
    ]
    # myself + other good nodes
    good_count = 1 + len(quorum_get_responses) - len(nodes_need_repair)
    if len(nodes_need_repair) > 0:
        # Quorum repair
        repair_quorum_count = max(0, quorum_count - good_count)
        set_arg = ABDSetLocalArg(entry=latest_entry)
        await localhost.call_multiple(
            nodes_need_repair,
            repair_quorum_count,
            ABD_SET_LOCAL,
            set_arg,
        )

    return latest_entry


##################################
# Web APIs
##################################


@implement_api(router, ABD_GET)
async def abd_get(arg: ABDGetArg, localhost: LocalHost):
    logger.info(f"ABD GET {arg.key}")

    entry = await get_and_repair_quorum(localhost, arg.key)

    resp = ABD_GET.ResponseClass(
        server_name=localhost.config.name,
        server_id=localhost.config.id,
        entry=entry,
    )
    return resp


@implement_api(router, ABD_GET_LOCAL)
async def abd_get_local(arg: ABDGetLocalArg, localhost: LocalHost):
    logger.info(f"ABD GET_LOCAL {arg.key}")

    entry = get_persisted(localhost, arg.key)

    resp = ABD_GET_LOCAL.ResponseClass(
        server_name=localhost.config.name,
        server_id=localhost.config.id,
        entry=entry,
    )
    return resp


@implement_api(router, ABD_SET_LOCAL)
async def abd_set_local(arg: ABDSetLocalArg, localhost: LocalHost):
    entry = arg.entry
    logger.info(f"ABD SET_LOCAL {entry.key} to {entry.data}")

    # Save to local storage if the received logical time stamp is greater than current timestamp
    ts = get_local_logical_timestamp(localhost)
    if entry.logical_timestamp > ts:
        set_persisted(localhost, entry.key, entry)
        ts = entry.logical_timestamp
        set_local_logical_timestamp(localhost, ts)

    # response in all cases, whether we saved it to DB or not
    resp = ABD_SET_LOCAL.ResponseClass(
        server_name=localhost.config.name,
        server_id=localhost.config.id,
        key=entry.key,
        logical_timestamp=ts,
    )
    return resp


@implement_api(router, ABD_SET)
async def abd_set(arg: ABDSetArg, localhost: LocalHost):
    logger.info(f"ABD SET {arg.key} to {arg.data}")

    entry, quorum_resp = await set_quorum(localhost, arg.key, arg.data)

    resp = ABD_SET.ResponseClass(
        server_name=localhost.config.name,
        server_id=localhost.config.id,
        key=entry.key,
        logical_timestamp=entry.logical_timestamp,
        quorum_responses=quorum_resp,
    )
    return resp
