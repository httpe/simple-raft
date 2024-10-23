# Attiya, Bar-Noy, Dolev (ABD) Quorum Get/Set Algorithm

from typing import Annotated

from fastapi import APIRouter, Request, Depends

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

##################################
# Module Globals
##################################

router = APIRouter(tags=["ABD"])

STORAGE_SECTION = "ABD"

##################################
# ABD Core Logics
##################################


class ABDApi:
    def __init__(self, localhost: LocalHost) -> None:
        self.localhost = localhost

    def get_persisted(self, key: str) -> ABDDataEntry | None:
        persisted_entry = self.localhost.storage.get(f"{STORAGE_SECTION}/data/{key}")
        if persisted_entry is None:
            entry = None
        else:
            entry = ABDDataEntry.model_validate_json(persisted_entry.data)
        return entry

    def set_persisted(self, key: str, entry: ABDDataEntry):
        self.localhost.storage.set(
            f"{STORAGE_SECTION}/data/{key}", entry.model_dump_json()
        )

    @property
    def logical_timestamp(self):
        """Local logical timestamp"""
        ts = self.localhost.storage.get(f"{STORAGE_SECTION}/LOCAL_LOGICAL_TIMESTAMP")
        if ts is None:
            return 0
        else:
            return int(ts.data)

    @logical_timestamp.setter
    def logical_timestamp(self, ts: int):
        self.localhost.storage.set(
            f"{STORAGE_SECTION}/LOCAL_LOGICAL_TIMESTAMP", str(ts)
        )

    async def set_quorum(self, key: str, data: str | None):
        # Bump logical ts for every set
        ts = self.logical_timestamp
        ts += 1
        self.logical_timestamp = ts

        # Propagate to all siblings
        entry = ABDDataEntry(data=data, key=key, logical_timestamp=ts)
        arg = ABDSetLocalArg(entry=entry)

        quorum_responses = await self.localhost.call_multiple(
            self.localhost.siblings,
            self.localhost.plant.quorum_count - 1,
            ABD_SET_LOCAL,
            arg,
        )

        # Set to local storage
        self.set_persisted(key, entry)

        return entry, quorum_responses

    async def get_and_repair_quorum(self, key: str):
        # Quorum read
        get_arg = ABDGetLocalArg(key=key)
        siblings = self.localhost.siblings
        quorum_count = self.localhost.plant.quorum_count

        # ourself is always good
        quorum_get_responses = await self.localhost.call_multiple(
            siblings, quorum_count - 1, ABD_GET_LOCAL, get_arg
        )

        local_entry = self.get_persisted(key)
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

        if local_entry is None or (
            local_entry.logical_timestamp < latest_entry.logical_timestamp
        ):
            self.set_persisted(key, latest_entry)

        nodes_need_repair = [
            self.localhost.plant.get_server(x.server_name)
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
            await self.localhost.call_multiple(
                nodes_need_repair,
                repair_quorum_count,
                ABD_SET_LOCAL,
                set_arg,
            )

        return latest_entry

    def set_local(self, entry: ABDDataEntry):
        # Save to local storage if the received logical time stamp is greater than current timestamp
        ts = self.logical_timestamp
        if entry.logical_timestamp > ts:
            self.set_persisted(entry.key, entry)
            ts = entry.logical_timestamp
            self.logical_timestamp = ts
        return ts


async def abd_api(request: Request) -> ABDApi:
    return request.app.state.abd


ABD = Annotated[ABDApi, Depends(abd_api)]

##################################
# RPC APIs
##################################


@implement_api(router, ABD_GET)
async def abd_get(arg: ABDGetArg, abd: ABD):
    logger.info(f"ABD GET {arg.key}")

    entry = await abd.get_and_repair_quorum(arg.key)

    resp = ABD_GET.ResponseClass(
        server_name=abd.localhost.config.name,
        server_id=abd.localhost.config.id,
        entry=entry,
    )
    return resp


@implement_api(router, ABD_GET_LOCAL)
async def abd_get_local(arg: ABDGetLocalArg, abd: ABD):
    logger.info(f"ABD GET_LOCAL {arg.key}")

    entry = abd.get_persisted(arg.key)

    resp = ABD_GET_LOCAL.ResponseClass(
        server_name=abd.localhost.config.name,
        server_id=abd.localhost.config.id,
        entry=entry,
    )
    return resp


@implement_api(router, ABD_SET_LOCAL)
async def abd_set_local(arg: ABDSetLocalArg, abd: ABD):
    logger.info(f"ABD SET_LOCAL {arg.entry.key} to {arg.entry.data}")

    ts = abd.set_local(arg.entry)

    # response in all cases, whether we saved it to DB or not
    resp = ABD_SET_LOCAL.ResponseClass(
        server_name=abd.localhost.config.name,
        server_id=abd.localhost.config.id,
        key=arg.entry.key,
        logical_timestamp=ts,
    )
    return resp


@implement_api(router, ABD_SET)
async def abd_set(arg: ABDSetArg, abd: ABD):
    logger.info(f"ABD SET {arg.key} to {arg.data}")

    entry, quorum_resp = await abd.set_quorum(arg.key, arg.data)

    resp = ABD_SET.ResponseClass(
        server_name=abd.localhost.config.name,
        server_id=abd.localhost.config.id,
        key=entry.key,
        logical_timestamp=entry.logical_timestamp,
        quorum_responses=quorum_resp,
    )
    return resp
