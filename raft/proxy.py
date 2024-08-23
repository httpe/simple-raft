from __future__ import annotations

############################################
## Proxy
############################################

from asyncio import sleep

import httpx
from fastapi import APIRouter, Response, HTTPException, status
from pydantic import BaseModel

from .network import (
    HttpNetworkInterface,
    NetworkAddress,
    NetworkRequest,
    NetworkGeneralException,
    NetworkTimeoutException,
)
from .logger import logger
from .singleton import singleton


############################################
## Globals
############################################

router = APIRouter(prefix="/proxy", tags=["Proxy"])

drop_rules: dict[str, RequestMatchingCriteria] = {}
timeout_rules: dict[str, RequestMatchingCriteria] = {}

############################################
## Interface
############################################


class HttpNetworkInterfaceWithProxy(HttpNetworkInterface):
    def __init__(self, self_addr: NetworkAddress, proxy_addr: NetworkAddress) -> None:
        super().__init__(self_addr=self_addr)
        self.proxy_addr = proxy_addr

    def call(
        self, destination: NetworkAddress, endpoint: str, params: dict | None = None
    ) -> str:
        base_proxy_url = self.proxy_addr.construct_base_url(f"{router.prefix}/route")
        request = NetworkRequest(
            origin=self.self_addr,
            destination=destination,
            endpoint=endpoint,
            params=params,
        )
        data = request.model_dump()
        logger.info(f"Sending request via proxy {self.proxy_addr.name}: {data}")
        try:
            r = httpx.post(base_proxy_url, json=data)
        except httpx.TimeoutException as e:
            raise NetworkTimeoutException() from e
        except Exception as e:
            raise NetworkGeneralException() from e
        if r.status_code != status.HTTP_200_OK:
            raise NetworkGeneralException(r.status_code)
        return r.text


############################################
## Proxy Routing API
############################################


@router.post("/route")
async def proxy_route(request_to_forward: NetworkRequest) -> Response:
    logger.info(
        f"Routing request from {request_to_forward.origin.name} to {request_to_forward.destination.name} for endpoint {request_to_forward.endpoint} with params {request_to_forward.params}"
    )

    process_proxy_drop_rule(request_to_forward)
    await process_proxy_timeout_rule(request_to_forward)

    r = singleton.network.call(
        destination=request_to_forward.destination,
        endpoint=request_to_forward.endpoint,
        params=request_to_forward.params,
    )

    return Response(content=r, media_type="application/json")


############################################
## Routing Rule API
############################################

INFINITE_SLEEP_PERIOD = 60 * 60 * 24 * 30


class RequestMatchingCriteria(BaseModel):
    origin_names: list[str] | None
    dest_names: list[str] | None
    endpoints: list[str] | None


@router.post("/set/drop_rule/{id}")
async def set_drop_rule(id: str, criteria: RequestMatchingCriteria):
    drop_rules[id] = criteria


@router.post("/set/timeout_rule/{id}")
async def set_timeout_rule(id: str, criteria: RequestMatchingCriteria):
    timeout_rules[id] = criteria


@router.get("/clear/drop_rules")
async def clear_drop_rules(ids: list[str] | None = None):
    if ids is None:
        drop_rules.clear()
    else:
        for id in ids:
            drop_rules.pop(id)


@router.get("/clear/timeout_rules")
async def clear_timeout_rules(ids: list[str] | None = None):
    if ids is None:
        timeout_rules.clear()
    else:
        for id in ids:
            timeout_rules.pop(id)


@router.get("/get/timeout_rule/{id}")
async def get_timeout_rule(id: str):
    return timeout_rules.get(id)


@router.get("/get/drop_rule/{id}")
async def get_drop_rule(id: str):
    return drop_rules.get(id)


def process_proxy_drop_rule(request: NetworkRequest):
    for id, criteria in drop_rules.items():
        if (
            (
                criteria.origin_names is None
                or request.origin.name in criteria.origin_names
            )
            and (
                criteria.dest_names is None
                or request.destination.name in criteria.dest_names
            )
            and (criteria.endpoints is None or request.endpoint in criteria.endpoints)
        ):
            raise HTTPException(status_code=status.HTTP_502_BAD_GATEWAY)


async def process_proxy_timeout_rule(request: NetworkRequest):
    for id, criteria in timeout_rules.items():
        if (
            (
                criteria.origin_names is None
                or request.origin.name in criteria.origin_names
            )
            and (
                criteria.dest_names is None
                or request.destination.name in criteria.dest_names
            )
            and (criteria.endpoints is None or request.endpoint in criteria.endpoints)
        ):
            await sleep(INFINITE_SLEEP_PERIOD)
