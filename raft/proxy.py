from __future__ import annotations

############################################
## Proxy
############################################

from asyncio import sleep

from fastapi import APIRouter, Response, HTTPException, status

from .network import NetworkRequest
from .logger import logger
from .api import (
    PROXY_ROUTE_ENDPOINT,
    PROXY_SET_RULE,
    PROXY_CLEAR_RULE,
    RequestMatchingCriteria,
    ProxySetRuleArg,
    ProxyClearRulesArg,
)
from .server import LocalHost, implement_api


############################################
## Globals
############################################

router = APIRouter(tags=["Proxy"])

drop_rules: dict[str, RequestMatchingCriteria] = {}
timeout_rules: dict[str, RequestMatchingCriteria] = {}

############################################
## Proxy Routing API
############################################


@router.post(PROXY_ROUTE_ENDPOINT)
async def proxy_route(req: NetworkRequest, localhost: LocalHost) -> Response:
    logger.info(
        f"Routing request from {req.origin.name} to {req.destination.name} for endpoint {req.endpoint} with body {req.body}"
    )

    process_proxy_drop_rule(req)
    await process_proxy_timeout_rule(req)

    target = localhost.plant.get_server(req.destination.name)

    r = await localhost.network.call(
        target.address, req.endpoint, req.body, localhost.config.timeout
    )

    return Response(content=r, media_type="application/json")


############################################
## Routing Rule API
############################################

INFINITE_SLEEP_PERIOD = 60 * 60 * 24 * 30


@implement_api(router, PROXY_SET_RULE)
async def set_rule(arg: ProxySetRuleArg, localhost: LocalHost):
    if arg.rule == "drop":
        drop_rules[arg.id] = arg.criteria
    else:
        assert arg.rule == "timeout"
        timeout_rules[arg.id] = arg.criteria
    return PROXY_SET_RULE.ResponseClass(id=arg.id)


@implement_api(router, PROXY_CLEAR_RULE)
async def clear_rules(arg: ProxyClearRulesArg, localhost: LocalHost):
    ids: list[str] = []
    if arg.rule == "drop":
        if arg.ids is None:
            ids.extend(drop_rules.keys())
            drop_rules.clear()
        else:
            for id in arg.ids:
                ids.append(id)
                drop_rules.pop(id)
    else:
        assert arg.rule == "timeout"
        if arg.ids is None:
            ids.extend(timeout_rules.keys())
            timeout_rules.clear()
        else:
            for id in arg.ids:
                ids.append(id)
                timeout_rules.pop(id)
    return PROXY_CLEAR_RULE.ResponseClass(ids=ids)


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
