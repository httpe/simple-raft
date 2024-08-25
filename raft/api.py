from datetime import datetime
from pydantic import BaseModel
from typing import Literal

##########################################
# Proxy
##########################################

PROXY_PREFIX = "/proxy"

PROXY_ROUTE_ENDPOINT = f"{PROXY_PREFIX}/route"


# Set Proxy Rule
class RequestMatchingCriteria(BaseModel):
    origin_names: list[str] | None
    dest_names: list[str] | None
    endpoints: list[str] | None


class ProxySetRuleResponse(BaseModel):
    id: str


class ProxySetRuleArg(BaseModel):
    rule: Literal["drop", "timeout"]
    id: str
    criteria: RequestMatchingCriteria


PROXY_RULE_SET_ENDPOINT = f"{PROXY_PREFIX}/set"

# Clear Proxy Rule


class ProxyClearRulesResponse(BaseModel):
    ids: list[str]


class ProxyClearRulesArg(BaseModel):
    rule: Literal["drop", "timeout"]
    ids: list[str] | None


PROXY_CLEAR_RULES_ENDPOINT = f"{PROXY_PREFIX}/clear"

##########################################
# Ping
##########################################


# Pong


class PongArg(BaseModel):
    requester: str


class PongResponse(BaseModel):
    server_name: str
    server_id: str
    requester_name: str


PONG_ENDPOINT = "/pong"

# Ping


class PingArg(BaseModel):
    server_name: str


class PingResponse(BaseModel):
    server_name: str
    server_id: str
    remote_pong_response: PongResponse


PING_ENDPOINT = "/ping"


##########################################
# Two Phase Commit
##########################################

TPC_PREFIX = "/tpc"


class DataEntry(BaseModel):
    data: str
    timestamp: datetime


# TPC DB get


class DBGetArg(BaseModel):
    key: str


class DBGetResponse(BaseModel):
    server_name: str
    server_id: str
    key: str
    db_size: int
    entry: DataEntry | None


TPC_GET_ENDPOINT = f"{TPC_PREFIX}/get"

# TPC DB Set


class DBPropagateResponse(BaseModel):
    server_name: str
    server_id: str
    key: str
    db_size: int


class DBSetArg(BaseModel):
    key: str
    data: str | None


class DBSetResponse(BaseModel):
    server_name: str
    server_id: str
    key: str
    db_size: int
    propagations: list[DBPropagateResponse]


TPC_SET_ENDPOINT = f"{TPC_PREFIX}/set"
