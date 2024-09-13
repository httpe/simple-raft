from datetime import datetime
from pydantic import BaseModel
from typing import Literal

##########################################
# Proxy
##########################################

PROXY_PREFIX = "/proxy"

# Route request
PROXY_ROUTE_ENDPOINT = f"{PROXY_PREFIX}/route"


# Set Proxy Rule
PROXY_RULE_SET_ENDPOINT = f"{PROXY_PREFIX}/set"


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


# Clear Proxy Rule
PROXY_CLEAR_RULES_ENDPOINT = f"{PROXY_PREFIX}/clear"


class ProxyClearRulesResponse(BaseModel):
    ids: list[str]


class ProxyClearRulesArg(BaseModel):
    rule: Literal["drop", "timeout"]
    ids: list[str] | None


##########################################
# Persisted Storage
##########################################


class PersistedEntry(BaseModel):
    data: str
    timestamp: datetime


##########################################
# Ping
##########################################


# Pong
PONG_ENDPOINT = "/pong"


class PongArg(BaseModel):
    requester: str


class PongResponse(BaseModel):
    server_name: str
    server_id: str
    requester_name: str


# Ping
PING_ENDPOINT = "/ping"


class PingArg(BaseModel):
    server_name: str


class PingResponse(BaseModel):
    server_name: str
    server_id: str
    remote_pong_response: PongResponse


##########################################
# Attiya, Bar-Noy, Dolev (ABD) Quorum Get/Set Algorithm
##########################################

ABD_PREFIX = "/abd"


# ABD Quorum Get
ABD_GET_ENDPOINT = f"{ABD_PREFIX}/get"


class ABDGetArg(BaseModel):
    key: str


class ABDDataEntry(BaseModel):
    data: str | None
    key: str
    logical_timestamp: int


class ABDGetResponse(BaseModel):
    server_name: str
    server_id: str
    entry: ABDDataEntry | None


# ABD Get Local
ABD_GET_LOCAL_ENDPOINT = f"{ABD_PREFIX}/get_local"


class ABDGetLocalArg(BaseModel):
    key: str


class ABDGetLocalResponse(BaseModel):
    server_name: str
    server_id: str
    entry: ABDDataEntry | None


# ABD Set Local
ABD_SET_LOCAL_ENDPOINT = f"{ABD_PREFIX}/set_local"


class ABDSetLocalArg(BaseModel):
    entry: ABDDataEntry


class ABDSetLocalResponse(BaseModel):
    server_name: str
    server_id: str
    key: str
    logical_timestamp: int


# ABD Quorum Set
ABD_SET_ENDPOINT = f"{ABD_PREFIX}/set"


class ABDSetArg(BaseModel):
    key: str
    data: str | None


class ABDSetResponse(BaseModel):
    server_name: str
    server_id: str
    key: str
    logical_timestamp: int
    quorum_responses: list[ABDSetLocalResponse]
