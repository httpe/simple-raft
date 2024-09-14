from datetime import datetime
from pydantic import BaseModel
from typing import Literal, TypeVar, Generic, Protocol
from dataclasses import dataclass

##############################################################
# Base API
##############################################################

TArg = TypeVar("TArg", bound=BaseModel)
TResp = TypeVar("TResp", bound=BaseModel)


# @dataclass
# class APIConceptProtocol(Protocol[TArg, TResp]):
#     endpoint: str
#     ArgumentClass: type[TArg]
#     ResponseClass: type[TResp]


@dataclass
class APIConcept(Generic[TArg, TResp]):
    endpoint: str
    ArgumentClass: type[TArg]
    ResponseClass: type[TResp]


##############################################################
# Proxy
##############################################################

PROXY_PREFIX = "/proxy"

# Route request
PROXY_ROUTE_ENDPOINT = f"{PROXY_PREFIX}/route"


# Set Proxy Rule
class RequestMatchingCriteria(BaseModel):
    origin_names: list[str] | None
    dest_names: list[str] | None
    endpoints: list[str] | None


class ProxySetRuleArg(BaseModel):
    rule: Literal["drop", "timeout"]
    id: str
    criteria: RequestMatchingCriteria


class ProxySetRuleResponse(BaseModel):
    id: str


PROXY_SET_RULE = APIConcept[ProxySetRuleArg, ProxySetRuleResponse](
    endpoint=f"{PROXY_PREFIX}/set",
    ArgumentClass=ProxySetRuleArg,
    ResponseClass=ProxySetRuleResponse,
)


# Clear Proxy Rule
class ProxyClearRulesArg(BaseModel):
    rule: Literal["drop", "timeout"]
    ids: list[str] | None


class ProxyClearRulesResponse(BaseModel):
    ids: list[str]


PROXY_CLEAR_RULE = APIConcept[ProxyClearRulesArg, ProxyClearRulesResponse](
    endpoint=f"{PROXY_PREFIX}/clear",
    ArgumentClass=ProxyClearRulesArg,
    ResponseClass=ProxyClearRulesResponse,
)

##############################################################
# Persisted Storage
##############################################################


class PersistedEntry(BaseModel):
    data: str
    timestamp: datetime


##############################################################
# Ping
##############################################################


# Pong
class PongArg(BaseModel):
    requester: str


class PongResp(BaseModel):
    server_name: str
    server_id: str
    requester_name: str


PONG = APIConcept[PongArg, PongResp](
    endpoint="/pong", ArgumentClass=PongArg, ResponseClass=PongResp
)


# Ping
class PingArg(BaseModel):
    server_name: str


class PingResponse(BaseModel):
    server_name: str
    server_id: str
    remote_pong_response: PongResp


PING = APIConcept[PingArg, PingResponse](
    endpoint="/ping", ArgumentClass=PingArg, ResponseClass=PingResponse
)


##############################################################
# Attiya, Bar-Noy, Dolev (ABD) Quorum Get/Set Algorithm
##############################################################

ABD_PREFIX = "/abd"


class ABDDataEntry(BaseModel):
    data: str | None
    key: str
    logical_timestamp: int


# ABD Quorum Get
class ABDGetArg(BaseModel):
    key: str


class ABDGetResponse(BaseModel):
    server_name: str
    server_id: str
    entry: ABDDataEntry | None


ABD_GET = APIConcept[ABDGetArg, ABDGetResponse](
    endpoint=f"{ABD_PREFIX}/get", ArgumentClass=ABDGetArg, ResponseClass=ABDGetResponse
)


# ABD Get Local
class ABDGetLocalArg(BaseModel):
    key: str


class ABDGetLocalResponse(BaseModel):
    server_name: str
    server_id: str
    entry: ABDDataEntry | None


ABD_GET_LOCAL = APIConcept[ABDGetLocalArg, ABDGetLocalResponse](
    endpoint=f"{ABD_PREFIX}/get_local",
    ArgumentClass=ABDGetLocalArg,
    ResponseClass=ABDGetLocalResponse,
)


# ABD Set Local
class ABDSetLocalArg(BaseModel):
    entry: ABDDataEntry


class ABDSetLocalResponse(BaseModel):
    server_name: str
    server_id: str
    key: str
    logical_timestamp: int


ABD_SET_LOCAL = APIConcept[ABDSetLocalArg, ABDSetLocalResponse](
    endpoint=f"{ABD_PREFIX}/set_local",
    ArgumentClass=ABDSetLocalArg,
    ResponseClass=ABDSetLocalResponse,
)


# ABD Quorum Set
class ABDSetArg(BaseModel):
    key: str
    data: str | None


class ABDSetResponse(BaseModel):
    server_name: str
    server_id: str
    key: str
    logical_timestamp: int
    quorum_responses: list[ABDSetLocalResponse]


ABD_SET = APIConcept[ABDSetArg, ABDSetResponse](
    endpoint=f"{ABD_PREFIX}/set",
    ArgumentClass=ABDSetArg,
    ResponseClass=ABDSetResponse,
)
