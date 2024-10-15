from pydantic import BaseModel
from typing import Literal, TypeVar, Generic
from dataclasses import dataclass

##############################################################
# Base API
##############################################################

TArg = TypeVar("TArg", bound=BaseModel)
TResp = TypeVar("TResp", bound=BaseModel)


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

##############################################################
# Raft Algorithm
##############################################################


RAFT_PREFIX = "/raft"


class RaftLogEntry(BaseModel):
    data: str | None
    term: int


# Raft RequestVote
class RaftReqVoteArg(BaseModel):
    term: int
    candidateId: str
    lastLogIndex: int
    lastLogTerm: int


class RaftReqVoteResponse(BaseModel):
    term: int
    voteGranted: bool


RAFT_REQ_VOTE = APIConcept[RaftReqVoteArg, RaftReqVoteResponse](
    endpoint=f"{RAFT_PREFIX}/RequestVote",
    ArgumentClass=RaftReqVoteArg,
    ResponseClass=RaftReqVoteResponse,
)


# Raft AppendEntries
class RaftAppEntArg(BaseModel):
    term: int
    leaderId: str
    prevLogIndex: int
    prevLogTerm: int
    entries: list[RaftLogEntry]
    leaderCommit: int


class RaftAppEntResponse(BaseModel):
    term: int
    success: bool


RAFT_APP_ENT = APIConcept[RaftAppEntArg, RaftAppEntResponse](
    endpoint=f"{RAFT_PREFIX}/AppendEntries",
    ArgumentClass=RaftAppEntArg,
    ResponseClass=RaftAppEntResponse,
)


# Raft AddLog
class RaftAddLogArg(BaseModel):
    data: str | None


class RaftAddLogResponse(BaseModel):
    term: int
    index: int


RAFT_ADD_LOG = APIConcept[RaftAddLogArg, RaftAddLogResponse](
    endpoint=f"{RAFT_PREFIX}/AddLog",
    ArgumentClass=RaftAddLogArg,
    ResponseClass=RaftAddLogResponse,
)


# Raft GetLogs


class RaftGetLogsArg(BaseModel):
    startIndex: int | None
    endIndex: int | None
    quorum: bool


class RaftIndexedLogEntry(BaseModel):
    data: str | None
    term: int
    index: int


class RaftGetLogsResponse(BaseModel):
    entries: list[RaftIndexedLogEntry]
    server_name: str
    server_id: str
    committedIndex: int
    maxLogsIndex: int


RAFT_GET_LOGS = APIConcept[RaftGetLogsArg, RaftGetLogsResponse](
    endpoint=f"{RAFT_PREFIX}/GetLogs",
    ArgumentClass=RaftGetLogsArg,
    ResponseClass=RaftGetLogsResponse,
)
