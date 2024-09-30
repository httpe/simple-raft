# Raft Algorithm
from typing import Annotated, Literal
import random
import time
import asyncio
import json

from pydantic import BaseModel
from fastapi import APIRouter, Request, Depends, APIRouter

from ..network import NetworkException
from ..server import LocalHost, implement_api
from ..logger import logger
from ..api import (
    RaftLogEntry,
    RAFT_REQ_VOTE,
    RaftReqVoteArg,
    RaftReqVoteResponse,
    RAFT_APP_ENT,
    RaftAppEntArg,
    RaftAppEntResponse,
)

##################################
# Module Globals
##################################

router = APIRouter(tags=["Raft"])

STORAGE_SECTION = "Raft"

##################################
# Raft States
##################################


class RaftLog(BaseModel):
    entries: list[RaftLogEntry]


class RaftVotedFor(BaseModel):
    term: int
    id: str


class RaftApi:
    def __init__(self, localhost: LocalHost) -> None:
        self.localhost = localhost
        self.id = self.localhost.name

        # Volatile states - All servers
        self.commitIndex = 0
        self.lastApplied = 0
        self.role: Literal["Leader", "Follower", "Candidate"] = "Follower"

        # Volatile states - Leader only
        self.nextIndex: dict[str, int] = {}
        self.matchIndex: dict[str, int] = {}

        # Leader election
        self.election_timeout_ms = random.randint(150, 300)
        self.election_timer_sec = time.monotonic()

        # Leader heartbeat
        self.leader_heart_beat_interval_ms = 30

    async def start(self):
        # TODO: do not start Raft yet, many features are not yet implemented
        return None

        election = asyncio.create_task(self.election_loop())
        leader_heart_beat = asyncio.create_task(self.leader_heart_beat())
        await asyncio.gather(election, leader_heart_beat)

    async def leader_heart_beat(self):
        timer_sec = time.monotonic()
        prev_role = self.role
        while True:
            curr_role = self.role
            role_changed = curr_role != prev_role
            prev_role = curr_role
            if curr_role == "Leader":
                # only Leader will send out heart beat
                elapsed_ms = (time.monotonic() - timer_sec) * 1000
                timeout_triggered = elapsed_ms > self.leader_heart_beat_interval_ms
                if role_changed or timeout_triggered:
                    timer_sec = time.monotonic()
                    asyncio.create_task(self.heart_beat())
            await asyncio.sleep(0.01)  # 10ms

    async def heart_beat(self):
        siblings = self.localhost.siblings
        quorum_count = self.localhost.plant.quorum_count
        vote_req = RaftAppEntArg(
            term=self.currentTerm,
            leaderId=self.id,
        )

        # Send request for vote in parallel to all siblings
        pending: set[asyncio.Task[RaftAppEntResponse]] = set()
        for remote in siblings:
            req = asyncio.create_task(
                self.localhost.call(remote, RAFT_REQ_VOTE, vote_req)
            )
            pending.add(req)

        # Wait for the required count of responses
        responses: list[RaftReqVoteResponse] = []
        done, pending = await asyncio.wait(
            pending, return_when=asyncio.FIRST_COMPLETED, timeout=timeout_sec
        )
        # if we are in a newer term (started by the election timer loop), abandon the current election
        if self.currentTerm != election_term:
            return

        for task in done:
            try:
                r = await task
                if r.voteGranted:
                    vote_gained += 1
                responses.append(r)
            except NetworkException as e:
                n_errored += 1

        if vote_gained + len(pending) < quorum_count - 1:
            # can no longer acquire votes from the quorum, end the election for this term
            return

        if vote_gained >= quorum_count - 1:
            # quorum vote granted, transit to Leader
            self.role = "Leader"
            return

    async def election_loop(self):
        while True:
            # Only enter leader election if is not leader
            # Leader will keep its role and does not start new election by timeout
            timer_sec = time.monotonic()
            elapsed_ms = (timer_sec - self.election_timer_sec) * 1000
            if elapsed_ms > self.election_timeout_ms:
                self.election_timer_sec = timer_sec
                if self.role != "Leader":
                    # Leader does not start new election by timeout
                    asyncio.create_task(self.elect_leader())
            await asyncio.sleep(0.01)  # 10ms

    async def elect_leader(self):
        self.currentTerm += 1
        election_term = self.currentTerm
        assert self.role in ["Leader", "Candidate"]
        self.role = "Candidate"

        # Vote for oneself
        self.votedFor = RaftVotedFor(term=election_term, id=self.id)

        # Gather log info
        logs = self.log.entries
        if len(logs) == 0:
            last_log_term = 0
        else:
            last_log_term = logs[-1].term
        last_log_index = len(logs)  # 1-based

        # Request votes
        siblings = self.localhost.siblings
        quorum_count = self.localhost.plant.quorum_count
        vote_req = RaftReqVoteArg(
            term=election_term,
            candidateId=self.id,
            lastLogTerm=last_log_term,
            lastLogIndex=last_log_index,
        )

        # Send request for vote in parallel to all siblings
        pending: set[asyncio.Task[RaftReqVoteResponse]] = set()
        for remote in siblings:
            req = asyncio.create_task(
                self.localhost.call(remote, RAFT_REQ_VOTE, vote_req)
            )
            pending.add(req)

        # Wait for the required count of responses
        responses: list[RaftReqVoteResponse] = []
        vote_gained = 1  # we voted for ourself
        n_errored = 0
        t0 = time.monotonic()

        while True:
            # timeout if we are in the era of the next election timeout
            elapsed_sec = time.monotonic() - t0
            timeout_sec = self.election_timeout_ms / 1000 - elapsed_sec
            if timeout_sec <= 0:
                return

            done, pending = await asyncio.wait(
                pending, return_when=asyncio.FIRST_COMPLETED, timeout=timeout_sec
            )

            # if we are in a newer term (started by the election timer loop), abandon the current election
            if self.currentTerm != election_term:
                return

            for task in done:
                try:
                    r = await task
                    if r.voteGranted:
                        vote_gained += 1
                    responses.append(r)
                except NetworkException as e:
                    n_errored += 1

            if vote_gained + len(pending) < quorum_count - 1:
                # can no longer acquire votes from the quorum, end the election for this term
                return

            if vote_gained >= quorum_count - 1:
                # quorum vote granted, transit to Leader
                self.role = "Leader"
                return

    @property
    def currentTerm(self) -> int:
        entry = self.localhost.storage.get_persisted(STORAGE_SECTION, "currentTerm")
        if entry is None:
            return 0
        return int(entry.data)

    @currentTerm.setter
    def currentTerm(self, term: int):
        self.localhost.storage.set_persisted(STORAGE_SECTION, "currentTerm", str(term))

    @property
    def votedFor(self) -> RaftVotedFor | None:
        entry = self.localhost.storage.get_persisted(STORAGE_SECTION, "votedFor")
        if entry is None:
            return None
        return RaftVotedFor.model_validate_json(entry.data)

    @votedFor.setter
    def votedFor(self, votedFor: RaftVotedFor | None):
        if votedFor is None:
            v = None
        else:
            v = votedFor.model_dump_json()
        self.localhost.storage.set_persisted(STORAGE_SECTION, "votedFor", v)

    @property
    def log(self) -> RaftLog:
        entry = self.localhost.storage.get_persisted(STORAGE_SECTION, "log")
        if entry is None:
            return RaftLog(entries=[])
        return RaftLog.model_validate_json(entry.data)

    @log.setter
    def log(self, log: RaftLog):
        self.localhost.storage.set_persisted(
            STORAGE_SECTION, "log", RaftLog.model_dump_json(log)
        )

    def reset_role(self, new_term: int):
        if new_term <= self.currentTerm:
            return
        self.currentTerm = new_term
        self.role = "Follower"
        self.votedFor = None
        self.election_timer_sec = time.monotonic() * 1000

    def vote(self, req: RaftReqVoteArg) -> RaftReqVoteResponse:
        granted = True
        if req.term < self.currentTerm:
            granted = False
        if granted and self.votedFor is not None and self.votedFor != req.candidateId:
            granted = False
        logs = self.log.entries
        if granted and len(logs) > 0:
            last_log = logs[-1]
            if req.lastLogTerm < last_log.term:
                granted = False
            elif req.lastLogIndex == last_log.term and req.lastLogIndex < len(logs):
                granted = False
        return RaftReqVoteResponse(term=self.currentTerm, voteGranted=granted)

    def append_entry(self, req: RaftAppEntArg) -> RaftAppEntResponse:
        pass


async def raft_api(request: Request) -> RaftApi:
    return request.app.state.raft


Raft = Annotated[RaftApi, Depends(raft_api)]


##################################
# RPC APIs
##################################


@implement_api(router, RAFT_REQ_VOTE)
async def raft_req_vote(arg: RaftReqVoteArg, raft: Raft):
    logger.info(f"Raft REQ VOTE {arg}")
    raft.reset_role(arg.term)
    return raft.vote(arg)


@implement_api(router, RAFT_APP_ENT)
async def raft_append_entries(arg: RaftAppEntArg, raft: Raft):
    logger.info(f"Raft APP ENT {arg}")
    raft.reset_role(arg.term)
    return raft.append_entry(arg)
