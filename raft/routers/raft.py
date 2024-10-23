# Raft Algorithm
from typing import Annotated, Callable, Awaitable
import random
import time
import asyncio
from enum import Enum
import statistics
import traceback

from pydantic import BaseModel
from fastapi import APIRouter, Request, Depends, APIRouter

from raft.configs import ServerConfig

from ..network import NetworkException
from ..server import LocalHost, implement_api
from ..logger import logger
from ..api import (
    APIConcept,
    TArg,
    TResp,
    RaftLogEntry,
    RAFT_REQ_VOTE,
    RaftReqVoteArg,
    RaftReqVoteResponse,
    RAFT_APP_ENT,
    RaftAppEntArg,
    RaftAppEntResponse,
    RAFT_ADD_LOG,
    RaftAddLogArg,
    RaftAddLogResponse,
    RAFT_GET_LOGS,
    RaftGetLogsArg,
    RaftGetLogsResponse,
    RaftIndexedLogEntry,
    StateMachineTransaction,
    RAFT_GET_STATES,
    RaftGetStatesArg,
    RaftGetStatesResponse,
)

##################################
# Module Globals
##################################

router = APIRouter(tags=["Raft"])

STORAGE_SECTION = "Raft"

TIME_SCALE_FACTOR = 1  # make the clock slower X times

##################################
# Raft States
##################################


class RaftRole(Enum):
    LEADER = "Leader"
    CANDIDATE = "Candidate"
    FOLLOWER = "Follower"


class RaftVotedFor(BaseModel):
    term: int
    id: str


class RaftPersistedStorage:
    def __init__(self, localhost: LocalHost) -> None:
        self.localhost = localhost
        self.cache: dict[str, str | None] = {}

    def _read_cached(self, path: str) -> str | None:
        if path in self.cache:
            return self.cache[path]
        else:
            entry = self.localhost.storage.get(f"{STORAGE_SECTION}/{path}")
            if entry is None:
                return None
            return entry.data

    def _write_cached(self, path: str, data: str | None) -> None:
        full_path = f"{STORAGE_SECTION}/{path}"
        if data is None:
            self.localhost.storage.remove(full_path)
            self.cache[path] = None
        else:
            self.localhost.storage.set(full_path, data)
            self.cache[path] = data

    def append_log(self, log: RaftLogEntry) -> None:
        self.append_multiple_logs([log])

    def append_multiple_logs(self, logs: list[RaftLogEntry]) -> None:
        logger.info(f"Raft: appending {len(logs)} logs to persisted storage")
        index = self.max_index()
        for log in logs:
            index += 1
            self._write_cached(f"logs/{index}", log.model_dump_json())
        self._write_cached(f"max_index", str(index))

    def max_index(self) -> int:
        data = self._read_cached("max_index")
        if data is None:
            max_index = 0
        else:
            max_index = int(data)
        return max_index

    def get_logs(
        self, start_index: int | None = None, end_index: int | None = None
    ) -> list[RaftLogEntry]:
        max_index = self.max_index()
        if max_index == 0:
            return []

        if end_index is None:
            end_index = max_index

        if start_index is None:
            start_index = 1

        assert 1 <= start_index <= max_index
        assert 1 <= end_index <= max_index
        assert start_index <= end_index

        logs: list[RaftLogEntry] = []
        for index in range(start_index, end_index + 1):
            data = self._read_cached(f"logs/{index}")
            assert data is not None
            log = RaftLogEntry.model_validate_json(data)
            logs.append(log)

        return logs

    def get_log(self, index: int) -> RaftLogEntry:
        return self.get_logs(index, index)[0]

    def truncate_logs(self, on_and_after: int) -> None:
        new_max_index = on_and_after - 1
        assert new_max_index >= 0
        self._write_cached("max_index", str(new_max_index))

    def get_currentTerm(self) -> int:
        data = self._read_cached("currentTerm")
        if data is None:
            return 0
        return int(data)

    def set_currentTerm(self, term: int):
        logger.info(f"Raft: persisting current term: {term}")
        self._write_cached("currentTerm", str(term))

    def get_votedFor(self) -> RaftVotedFor | None:
        data = self._read_cached("votedFor")
        if data is None:
            return None
        return RaftVotedFor.model_validate_json(data)

    def set_votedFor(self, votedFor: RaftVotedFor | None):
        logger.info(f"Raft: persisting voted for: {votedFor}")
        if votedFor is None:
            v = None
        else:
            v = votedFor.model_dump_json()
        self._write_cached("votedFor", v)


class RaftApi:
    def __init__(self, localhost: LocalHost) -> None:
        self.localhost = localhost
        self.id = self.localhost.name

        # Persisted states
        self.storage: RaftPersistedStorage = RaftPersistedStorage(localhost)

        # Volatile states - All servers
        self.commitIndex = 0
        self.lastApplied = 0
        self.role: RaftRole = RaftRole.FOLLOWER
        self.leader_id: None | str = None
        # state machine: KV store
        self.states = {}

        # Volatile states - Leader only
        self.nextIndex: dict[str, int] = {}
        self.matchIndex: dict[str, int] = {}

        # Leader election
        self.election_timeout_ms = random.randint(200, 1000) * TIME_SCALE_FACTOR
        logger.info(f"Raft: election timeout is set to {self.election_timeout_ms} ms")
        self.next_election_time_sec = time.monotonic()
        self.reset_next_election_time()

        # Leader heartbeat
        self.last_successful_heart_beat: float = 0
        self.leader_heart_beat_interval_ms = 30 * TIME_SCALE_FACTOR
        logger.info(
            f"Raft: leader heart beat interval is set to {self.leader_heart_beat_interval_ms} ms"
        )
        self.leader_heart_beat_timer_sec = time.monotonic()

        logger.info("Raft API initialized")

    async def start(self):
        logger.info(f"Raft: starting timer jobs")
        election = asyncio.create_task(self.election_timer())
        leader_heart_beat = asyncio.create_task(self.leader_heart_beat_timer())
        await asyncio.gather(election, leader_heart_beat)

    async def add_log(self, req: RaftAddLogArg) -> RaftAddLogResponse:
        # verify the transaction first
        try:
            self.apply_state_machine_transaction(self.states, req.data)
        except Exception as e:
            logger.error(f"Raft: error when test applying new transaction, abort")
            logger.error(traceback.format_exc())
            return RaftAddLogResponse(successful=False, term=None, index=None)

        # if we are leader, add to the local log
        logger.info(f"Raft: adding entry to local logs")
        add_log_term = self.storage.get_currentTerm()
        self.storage.append_log(RaftLogEntry(data=req.data, term=add_log_term))
        new_index = self.storage.max_index()

        # trigger replication and return only after replicated to quorum
        logger.info(f"Raft: triggering replication after adding log entry locally")
        while True:
            pending: set[asyncio.Task[bool]] = set()
            for follower in self.localhost.siblings:
                replication_req = asyncio.create_task(self.replicate_logs_to(follower))
                pending.add(replication_req)

            # Wait for replication to quorum
            replicated = 1  # added to local logs already
            error_count = 0
            quorum_count = self.localhost.plant.quorum_count
            while True:
                # Wait for any responses until the end of this election period
                done, pending = await asyncio.wait(
                    pending, return_when=asyncio.FIRST_COMPLETED
                )

                for task in done:
                    try:
                        r = await task
                        if r:
                            replicated += 1
                    except NetworkException:
                        error_count += 1
                        logger.warning(
                            f"Raft: replication failed once, replicated {replicated}, errored {error_count}, pending {len(pending)}"
                        )

                # after the awaits above, check if we are already in a newer term
                # (started by the election timer loop)
                # if so, error out the add log request
                if self.storage.get_currentTerm() != add_log_term:
                    logger.error(
                        f"Raft: add log failed, interrupted by election, old term {add_log_term}, new term {self.storage.get_currentTerm()}"
                    )
                    raise Exception("Add log request interrupted by election")

                # if we cannot replicate to the quorum, wait for a while and retry
                if replicated + len(pending) < quorum_count:
                    logger.warning(f"Raft: failed replicating new entry to the quorum")
                    break

                # if we got the majority, commit
                if replicated >= quorum_count:
                    assert self.role == RaftRole.LEADER
                    logger.info(f"Raft: successfully replicated new entry to quorum")
                    self.leader_check_and_commit()
                    assert self.commitIndex >= new_index
                    return RaftAddLogResponse(
                        successful=True, term=add_log_term, index=new_index
                    )

            # wait and retry
            await asyncio.sleep(500)
            if self.storage.get_currentTerm() != add_log_term:
                logger.error(
                    f"Raft: add log failed, interrupted by election, old term {add_log_term}, new term {self.storage.get_currentTerm()}"
                )
                raise Exception("Add log request interrupted by election")
            logger.info(f"Raft: retrying replication for new log")

    async def replicate_logs_to(self, server: ServerConfig):
        logger.debug(f"Raft: replicating logs to {server.name}")
        assert self.role == RaftRole.LEADER

        # Current log info
        last_log_index = self.storage.max_index()
        replication_term = self.storage.get_currentTerm()
        logger.debug(f"Raft: last log index {last_log_index}")

        while True:
            # TODO: dynamic pool of servers?
            next_index = self.nextIndex[server.id]

            prev_log_index = next_index - 1
            if prev_log_index > 0:
                prev_log_term = self.storage.get_log(prev_log_index).term
            else:
                prev_log_term = 0

            if self.storage.max_index() >= next_index:
                entries = self.storage.get_logs(next_index)
            else:
                entries = []

            append_req = RaftAppEntArg(
                term=self.storage.get_currentTerm(),
                leaderId=self.id,
                prevLogIndex=prev_log_index,
                prevLogTerm=prev_log_term,
                leaderCommit=self.commitIndex,
                entries=entries,
            )

            if len(entries) > 0:
                req_logger = logger.info
            else:
                req_logger = logger.debug

            logger.debug(
                f"""Raft: sending replication request to {server.name}, term {append_req.term}, \
                    prevLogIndex {append_req.prevLogIndex}, prevLogTerm {append_req.prevLogTerm}, \
                    leaderCommit {append_req.leaderCommit}, #entries {len(append_req.entries)}"""
            )
            resp = await self.localhost.call(server, RAFT_APP_ENT, append_req)

            self.check_and_bump_term(resp.term)

            # if follower is already in a new term, abort
            if replication_term < resp.term:
                logger.warning("Raft: follower is in a newer term, abort replication")
                return False
            assert resp.term == replication_term

            if resp.success:
                req_logger(f"Raft: logs replicated successfully for {server.name}")
                self.nextIndex[server.id] = max(
                    self.nextIndex[server.id], last_log_index + 1
                )
                self.matchIndex[server.id] = max(
                    self.matchIndex[server.id], last_log_index
                )
                return True

            # if not successful, retry by sending one more entry
            logger.warning(f"Raft: log replication not successful, retrying")
            assert self.nextIndex[server.id] - 1 > 0
            self.nextIndex[server.id] = self.nextIndex[server.id] - 1

    def leader_check_and_commit(self):
        assert self.role == RaftRole.LEADER

        # nothing to commit
        nLogs = self.storage.max_index()
        if nLogs == 0:
            return

        # if the last entry is not added in this term, do not commit
        last_entry_term = self.storage.get_log(nLogs).term
        if last_entry_term != self.storage.get_currentTerm():
            return

        # find the max index that has been replicated to the quorum
        # median_low has the property that, at least half of the matchIndex will be greater or equal than it
        # for ourself, it's not in self.matchIndex, so need to add it, and all local logs are considered replicated
        match_indices = list(self.matchIndex.values()) + [nLogs]
        match_median = statistics.median_low(match_indices)

        if match_median <= self.commitIndex:
            return

        self.commit(match_median)

    def reset_next_election_time(self):
        logger.debug("Raft: resetting election timer")
        self.next_election_time_sec = time.monotonic() + self.election_timeout_ms / 1000

    async def election_timer(self):
        logger.info("Raft: starting election timer")
        while True:
            current_time_sec = time.monotonic()
            if current_time_sec >= self.next_election_time_sec:
                self.reset_next_election_time()
                # If is already leader, do not start new election by timeout, just keep being leader
                # Otherwise, convert to Candidate and start leader election for the next term
                if self.role != RaftRole.LEADER:
                    self.role = RaftRole.CANDIDATE
                    term = self.storage.get_currentTerm()
                    term += 1
                    self.storage.set_currentTerm(term)
                    self.storage.set_votedFor(None)
                    # we can await because the elect_leader coroutine explicitly check for timer in itself
                    # so we know it will not spill into the new election cycle
                    try:
                        await self.elect_leader(term, self.next_election_time_sec)
                    except Exception as e:
                        logger.error(f"Raft: election failed for term {term}")
                        logger.error(traceback.format_exc())
            # check every 10ms instead of sleep for election_timeout_ms
            # because heart beats from Leader can reset next_election_time_sec
            await asyncio.sleep(0.01)

    async def elect_leader(self, election_term: int, next_election_time_sec: float):
        logger.warning(
            f"Raft: starting leader election for term {election_term}, time until next election {next_election_time_sec - time.monotonic()} sec"
        )

        # Vote for oneself
        assert self.storage.get_votedFor() is None
        self.storage.set_votedFor(RaftVotedFor(term=election_term, id=self.id))

        # Gather log info
        nLogs = self.storage.max_index()
        if nLogs == 0:
            last_log_term = 0
        else:
            last_log_term = self.storage.get_log(nLogs).term
        last_log_index = nLogs  # 1-based
        logger.info(
            f"Raft: current logs info, last log term = {last_log_term}, last log index = {last_log_index}"
        )

        # Request votes
        request_vote_req = RaftReqVoteArg(
            term=election_term,
            candidateId=self.id,
            lastLogTerm=last_log_term,
            lastLogIndex=last_log_index,
        )

        # Send request for vote in parallel to all siblings
        siblings = self.localhost.siblings
        pending: set[asyncio.Task[RaftReqVoteResponse]] = set()
        for remote in siblings:
            logger.info(f"Raft: sending vote request to {remote}")
            req = asyncio.create_task(
                self.localhost.call(remote, RAFT_REQ_VOTE, request_vote_req)
            )
            pending.add(req)

        # Wait for the required count of responses
        vote_gained = 1  # we voted for ourself
        error_count = 0

        quorum_count = self.localhost.plant.quorum_count
        while True:
            # Wait for any responses until the end of this election period
            done, pending = await asyncio.wait(
                pending,
                return_when=asyncio.FIRST_COMPLETED,
                timeout=(next_election_time_sec - time.monotonic()) / 1000,
            )

            for task in done:
                try:
                    r = await task
                    self.check_and_bump_term(r.term)
                    if r.voteGranted:
                        vote_gained += 1
                        logger.info(
                            f"Raft: vote gained (gained {vote_gained}, errored {error_count}, pending {len(pending)})"
                        )
                except NetworkException:
                    error_count += 1
                    logger.warning(
                        f"Raft: one vote request failed (gained {vote_gained}, errored {error_count}, pending {len(pending)})"
                    )

            # exit current election if we are in the era of the next election timeout
            if time.monotonic() >= next_election_time_sec:
                logger.warning(f"Raft: timed out, aborting election")
                return

            # after the awaits above, check if we are already in a newer term (started by the election timer loop), abandon the current election
            if self.storage.get_currentTerm() != election_term:
                logger.warning(
                    f"Raft: already in a new term {self.storage.get_currentTerm()}, aborting election for term {election_term}"
                )
                return

            # exit if we can no longer acquire enough votes to form the quorum
            # we will wait for the next election to be started by the election timer
            if vote_gained + len(pending) < quorum_count:
                logger.warning(
                    f"Raft: cannot gain enough vote anymore for this term {election_term}, abort election (gained {vote_gained}, errored {error_count}, pending {len(pending)})"
                )
                return

            if vote_gained >= quorum_count:
                logger.info(
                    f"Raft: majority vote gained, converting to leader (gained {vote_gained}, errored {error_count}, pending {len(pending)})"
                )
                # quorum vote granted, convert to Leader
                self.role = RaftRole.LEADER
                self.leader_id = self.id

                # (Re-)Initialize Leader volatile states
                last_log_index = self.storage.max_index()  # 1-based
                # TODO: handle dynamic node joining
                for server in self.localhost.siblings:
                    self.nextIndex[server.id] = last_log_index + 1
                    self.matchIndex[server.id] = 0

                # append a no-op entry into the log so we can commit everything from previous terms
                try:
                    r = await self.add_log(
                        RaftAddLogArg(data=StateMachineTransaction(instructions=[]))
                    )
                    assert r.successful
                except Exception:
                    logger.error(f"Raft: failed committing no-op after election")
                    logger.error(traceback.format_exc())

                return

    def vote(self, req: RaftReqVoteArg) -> RaftReqVoteResponse:
        logger.info(f"Raft: vote request received for term {req.term}")

        # Check and reset term/voted first if received a new term
        self.check_and_bump_term(req.term)

        granted = True

        if req.term < self.storage.get_currentTerm():
            logger.warning(
                f"Raft: refuse to vote, requesting an older term (vote term {req.term}, current term {self.storage.get_currentTerm()})"
            )
            granted = False

        voted_for = self.storage.get_votedFor()
        if granted and voted_for is not None and voted_for.id != req.candidateId:
            logger.warning(
                f"Raft: refuse to vote, we already voted for {voted_for} in term {req.term}"
            )
            granted = False

        nLogs = self.storage.max_index()

        if granted and nLogs > 0:
            last_log = self.storage.get_log(nLogs)

            if req.lastLogTerm < last_log.term:
                logger.warning(
                    f"Raft: refuse to vote, stale last log term (our: {last_log.term}, request: {req.lastLogTerm})"
                )
                granted = False

            elif req.lastLogTerm == last_log.term and req.lastLogIndex < nLogs:
                logger.warning(
                    f"Raft: refuse to vote,stale last log index (our: {nLogs}, request: {req.lastLogIndex})"
                )
                granted = False

        if granted:
            logger.info(f"Raft: voting for {req.candidateId} in term {req.term}")
            self.storage.set_votedFor(RaftVotedFor(term=req.term, id=req.candidateId))

        return RaftReqVoteResponse(
            term=self.storage.get_currentTerm(), voteGranted=granted
        )

    async def leader_heart_beat_timer(self):
        logger.info("Raft: starting leader heart beat timer")
        while True:
            if self.role == RaftRole.LEADER:
                # only Leader will send out heart beats
                current_time_sec = time.monotonic()
                next_heart_beat_time = (
                    current_time_sec + self.leader_heart_beat_interval_ms / 1000
                )
                term = self.storage.get_currentTerm()
                try:
                    await self.leader_heart_beat(term)
                except Exception as e:
                    logger.error(f"Raft: leader heart beat for term {term} failed")
                    logger.error(traceback.format_exc())
                # sleep until the next heart beat time
                sleep_sec = max(0, next_heart_beat_time - time.monotonic())
                await asyncio.sleep(sleep_sec)
            else:
                # otherwise, check every 10ms for role change
                # TODO: responsive?
                await asyncio.sleep(0.01)

    async def leader_heart_beat(self, heart_beat_term: int):
        logger.debug(f"Raft: leader heart beating")
        # Heart beat by triggering the replication
        pending: set[asyncio.Task[bool]] = set()
        for follower in self.localhost.siblings:
            replication_req = asyncio.create_task(self.replicate_logs_to(follower))
            pending.add(replication_req)

        quorum_count = self.localhost.plant.quorum_count
        while True:
            heart_beat_time = time.monotonic()
            # Wait for any responses until the next heart beat
            done, pending = await asyncio.wait(
                pending,
                return_when=asyncio.FIRST_COMPLETED,
            )
            success = 1  # ourself is always alive
            for task in done:
                try:
                    r = await task
                    if r:
                        success += 1
                except NetworkException:
                    pass

            # heart beat is only used within a term
            if self.storage.get_currentTerm() != heart_beat_term:
                return

            # if there is a newer successful heart beat, use that, abort the current one
            if self.last_successful_heart_beat > heart_beat_time:
                return

            # record last successful heart beat time for read-only operation
            if success >= quorum_count:
                self.last_successful_heart_beat = heart_beat_time
                return

            # if we can't make a successful heart beat anymore, abort
            if success + len(pending) < quorum_count:
                return

    def append_entry(self, req: RaftAppEntArg) -> RaftAppEntResponse:
        logger.debug(f"Raft: append entry request received")

        # Check and reset term/voted first if received a new term
        self.check_and_bump_term(req.term)

        failure_resp = RaftAppEntResponse(
            term=self.storage.get_currentTerm(), success=False
        )

        # Reject stale Leader
        if req.term < self.storage.get_currentTerm():
            logger.warning(
                f"Raft: rejecting append entry request, stale term (current {self.storage.get_currentTerm()}, request {req.term})"
            )
            return failure_resp
        assert req.term == self.storage.get_currentTerm()

        # Set leader from heart beat / append entry request of the current term
        self.leader_id = req.leaderId

        nLogs = self.storage.max_index()

        # Reject new entries if prev is not in local logs and the term doesn't match
        if nLogs < req.prevLogIndex:
            logger.warning(
                f"Raft: rejecting append entry request, prev log index does not exist (current logs length {nLogs}, request prevLogIndex {req.prevLogIndex})"
            )
            return failure_resp
        if req.prevLogIndex != 0:
            prev_log_entry = self.storage.get_log(req.prevLogIndex)
            if prev_log_entry.term != req.prevLogTerm:
                logger.warning(
                    f"Raft: rejecting append entry request, prev log term does not match (current {prev_log_entry}, request prevLogIndex {req.prevLogTerm})"
                )
                return failure_resp

        # If an existing entry conflicts with a new one (same index but different terms)
        # delete the existing entry and all that follow it
        index = req.prevLogIndex
        skip_new_entries = 0
        remove_on_and_after_index = None
        for new_entry in req.entries:
            index += 1
            if nLogs < index:
                break
            if self.storage.get_log(index).term != new_entry.term:
                remove_on_and_after_index = index
                break
            skip_new_entries += 1
        if remove_on_and_after_index is not None:
            logger.info(
                f"Raft: removing conflicting local log entries, on and after index {remove_on_and_after_index}"
            )
            self.storage.truncate_logs(remove_on_and_after_index)
        if skip_new_entries > 0:
            logger.info(
                f"Raft: skipping first {skip_new_entries} new entries in the append request, they already exist in local logs"
            )

        # Append any new entries not already in the log
        logs_to_append = req.entries[skip_new_entries:]
        if len(logs_to_append) > 0:
            logger.info(f"Raft: appending {len(logs_to_append)} new log entries")
            self.storage.append_multiple_logs(logs_to_append)

        # If leaderCommit > commitIndex, set:
        # commitIndex = min(leaderCommit, index of last new entry)
        if req.leaderCommit > self.commitIndex:
            self.commit(req.leaderCommit)

        return RaftAppEntResponse(term=self.storage.get_currentTerm(), success=True)

    def check_and_bump_term(self, new_term: int):
        if new_term < self.storage.get_currentTerm():
            return

        # reset election timer if received a heart beat for the current term
        if new_term == self.storage.get_currentTerm():
            if self.role == RaftRole.LEADER:
                return
            logger.debug("Raft: heart beat received")
            self.reset_next_election_time()
            return

        # if a new term is started, convert to follower and reset the election timer
        logger.info(
            f"Raft: new term detected, current={self.storage.get_currentTerm()}, new={new_term}. Converting to follower"
        )
        self.storage.set_currentTerm(new_term)
        self.role = RaftRole.FOLLOWER
        self.storage.set_votedFor(None)
        self.leader_id = None
        self.reset_next_election_time()
        return

    def apply_state_machine_transaction(
        self, states: dict, instructions: StateMachineTransaction
    ):
        states = {k: v for k, v in states.items()}
        for inst in instructions.instructions:
            if inst.op == "ASSERT":
                assert states.get(inst.key) == inst.val
            elif inst.op == "SET":
                if inst.val is None:
                    del states[inst.key]
                else:
                    states[inst.key] = inst.val
            else:
                raise NotImplementedError()
        return states

    def commit(self, commit_to_index: int):
        assert commit_to_index >= self.commitIndex
        assert commit_to_index >= self.lastApplied
        assert self.commitIndex >= self.lastApplied

        prev_committed = self.commitIndex
        if prev_committed < commit_to_index:
            logger.info(
                f"Raft: committing new entry, index {commit_to_index}, previous committed index {prev_committed}"
            )
            self.commitIndex = commit_to_index

        if self.lastApplied < self.commitIndex:
            logger.info(
                f"Raft: applying new entry to state machine, max index to apply {self.commitIndex}, previous applied index {self.lastApplied}"
            )
            # apply transactions to the state machine
            index = self.lastApplied + 1
            transaction = None
            try:
                while index <= self.commitIndex:
                    logger.debug(
                        f"Raft: applying log at index {index} to state machine"
                    )
                    transaction = self.storage.get_log(index).data
                    self.states = self.apply_state_machine_transaction(
                        self.states, transaction
                    )
                    self.lastApplied = index
                    index += 1
            except Exception as e:
                logger.critical(
                    f"Raft: cannot apply committed transactions to the state machine at index {index}, transaction {transaction}"
                )
                logger.critical(traceback.format_exc())

    async def get_logs(self, arg: RaftGetLogsArg) -> RaftGetLogsResponse:
        max_index = self.storage.max_index()
        if max_index > 0:
            if arg.startIndex is None:
                startIndex = 1
            else:
                startIndex = arg.startIndex
            assert startIndex != 0
            if startIndex < 0:
                startIndex = max_index + (startIndex + 1)
            if arg.endIndex is None:
                endIndex = max_index
            else:
                endIndex = arg.endIndex
            assert endIndex != 0
            if endIndex < 0:
                endIndex = max_index + (endIndex + 1)
            entries: list[RaftIndexedLogEntry] = []
            for index in range(startIndex, endIndex + 1):
                log = self.storage.get_log(index)
                entry = RaftIndexedLogEntry(data=log.data, term=log.term, index=index)
                entries.append(entry)
        else:
            entries = []
        resp = RaftGetLogsResponse(
            entries=entries,
            server_name=self.localhost.config.name,
            server_id=self.localhost.config.id,
            committedIndex=self.commitIndex,
            maxLogsIndex=max_index,
        )
        return resp

    async def forward_to_leader(
        self,
        api: APIConcept[TArg, TResp],
        arg: TArg,
        leader_handler: Callable[[TArg], Awaitable[TResp]],
        max_try: int = 10,
    ) -> TResp:
        try_count = 0
        while True:
            try_count += 1
            try:
                if self.role != RaftRole.LEADER:
                    logger.info(
                        f"Raft: forwarding request to leader {self.leader_id}: {api.endpoint}"
                    )
                    assert (
                        self.leader_id is not None
                    ), f"No leader elected for this term {self.storage.get_currentTerm()}"
                    leader = self.localhost.plant.get_server(self.leader_id)
                    return await self.localhost.call(leader, api, arg)
                else:
                    # for read request, only process it after we have another successful quorum heart beat
                    # such that we know we are still the leader, preventing to return stale info
                    logger.info("Raft: processing quorum request, waiting heart beat")
                    last_heart_beat = self.last_successful_heart_beat
                    while self.last_successful_heart_beat <= last_heart_beat:
                        await asyncio.sleep(0.01)
                        assert self.role == RaftRole.LEADER
                    logger.info("Raft: heart beat wait succeeded, continuing in leader")
                    return await leader_handler(arg)
            except Exception:
                logger.error(
                    f"Raft: Cannot forward request to the leader, will retry. Tried {try_count}/{max_try}"
                )
                logger.error(traceback.format_exc())
            if try_count < max_try:
                await asyncio.sleep(2 * self.election_timeout_ms / 1000)
            else:
                logger.error(
                    f"Raft: Max retry for request to leader reached, tried {try_count} times"
                )
                raise Exception("Max retry reached")

    async def get_states(self, arg: RaftGetStatesArg) -> RaftGetStatesResponse:
        if arg.keys is None:
            states = self.states
        else:
            states = {k: v for k, v in self.states.items() if k in arg.keys}

        return RaftGetStatesResponse(
            server_name=self.localhost.config.name,
            server_id=self.localhost.config.id,
            committedIndex=self.commitIndex,
            maxLogsIndex=self.storage.max_index(),
            states=states,
        )


async def raft_api(request: Request) -> RaftApi:
    return request.app.state.raft


Raft = Annotated[RaftApi, Depends(raft_api)]


##################################
# RPC APIs
##################################


@implement_api(router, RAFT_REQ_VOTE)
async def raft_req_vote(arg: RaftReqVoteArg, raft: Raft):
    logger.debug(f"Raft REQ VOTE {arg}")
    return raft.vote(arg)


@implement_api(router, RAFT_APP_ENT)
async def raft_append_entries(arg: RaftAppEntArg, raft: Raft):
    logger.debug(f"Raft APP ENT {arg}")
    return raft.append_entry(arg)


@implement_api(router, RAFT_ADD_LOG)
async def raft_add_log(arg: RaftAddLogArg, raft: Raft):
    logger.info(f"Raft ADD LOG {arg}")
    return await raft.forward_to_leader(RAFT_ADD_LOG, arg, raft.add_log)


@implement_api(router, RAFT_GET_LOGS)
async def raft_get_logs(arg: RaftGetLogsArg, raft: Raft):
    logger.info(f"Raft GET LOGS {arg}")
    if arg.quorum:
        return await raft.forward_to_leader(RAFT_GET_LOGS, arg, raft.get_logs)
    else:
        return await raft.get_logs(arg)


@implement_api(router, RAFT_GET_STATES)
async def raft_get_states(arg: RaftGetStatesArg, raft: Raft):
    logger.info(f"Raft GET STATES {arg}")
    if arg.quorum:
        return await raft.forward_to_leader(RAFT_GET_STATES, arg, raft.get_states)
    else:
        return await raft.get_states(arg)
