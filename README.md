# Simple-Raft (Python Raft Server)

<!-- @import "[TOC]" {cmd="toc" depthFrom=1 depthTo=6 orderedList=false} -->

<!-- code_chunk_output -->

- [Simple-Raft (Python Raft Server)](#simple-raft-python-raft-server)
  - [Goals](#goals)
  - [Initial Setup](#initial-setup)
  - [Start Servers](#start-servers)
  - [Combining Logs](#combining-logs)
  - [Running Tests](#running-tests)
  - [Reference](#reference)

<!-- /code_chunk_output -->

## Goals

This project is developed with the following goals in mind:

- Implement a simple Raft algorithm in Python with an abstract networking/communication and persisted storage layer
- Provide tools to test and trace the reliability of the implementation, including a network proxy to simulate network partition, packet drops/delays etc.

## Initial Setup

This repos is tested with Python 3.10 under WSL Ubuntu:

```bash
python -m venv venv
source ./venv/bin/activate
python -m pip install -v --upgrade pip 
pip install -v -r requirements.txt
```

## Start Servers

All options are configured in `config.json`.

To run 3 servers named `A`, `B`, `C`, and a network proxy:

Run each command in a separate terminal:

```powershell
python -m raft.main ./config.json A
```

```powershell
python -m raft.main ./config.json B
```

```powershell
python -m raft.main ./config.json C
```

```powershell
python -m raft.main ./config.json Proxy
```

Persisted databases will be written to the `db/` folder.

## Combining Logs

Logs will be written to `logs/` folder, and you can consolidate all log files into one by:

```powershell
python -m raft.monitor
```

This can help you understand the timeline for a particular issue across different servers.

## Running Tests

You can run tests against the servers with:

```powershell
python -m raft.client
```

We have implemented 3 generic correctness tests:

- Read-after-Write consistency test
  1. Write to one node
  2. Read immediately another node, make sure it returns what we just wrote
- Fault tolerant linearizable consistency test:
  1. Isolate one node out of the network
  2. Write to another node
  3. Read from a third node, make sure it returns what we wrote
  4. Resume network
  5. Read immediately the previously isolated node, make sure it returns what we wrote
- Eventual consistency test:
  1. Isolate one node out of the network
  2. Write to another node
  3. Poll periodically and make sure that eventually all nodes return the data we wrote

, and 2 performance tests:

- Write performance test: write 100 unique values sequentially, each to a randomly selected node
- Read performance test: read 100 times sequentially, each to a random node

Currently these tests run against both the "Attiya, Bar-Noy, Dolev" (ABD) quorum get/set algorithm (`raft/routers/abd.py`), and the Raft algorithm (`raft/routers/raft.py`).

Both algorithms provide fault-tolerant linearizable consistency, so it will pass all the tests given that we don't try to read from the faulty node(s).

We also have a simple test for the Raft state machine, where we append fibonacci numbers sequentially only after validating (at the Raft server side) the previous state is unchanged, i.e. atomic compare and swap.

## Reference

Video Lectures:

1. Distributed Systems, Cambridge 2122 (Martin Kleppmann): [Youtube](https://www.youtube.com/playlist?list=PLeKd45zvjcDFUEv_ohr_HdUFe97RItdiB)
2. Distributed Systems, MIT 6.824 (Robert Morris): [Youtube](https://www.youtube.com/@6.824)

Papers:

1. In Search of an Understandable Consensus Algorithm [PDF](https://raft.github.io/raft.pdf)
2. Consensus: Bridging Theory and Practice [PDF](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
3. Designing Data-Intensive Applications (Martin Kleppmann)
4. Distributed Systems (Maarten van Steen) [PDF](https://www.distributed-systems.net/index.php/books/ds3/)
