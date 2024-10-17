# Simple-Raft (Python Raft Server)

## Goals

- A simple Raft implementation in Python with an abstract networking/communication and persisted storage layer
- A set of tools to test and trace the reliability of the implementation, including a network proxy to simulate network partition, packet drops/delays etc.

## Initial Setup

For development, this repos is tested against Python 3.10 under WSL Ubuntu:

```bash
python -m venv venv
source ./venv/bin/activate
python -m pip install -v --upgrade pip 
pip install -v -r requirements.txt
```

## Start Servers

All options are configured in `config.json`.

To run 3 servers `A`, `B`, `C`, and the network proxy:

Run each in a separate terminal:

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

Logs will be written to `logs/` folder, and you can consolidate all log files into one by:

```powershell
python -m raft.monitor
```

You can run test against the servers with:

```powershell
python -m raft.client
```

We have 3 generic tests implemented:

1. Read-after-Write consistency test
2. Fault tolerant linearizable consistency test
3. Eventual consistency test

Currently these tests run against both the "Attiya, Bar-Noy, Dolev" (ABD) quorum get/set algorithm (`raft/routers/abd.py`), and the Raft algorithm (`raft/routers/raft.py`).

They both provide fault-tolerant linearizable consistency, so it will pass all the tests given that we don't try to read from the faulty node(s).

We also have a simple test for the Raft state machine, where we append fibonacci numbers sequentially, but only after validating the previous state is unchanged at the Raft server side.

## Reference

Video Lectures:

1. Distributed Systems, Cambridge 2122 (Martin Kleppmann): [Youtube](https://www.youtube.com/playlist?list=PLeKd45zvjcDFUEv_ohr_HdUFe97RItdiB)
2. Distributed Systems, MIT 6.824 (Robert Morris): [Youtube](https://www.youtube.com/@6.824)

Papers:

1. In Search of an Understandable Consensus Algorithm [PDF](https://raft.github.io/raft.pdf)
2. Consensus: Bridging Theory and Practice [PDF](https://web.stanford.edu/~ouster/cgi-bin/papers/OngaroPhD.pdf)
3. Designing Data-Intensive Applications (Martin Kleppmann)
4. Distributed Systems (Maarten van Steen) [PDF](https://www.distributed-systems.net/index.php/books/ds3/)
