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

We have 3 tests implemented:

1. Read-after-Write consistency test
2. Fault tolerant linearizable consistency test
3. Eventual consistency test

Currently these tests run against the "Attiya, Bar-Noy, Dolev" (ABD) quorum get/set algorithm (`raft/routers/abd.py`), which provides fault-tolerant linearizable consistency, so it will pass all the tests given that we don't try to read from the faulty node(s).
