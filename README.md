# Python Raft Server

## Goals

- A simple Raft implementation in Python with an abstract networking/communication layer
- A set of tools to test and trace the reliability of the implementation, including a network proxy to simulate network partition

## Initial Setup

For development, I am using Python 3.10 under Windows + PowerShell:

```powershell
python -m venv venv
.\venv\Scripts\Activate.ps1
python -m pip install -v --upgrade pip 
pip install -v -r requirements.txt
```

## Start Servers

All options are configured in `config.json`.

To run two servers `A` and `B` and the network proxy:

In one terminal:

```powershell
python -m raft.server ./config.json A
```

In another terminal:

```powershell
python -m raft.server ./config.json B
```

In yet another terminal:

```powershell
python -m raft.server ./config.json Proxy
```

Logs will be written to `logs/` folder, and you can consolidate all log files into one by:

```powershell
python -m raft.monitor
```

You can run test against the servers with:

```powershell
python -m raft.client
```
