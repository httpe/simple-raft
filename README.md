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

## TODO

- Implement two phase commit
- Develop test suits to prove its reliability
- Implement the Raft algorithm
