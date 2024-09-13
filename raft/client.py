from typing import TypeVar, Type
from abc import ABC, abstractmethod
import random
import string
import argparse
import json
import logging
import time

import httpx
from fastapi import status
from pydantic import BaseModel

from .configs import PlantConfig, ServerConfig
from .api import (
    TPC_GET_ENDPOINT,
    DBGetArg,
    DBGetResponse,
    TPC_SET_ENDPOINT,
    DBSetArg,
    DBSetResponse,
    RequestMatchingCriteria,
    PROXY_RULE_SET_ENDPOINT,
    ProxySetRuleArg,
    ProxySetRuleResponse,
    PROXY_CLEAR_RULES_ENDPOINT,
    ProxyClearRulesResponse,
    ProxyClearRulesArg,
)

#############################################
## Logging
#############################################

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

TResp = TypeVar("TResp", bound=BaseModel)

#############################################
## Util
#############################################


def call_api(
    server: ServerConfig, endpoint: str, arg: BaseModel, resp_class: Type[TResp]
) -> TResp:
    url = server.address.construct_base_url(endpoint)
    r = httpx.post(url, json=arg.model_dump())
    if r.status_code != status.HTTP_200_OK:
        raise Exception(f"status_code={r.status_code}")
    return resp_class(**r.json())


def gen_random_str(N: int):
    return "".join(random.choices(string.ascii_letters + string.digits, k=N))


def proxy_drop_all(proxy: ServerConfig):
    logger.info(f"Introducing network partition, make proxy to drop all packets")
    criteria = RequestMatchingCriteria(
        origin_names=None, dest_names=None, endpoints=None
    )
    call_api(
        proxy,
        PROXY_RULE_SET_ENDPOINT,
        ProxySetRuleArg(rule="drop", id="drop_all", criteria=criteria),
        ProxySetRuleResponse,
    )


def proxy_resume_all(proxy: ServerConfig):
    logger.info(f"Resume network, clearing all proxy rules")
    call_api(
        proxy,
        PROXY_CLEAR_RULES_ENDPOINT,
        ProxyClearRulesArg(rule="drop", ids=["drop_all"]),
        ProxyClearRulesResponse,
    )


#############################################
## Abstract Test Cases
#############################################


class DBInterface(ABC):
    @abstractmethod
    def read(self, server: ServerConfig, key: str) -> str | None:
        pass

    @abstractmethod
    def write(self, server: ServerConfig, key: str, data: str | None):
        pass


def test_get_set_linearizability(servers: list[ServerConfig], db: DBInterface):
    logger.info(f"Test started: get/set linearizability between {servers}")

    key = "test_get_set_linearizability"
    servers = list(servers)

    # Write data
    random.shuffle(servers)
    data = gen_random_str(10)
    logger.info(f"Writing data, key: {key}, data: {data}")
    db.write(random.choice(servers), key, data)
    for s in servers:
        assert db.read(s, key) == data

    # Update data
    random.shuffle(servers)
    data_alt = gen_random_str(10)
    logger.info(f"Updating data, key: {key}, data: {data_alt}")
    db.write(random.choice(servers), key, data_alt)
    for s in servers:
        assert db.read(s, key) == data_alt

    # Remove data
    random.shuffle(servers)
    logger.info(f"Removing data, key: {key}")
    db.write(random.choice(servers), key, None)
    for s in servers:
        assert db.read(s, key) is None
    logger.info(f"Test completed: get/set linearizability between {servers}")


def test_eventual_consistency_after_network_partition(
    proxy: ServerConfig,
    servers: list[ServerConfig],
    db: DBInterface,
    timeout_sec=10,
):
    logger.info(
        f"Test started: Eventual consistency after network partition between {servers}"
    )

    servers = list(servers)
    random.shuffle(servers)

    # Make proxy to drop all packets, i.e. introduce network partition
    proxy_drop_all(proxy)

    key = "test_eventual_consistency_after_network_partition"

    # Write data
    data = gen_random_str(10)
    logger.info(f"Writing data during network partition, key: {key}, data: {data}")
    # We expect the data write and read on a node to be successful
    main_node = random.choice(servers)
    db.write(main_node, key, data)

    node_data: dict[str, str | None] = {}
    for s in servers:
        if s == main_node:
            # main node should preserve write-after-read consistency
            resp = db.read(s, key)
            assert data == resp
        else:
            # other nodes should still work but doesn't need to be consistent
            resp = db.read(s, key)
        node_data[s.name] = resp
    logger.info(f"Current data for nodes: {node_data}")

    # Resume network
    proxy_resume_all(proxy)

    # Wait for eventual consistency to realize until timeout
    logger.info(f"Waiting for eventual consistency to realize")
    t0 = time.time()
    while time.time() - t0 < timeout_sec:
        all_same = True
        for s in servers:
            resp = db.read(s, key)
            node_data[s.name] = resp
            all_same = all_same and resp == data
        logger.info(f"Current data for nodes: {node_data}")
        if all_same:
            logger.info(f"All nodes are synced now")
            break

        logger.info(f"Sub node state not synced yet, will retry in 0.5s...")
        time.sleep(0.5)

    logger.info(
        f"Test completed: Eventual consistency after network partition between {servers}"
    )


def test_all(plant: PlantConfig, db: DBInterface):
    logger.info(f"Start running all tests")

    # Normal test
    test_get_set_linearizability(plant.servers, db)

    # Network partition test
    assert plant.proxy is not None
    logger.info(f"Using proxy {plant.proxy}")
    test_eventual_consistency_after_network_partition(plant.proxy, plant.servers, db)

    logger.info(f"All tests finished")


#############################################
## Two Phase Commit
#############################################


class TPC(DBInterface):
    def read(self, server: ServerConfig, key: str) -> str | None:
        r = call_api(server, TPC_GET_ENDPOINT, DBGetArg(key=key), DBGetResponse)
        if r.entry is None:
            return None
        return r.entry.data

    def write(self, server: ServerConfig, key: str, data: str | None):
        call_api(server, TPC_SET_ENDPOINT, DBSetArg(key=key, data=data), DBSetResponse)


#############################################
## Main
#############################################


def main():
    args = parse_cml_args()

    with open(args.plant_config, "r") as f:
        config = json.load(f)
    plant = PlantConfig(**config)
    assert plant.proxy is not None

    # Test two phase commit
    db = TPC()

    test_all(plant, db)


def parse_cml_args():
    parser = argparse.ArgumentParser(
        prog="Test Client",
        description="Test Client",
        epilog="Run a test client",
    )
    parser.add_argument(
        "--plant_config",
        default="config.json",
        help="Path to the plant configuration JSON file",
    )
    args = parser.parse_args()
    return args


if __name__ == "__main__":
    main()
