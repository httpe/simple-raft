from typing import TypeVar, Type, Literal
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
    APIConcept,
    ABD_GET,
    ABD_SET,
    RequestMatchingCriteria,
    PROXY_SET_RULE,
    PROXY_CLEAR_RULE,
)

#############################################
## Logging
#############################################

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

#############################################
## Constants
#############################################

TArg = TypeVar("TArg", bound=BaseModel)
TResp = TypeVar("TResp", bound=BaseModel)

#############################################
## Util
#############################################


def call_api(server: ServerConfig, api: APIConcept[TArg, TResp], arg: TArg) -> TResp:
    url = server.address.construct_base_url(api.endpoint)
    r = httpx.post(url, json=arg.model_dump())
    if r.status_code != status.HTTP_200_OK:
        raise Exception(f"status_code={r.status_code}")
    return api.ResponseClass(**r.json())


def gen_random_str(N: int):
    return "".join(random.choices(string.ascii_letters + string.digits, k=N))


def proxy_set_rule(
    proxy: ServerConfig,
    id: str,
    rule: Literal["drop", "timeout"],
    orig_names: list[str] | None = None,
    dest_names: list[str] | None = None,
    endpoints: list[str] | None = None,
):
    logger.info(
        f"Introducing network partition, make proxy to '{rule}' all packets from {orig_names} to {dest_names} for endpoints {endpoints}"
    )
    criteria = RequestMatchingCriteria(
        origin_names=orig_names, dest_names=dest_names, endpoints=endpoints
    )
    call_api(
        proxy,
        PROXY_SET_RULE,
        PROXY_SET_RULE.ArgumentClass(rule=rule, id=id, criteria=criteria),
    )


def proxy_clear_rules(proxy: ServerConfig, ids: list[str] | None):
    logger.info(f"Resume network, clearing proxy rules of ids {ids}")
    call_api(
        proxy, PROXY_CLEAR_RULE, PROXY_CLEAR_RULE.ArgumentClass(rule="drop", ids=ids)
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


def test_read_after_write_consistency(servers: list[ServerConfig], db: DBInterface):
    logger.info(
        f"Test started: read after write consistency between {[x.name for x in servers]}"
    )

    key = "test_read_after_write_consistency"
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


def test_fault_tolerant_linearizability(
    proxy: ServerConfig, servers: list[ServerConfig], db: DBInterface
):
    server_names = [x.name for x in servers]

    logger.info(f"Test started: Fault tolerant linearizability between {server_names}")

    servers = list(servers)
    random.shuffle(servers)

    faulty_node = random.choice(servers)
    good_nodes = [s for s in servers if s != faulty_node]

    # Make proxy to drop all packets to the faulty node
    # i.e. introduce network partition
    proxy_set_rule(proxy, "partition_dest", "drop", dest_names=[faulty_node.name])
    proxy_set_rule(proxy, "partition_src", "drop", orig_names=[faulty_node.name])

    key = "test_fault_tolerant_linearizability"

    # Write data
    data = gen_random_str(10)
    main_node = random.choice(good_nodes)
    logger.info(
        f"Writing data during network partition, to_node: {main_node.name}, key: {key}, data: {data}"
    )
    # We expect the data write and read on a non-faulty node to be successful
    db.write(main_node, key, data)

    node_data: dict[str, str | None] = {}
    for s in good_nodes:
        node_data[s.name] = db.read(s, key)
        # good nodes should preserve read-after-write consistency
        assert data == node_data[s.name]

    logger.info(f"Current data for nodes: {node_data}")

    # Resume network
    logger.info(f"Resume network to the faulty node {faulty_node.name}")
    proxy_clear_rules(proxy, ["partition_src", "partition_dest"])

    # read from the faulty node should give us the latest value
    assert data == db.read(faulty_node, key)

    # all nodes should returns the same data
    for s in servers:
        node_data[s.name] = db.read(s, key)
        # good nodes should preserve read-after-write consistency
        assert data == node_data[s.name]
    logger.info(f"Current data for nodes: {node_data}")

    logger.info(
        f"Test completed: Fault tolerant linearizability between {server_names}"
    )


def test_eventual_consistency(
    proxy: ServerConfig,
    servers: list[ServerConfig],
    db: DBInterface,
    timeout_sec=10,
    read_faulty_nodes=False,
):
    server_names = [x.name for x in servers]

    logger.info(
        f"Test started: Eventual consistency after network partition between {server_names}"
    )

    servers = list(servers)
    random.shuffle(servers)

    faulty_node = random.choice(servers)
    good_nodes = [s for s in servers if s != faulty_node]

    # Make proxy to drop all packets to the faulty node
    # i.e. introduce network partition
    proxy_set_rule(proxy, "partition_dest", "drop", dest_names=[faulty_node.name])
    proxy_set_rule(proxy, "partition_src", "drop", orig_names=[faulty_node.name])

    key = "test_eventual_consistency_after_network_partition"

    # Write data
    data = gen_random_str(10)
    main_node = random.choice(good_nodes)
    logger.info(
        f"Writing data during network partition, to_node: {main_node.name}, key: {key}, data: {data}"
    )
    # We expect the data write and read on a non-faulty node to be successful
    db.write(main_node, key, data)

    node_data: dict[str, str | None] = {}
    for s in servers:
        if not read_faulty_nodes and s not in good_nodes:
            continue
        node_data[s.name] = db.read(s, key)
        if s in good_nodes:
            # good nodes should preserve read-after-write consistency
            assert data == node_data[s.name]
        else:
            # faulty node can return stale data but it should still work
            pass

    logger.info(f"Current data for nodes: {node_data}")

    # Resume network
    logger.info(f"Resume network to the faulty node {faulty_node.name}")
    proxy_clear_rules(proxy, ["partition_src", "partition_dest"])

    # Wait for eventual consistency to realize until timeout
    logger.info(f"Waiting for eventual consistency to realize")
    t0 = time.time()
    while time.time() - t0 < timeout_sec:
        all_same = True
        for s in servers:
            node_data[s.name] = db.read(s, key)
            all_same = all_same and data == node_data[s.name]
        logger.info(f"Current data for nodes: {node_data}")
        if all_same:
            logger.info(f"All nodes are synced now")
            break

        logger.info(f"Sub node state not synced yet, will retry in 0.5s...")
        time.sleep(0.5)

    logger.info(
        f"Test completed: Eventual consistency after network partition between {server_names}"
    )


def test_all(plant: PlantConfig, db: DBInterface):
    logger.info(f"Start running all tests")

    assert plant.proxy is not None
    logger.info(f"Using proxy {plant.proxy}")

    # clear all proxy rules before running tests
    proxy_clear_rules(plant.proxy, None)

    # Normal test
    test_read_after_write_consistency(plant.servers, db)  # ABD should pass this

    # Fault tolerance tests
    test_fault_tolerant_linearizability(
        plant.proxy, plant.servers, db
    )  # ABD should pass this

    # ABD will fail this if we try to read faulty node
    test_eventual_consistency(plant.proxy, plant.servers, db, read_faulty_nodes=False)

    logger.info(f"All tests finished")


#############################################
## ADB Algorithm
#############################################


class ABD(DBInterface):
    def read(self, server: ServerConfig, key: str) -> str | None:
        r = call_api(server, ABD_GET, ABD_GET.ArgumentClass(key=key))
        if r.entry is None:
            return None
        return r.entry.data

    def write(self, server: ServerConfig, key: str, data: str | None):
        call_api(server, ABD_SET, ABD_SET.ArgumentClass(key=key, data=data))


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
    db = ABD()

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
