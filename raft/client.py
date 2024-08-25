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
        raise Exception(r.status_code)
    return resp_class(**r.json())


def gen_random_str(N: int):
    return "".join(random.choices(string.ascii_letters + string.digits, k=N))


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


def test_two_server_sync(main: ServerConfig, sub: ServerConfig, db: DBInterface):
    logger.info(f"Test started: simple sync between {main} and {sub}")
    key = "test_two_server_sync"
    # Write data
    data = gen_random_str(10)
    logger.info(f"Writing data, key: {key}, data: {data}")
    db.write(main, key, data)
    assert db.read(sub, key) == data
    assert db.read(main, key) == data
    # Update data
    data_alt = gen_random_str(10)
    logger.info(f"Updating data, key: {key}, data: {data_alt}")
    db.write(sub, key, data_alt)
    assert db.read(sub, key) == data_alt
    assert db.read(main, key) == data_alt
    # Remove data
    logger.info(f"Removing data, key: {key}")
    db.write(sub, key, None)
    assert db.read(main, key) is None
    assert db.read(sub, key) is None
    logger.info(f"Test completed: simple sync between {main} and {sub}")


def test_eventual_consistency_after_network_partition(
    proxy: ServerConfig,
    main: ServerConfig,
    sub: ServerConfig,
    db: DBInterface,
    timeout_sec=10,
):
    logger.info(
        f"Test started: Eventual consistency after network partition between {main} and {sub}"
    )

    # Make proxy to drop all packets, i.e. introduce network partition
    logger.info(f"Introducing network partition")
    criteria = RequestMatchingCriteria(
        origin_names=None, dest_names=None, endpoints=None
    )
    call_api(
        proxy,
        PROXY_RULE_SET_ENDPOINT,
        ProxySetRuleArg(rule="drop", id="drop_all", criteria=criteria),
        ProxySetRuleResponse,
    )

    key = "test_eventual_consistency_after_network_partition"

    # Write data
    data = gen_random_str(10)
    logger.info(f"Writing data during network partition, key: {key}, data: {data}")
    # We expect the data write and read on the main node to be successful
    db.write(main, key, data)
    assert db.read(main, key) == data
    # sub node should work but doesn't need to be consistent
    sub_data = db.read(sub, key)

    # Resume network
    logger.info(f"Resuming network")
    call_api(
        proxy,
        PROXY_CLEAR_RULES_ENDPOINT,
        ProxyClearRulesArg(rule="drop", ids=["drop_all"]),
        ProxyClearRulesResponse,
    )

    # Wait for eventual consistency to realize until timeout
    logger.info(f"Waiting for eventual consistency to realize")
    t0 = time.time()
    while time.time() - t0 < timeout_sec:
        sub_data_next = db.read(sub, key)
        if sub_data_next != sub_data:
            assert sub_data_next == data
            logger.info(f"Sub node state is synced now")
            break
        else:
            assert sub_data_next == sub_data
            logger.info(f"Sub node state not synced yet, will retry in 0.5s...")
        time.sleep(0.5)

    logger.info(
        f"Test completed: Eventual consistency after network partition between {main} and {sub}"
    )


def test_all(plant: PlantConfig, db: DBInterface):
    logger.info(f"Start running all tests")

    # Normal test
    test_two_server_sync(plant.servers[0], plant.servers[1], db)

    # Network partition test
    assert plant.proxy is not None
    logger.info(f"Using proxy {plant.proxy}")
    test_eventual_consistency_after_network_partition(
        plant.proxy, plant.servers[0], plant.servers[1], db
    )

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
