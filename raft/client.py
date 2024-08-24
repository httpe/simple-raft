from typing import TypeVar, Type
import argparse
import json
import logging
import httpx
from fastapi import status
from pydantic import BaseModel

from .configs import PlantConfig, ServerConfig
from .network import NetworkGeneralException
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

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)

TResp = TypeVar("TResp", bound=BaseModel)


def call_api(
    server: ServerConfig, endpoint: str, arg: BaseModel, resp_class: Type[TResp]
) -> TResp:
    url = server.address.construct_base_url(endpoint)
    r = httpx.post(url, json=arg.model_dump())
    if r.status_code != status.HTTP_200_OK:
        raise NetworkGeneralException(r.status_code)
    return resp_class(**r.json())


def test_tpc_simple_sync(main: ServerConfig, sub: ServerConfig):
    logger.info(f"Two-Phase-Commit: Running simple sync test between {main} and {sub}")
    key = "abc"
    data = "xyz"
    write = call_api(
        main, TPC_SET_ENDPOINT, DBSetArg(key=key, data=data), DBSetResponse
    )
    logger.info(f"TPC SET response: {write}")
    assert write.key == key
    read_sub = call_api(sub, TPC_GET_ENDPOINT, DBGetArg(key=key), DBGetResponse)
    read_main = call_api(main, TPC_GET_ENDPOINT, DBGetArg(key=key), DBGetResponse)
    logger.info(f"TPC GET main response: {read_main}")
    logger.info(f"TPC SET sub response: {read_sub}")
    assert read_sub.entry is not None and read_sub.entry.data == data
    assert read_main.entry is not None and read_main.entry.data == data


def test_tpc(main: ServerConfig, sub: ServerConfig):
    logger.info(f"Start testing Two-Phase-Commit between {main} and {sub}")
    test_tpc_simple_sync(main, sub)
    logger.info(f"Two-Phase-Commit tests finished")


def main():
    args = parse_cml_args()

    with open(args.plant_config, "r") as f:
        config = json.load(f)
    plant = PlantConfig(**config)
    assert plant.proxy is not None

    main = plant.servers[0]
    sub = plant.servers[1]

    logger.info(f"Start running tests")

    test_tpc(main, sub)

    criteria = RequestMatchingCriteria(
        origin_names=None, dest_names=None, endpoints=None
    )
    call_api(
        plant.proxy,
        PROXY_RULE_SET_ENDPOINT,
        ProxySetRuleArg(rule="drop", id="drop_all", criteria=criteria),
        ProxySetRuleResponse,
    )

    try:
        test_tpc(main, sub)
        assert False
    except NetworkGeneralException as e:
        logger.info(f"Expected error: {e}")
        pass

    call_api(
        plant.proxy,
        PROXY_CLEAR_RULES_ENDPOINT,
        ProxyClearRulesArg(rule="drop", ids=["drop_all"]),
        ProxyClearRulesResponse,
    )

    test_tpc(main, sub)

    logger.info(f"Tests finished running")


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
