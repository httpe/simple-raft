import argparse
import json
import logging
import httpx
from fastapi import status
from .configs import PlantConfig, ServerConfig
from .routers.tpc import DBGetResponse, DBSetResponse
from .proxy import RequestMatchingCriteria
from .network import NetworkGeneralException

logging.basicConfig(format="%(asctime)s %(levelname)s:%(message)s", level=logging.INFO)
logger = logging.getLogger(__name__)


def tpc_set(server: ServerConfig, id: str, data: str):
    url = server.address.construct_base_url(f"/tpc/set/{id}")
    r = httpx.get(url, params={"data": data})
    if r.status_code != status.HTTP_200_OK:
        raise NetworkGeneralException(r.status_code)
    return DBSetResponse(**r.json())


def tpc_get(server: ServerConfig, id: str) -> DBGetResponse:
    url = server.address.construct_base_url(f"/tpc/get/{id}")
    r = httpx.get(url)
    if r.status_code != status.HTTP_200_OK:
        raise NetworkGeneralException(r.status_code)
    return DBGetResponse(**r.json())


def proxy_set_drop_rule(
    proxy: ServerConfig, id: str, criteria: RequestMatchingCriteria
):
    url = proxy.address.construct_base_url(f"/proxy/set/drop_rule/{id}")
    r = httpx.post(url, json=criteria.model_dump())
    assert r.status_code == status.HTTP_200_OK


def proxy_clear_drop_rules(proxy: ServerConfig, ids: list[str] | None = None):
    url = proxy.address.construct_base_url(f"/proxy/clear/drop_rules")
    r = httpx.get(url, params={"ids": ids})
    assert r.status_code == status.HTTP_200_OK


def test_tpc_simple_sync(main: ServerConfig, sub: ServerConfig):
    logger.info(f"Two-Phase-Commit: Running simple sync test between {main} and {sub}")
    id = "abc"
    data = "xyz"
    write = tpc_set(main, id, data)
    logger.info(f"TPC SET response: {write}")
    assert write.id == id
    read_sub = tpc_get(sub, id)
    read_main = tpc_get(main, id)
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
    proxy_set_drop_rule(plant.proxy, "drop_all", criteria)

    try:
        test_tpc(main, sub)
        assert False
    except NetworkGeneralException as e:
        logger.info(f"Expected error: {e}")
        pass

    proxy_clear_drop_rules(plant.proxy, ids=["drop_all"])

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
