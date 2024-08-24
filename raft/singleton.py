from typing import TypeVar, Type
import argparse
import json

from pydantic import BaseModel

from .logger import logger

from .configs import PlantConfig
from .network import (
    NetworkInterface,
    HttpNetworkInterface,
    HttpNetworkInterfaceWithProxy,
)

TResp = TypeVar("TResp", bound=BaseModel)


class Server:
    def __init__(
        self, plant: PlantConfig, network: NetworkInterface, name: str
    ) -> None:
        self.plant = plant
        self.name = name
        self.config = plant.get_server(name)
        self.network = network

    def call(
        self, endpoint: str, resp_class: Type[TResp], arg: BaseModel | None = None
    ) -> TResp:
        assert self != server, "Cannot call oneself"

        if arg is None:
            body = None
        else:
            body = arg.model_dump()

        resp = self.network.call(
            self.config.address,
            endpoint,
            body=body,
            timeout=self.config.timeout,
        )

        return resp_class.model_validate_json(resp)

    @property
    def siblings(self):
        siblings = [
            self.__class__(self.plant, self.network, x.name)
            for x in self.plant.servers
            if x.name != self.name
        ]
        return siblings

    def get_server(self, name: str):
        return self.__class__(self.plant, self.network, name)


#################################
# CMD Args and Config Parsing
#################################


def parse_cml_args():
    parser = argparse.ArgumentParser(
        prog="Demo Server",
        description="Demo Server",
        epilog="Run a demo server",
    )
    parser.add_argument(
        "plant_config", help="Path to the plant configuration JSON file"
    )
    parser.add_argument(
        "server_name", help="Start the server of this name in the plant config"
    )
    args = parser.parse_args()
    return args


def get_current_server(plant_config_path: str, server_name: str):
    with open(plant_config_path, "r") as f:
        config = json.load(f)
    plant = PlantConfig(**config)

    server = plant.get_server(server_name)
    logger.info(f"Start server: {server}")

    # Use proxy if set so
    if plant.use_proxy and plant.proxy is not None and server_name != plant.proxy.name:
        logger.warning(f"Using proxy: {plant.proxy.address}")
        network = HttpNetworkInterfaceWithProxy(
            self_addr=server.address, proxy_addr=plant.proxy.address
        )
    else:
        network = HttpNetworkInterface(self_addr=server.address)

    server = Server(plant, network, server_name)

    return server


#################################
# Initialize Global Singleton
#################################

args = parse_cml_args()
server = get_current_server(args.plant_config, args.server_name)
