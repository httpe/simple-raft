from typing import TypeVar, Type, Annotated
from functools import lru_cache
from asyncio import create_task, wait, Task
from asyncio.tasks import FIRST_COMPLETED

from pydantic import BaseModel
from fastapi import Request, Depends
from httpx import AsyncClient

from .configs import PlantConfig, ServerConfig
from .network import (
    NetworkInterface,
    HttpNetworkInterfaceWithProxy,
    HttpNetworkInterface,
    NetworkException,
)
from .persist import PersistedStorage
from .logger import logger

TResp = TypeVar("TResp", bound=BaseModel)


class Server:
    def __init__(
        self, plant: PlantConfig, network: NetworkInterface, name: str
    ) -> None:
        self.plant = plant
        self.name = name
        self.config = plant.get_server(name)
        self.network = network

    def get_server(self, name: str):
        return self.__class__(self.plant, self.network, name)


class LocalServer:
    def __init__(self, plant: PlantConfig, local_name: str) -> None:

        local_server_config = plant.get_server(local_name)

        # Initialize local network interface
        http_client = AsyncClient()
        # Use proxy if set so
        if (
            plant.use_proxy
            and plant.proxy is not None
            and local_name != plant.proxy.name
        ):
            logger.warning(f"Using proxy: {plant.proxy.address}")
            network = HttpNetworkInterfaceWithProxy(
                self_addr=local_server_config.address,
                proxy_addr=plant.proxy.address,
                http_client=http_client,
            )
        else:
            network = HttpNetworkInterface(
                self_addr=local_server_config.address, http_client=http_client
            )

        # Initialize local storage
        storage = PersistedStorage(local_server_config.db_path)

        self.plant = plant
        self.name = local_name
        self.config = local_server_config
        self.network = network
        self.storage = storage

    async def call(
        self,
        server: ServerConfig,
        endpoint: str,
        resp_class: Type[TResp],
        arg: BaseModel | None = None,
    ) -> TResp:

        if arg is None:
            body = None
        else:
            body = arg.model_dump()

        resp = await self.network.call(
            server.address,
            endpoint,
            body=body,
            timeout=self.config.timeout,
        )

        return resp_class.model_validate_json(resp)

    @property
    def siblings(self):
        return [x for x in self.plant.servers if x.name != self.name]

    async def call_multiple(
        self,
        servers: list[ServerConfig],
        min_resp_required: int,
        endpoint: str,
        resp_class: Type[TResp],
        arg: BaseModel | None = None,
    ) -> list[TResp]:
        logger.info(
            f"Calling endpoint {endpoint} for at least {min_resp_required} responses"
        )

        assert min_resp_required <= len(servers)

        pending: set[Task[TResp]] = set()

        for remote in servers:
            req = create_task(self.call(remote, endpoint, resp_class, arg))
            pending.add(req)

        # Wait for the required count of responses
        responses: list[TResp] = []
        n_errored = 0

        while True:
            done, pending = await wait(pending, return_when=FIRST_COMPLETED)
            for task in done:
                try:
                    r = await task
                    responses.append(r)
                    logger.info(
                        f"Received a response from endpoint {endpoint}, received/pending/errored/required {len(responses)}/{len(pending)}/{n_errored}/{min_resp_required}"
                    )
                except NetworkException as e:
                    n_errored += 1
                    logger.error(
                        f"Network error from endpoint {endpoint}, received/pending/errored/required {len(responses)}/{len(pending)}/{n_errored}/{min_resp_required}"
                    )
                    if len(responses) + len(pending) < min_resp_required:
                        msg = f"Failed obtaining required count of responses for endpoint {endpoint}, received/pending/errored/required {len(responses)}/{len(pending)}/{n_errored}/{min_resp_required}"
                        logger.error(msg)
                        raise NetworkException(msg) from e

            if len(responses) >= min_resp_required:
                logger.info(
                    f"Required responses of endpoint {endpoint} gathered, received/pending/errored/required {len(responses)}/{len(pending)}/{n_errored}/{min_resp_required}"
                )
                break

        return responses


async def _local_server(request: Request) -> LocalServer:
    return request.app.state.localhost


LocalHost = Annotated[LocalServer, Depends(_local_server)]
