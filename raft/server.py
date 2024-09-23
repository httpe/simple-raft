from typing import (
    Annotated,
    Callable,
    Coroutine,
    Any,
)
from asyncio import create_task, wait, Task
from asyncio.tasks import FIRST_COMPLETED

from fastapi import Request, Depends, APIRouter

from .configs import PlantConfig, ServerConfig
from .network import (
    HttpNetworkInterface,
    NetworkException,
)
from .persist import PersistedStorage
from .logger import logger
from .api import APIConcept, TArg, TResp


class LocalServer:
    def __init__(
        self, plant: PlantConfig, local_name: str, network: HttpNetworkInterface
    ) -> None:

        local_server_config = plant.get_server(local_name)

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
        api: APIConcept[TArg, TResp],
        arg: TArg | None = None,
    ) -> TResp:

        if arg is None:
            body = None
        else:
            body = arg.model_dump()

        resp = await self.network.call(
            server.address,
            api.endpoint,
            body=body,
            timeout=self.config.timeout,
        )

        return api.ResponseClass.model_validate_json(resp)

    @property
    def siblings(self):
        return [x for x in self.plant.servers if x.name != self.name]

    async def call_multiple(
        self,
        servers: list[ServerConfig],
        min_resp_required: int,
        api: APIConcept[TArg, TResp],
        arg: TArg | None = None,
    ) -> list[TResp]:
        logger.info(
            f"Calling endpoint {api.endpoint} for at least {min_resp_required} responses"
        )

        assert min_resp_required <= len(servers)

        pending: set[Task[TResp]] = set()

        for remote in servers:
            req = create_task(self.call(remote, api, arg))
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
                        f"Received a response from endpoint {api.endpoint}, received/pending/errored/required {len(responses)}/{len(pending)}/{n_errored}/{min_resp_required}"
                    )
                except NetworkException as e:
                    n_errored += 1
                    logger.error(
                        f"Network error from endpoint {api.endpoint}, received/pending/errored/required {len(responses)}/{len(pending)}/{n_errored}/{min_resp_required}"
                    )
                    if len(responses) + len(pending) < min_resp_required:
                        msg = f"Failed obtaining required count of responses for endpoint {api.endpoint}, received/pending/errored/required {len(responses)}/{len(pending)}/{n_errored}/{min_resp_required}"
                        logger.error(msg)
                        raise NetworkException(msg) from e

            if len(responses) >= min_resp_required:
                logger.info(
                    f"Required responses of endpoint {api.endpoint} gathered, received/pending/errored/required {len(responses)}/{len(pending)}/{n_errored}/{min_resp_required}"
                )
                break

        return responses


async def _local_server(request: Request) -> LocalServer:
    return request.app.state.localhost


LocalHost = Annotated[LocalServer, Depends(_local_server)]

#####################################################################
## API
#####################################################################


def implement_api(router: APIRouter, api: APIConcept[TArg, TResp]):
    def process(
        process_func: (
            Callable[[TArg, Any], Coroutine[Any, Any, TResp]]
            | Callable[[TArg], Coroutine[Any, Any, TResp]]
        )
    ):
        wrapper = router.post(api.endpoint, response_model=api.ResponseClass)(
            process_func
        )
        return wrapper

    return process
