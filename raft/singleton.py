from typing import TYPE_CHECKING

# Ref: https://stackoverflow.com/questions/39740632/python-type-hinting-without-cyclic-imports
if TYPE_CHECKING:
    from .configs import PlantConfig, ServerConfig
    from .network import NetworkInterface


class Singleton:
    def init(
        self, plant: "PlantConfig", server: "ServerConfig", network: "NetworkInterface"
    ) -> None:
        self._plant = plant
        self._server = server
        self._network = network

    @property
    def plant(self):
        assert self._plant is not None
        return self._plant

    @property
    def server(self):
        assert self._server is not None
        return self._server

    @property
    def network(self):
        assert self._network is not None
        return self._network


singleton = Singleton()
