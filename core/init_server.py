import logging
from dataclasses import dataclass

import uvicorn

from log_manager.logger_manager import LoggerManager


@dataclass
class ServerConfDTO:
    app_type:str
    enable:bool
    name: str
    description: str
    host: str
    port: int
    reload: bool

class InitServer:

    def __init__(self, conf):
        self.logger = LoggerManager(type(self).__name__)
        self.conf = ServerConfDTO(**conf)
        if not self.conf.enable:
            self.logger.warning("Fast API server enable set to False. Continue...")
            return
        self.initialize_uvicorn_server()

    def initialize_uvicorn_server(self):

        # self.logger.info("Fast Api server up and running ")
        uvicorn.run(self.conf.app_type, host=self.conf.host, port=self.conf.port, reload=self.conf.reload)
