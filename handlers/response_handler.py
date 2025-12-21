from log_manager.logger_manager import LoggerManager


class ResponseHandler:
    def __init__(self):
        self.logger = LoggerManager(type(self).__name__)

