import uvicorn
from core.application import Application
from main_core.core_config import CoreConfig


if __name__ == "__main__":
    conf = CoreConfig().get_value("server")
    if conf.get("enable"):
        uvicorn.run(conf["app_type"], host=conf["host"], port=conf["port"], reload=conf["reload"])
    else:
        Application().run_standalone()
