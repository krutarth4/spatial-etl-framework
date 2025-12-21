from starlette.middleware.cors import CORSMiddleware

from NotUsed.db_inst import DBInst
from main_core.core_config import CoreConfig
from fastapi import FastAPI
from dataclasses import dataclass

app = FastAPI()

app.add_middleware(CORSMiddleware,
                allow_origins = ["http://localhost:4200"]
            )


@app.get("/")
async def root():
    return {"message": "Hello World"}


@app.get("/hello/{name}")
async def say_hello(name: str):
    return {"message": f"Hello {name}"}

@dataclass
class Coordinate:
    lat: int
    lng: int


# def get_db():
#     conf = CoreConfig()
#     db2 = DbConf(conf.get_value("db")).session()
#     try:
#         yield db2
#     finally:
#         db2.close()
#
# db = DBInstance(CoreConfig().get_value("db"))
# @app.get("/route/")
# async def get_route(startLon: float, startLat: float,endLon: float,endLat: float):
#     print(f"start {startLon} , {startLat}")
#     print(f"start {endLon} , {endLat}")
#
#     start_time = time.perf_counter()
#
#
#     result =  db.get_route(startLon, startLat,endLon,endLat)
#     end_time = time.perf_counter()
#     duration = end_time -start_time
#     print(f"route generation took {duration}")
#     return result

#TODO: implement directly in the main resource manager
# @app.on_event("startup")
# def start_scheduler():
#     scheduler = SchedulerManager()
#     scheduler.add_job(update_config_job, "config_updater", minutes=30)
#     print("[FastAPI] Scheduler started.")
#
# @app.on_event("shutdown")
# def stop_scheduler():
#     SchedulerManager().shutdown()
if __name__ == "__main__":
    # check the core config and read the attributes
    config = CoreConfig()

    db= DBInst(config.get_value("db"))
    # a = db.get_nearest_node(13.3231919715,52.5109321435)
    a = db.get_route(13.3231919715,52.5109321435, 13.324586511, 52.51260044 )
    print(f"{a} id")




# read all the files from the data source directory

    # all_files = DirectoryReader(config.get_source_directory()).get_all_files()
    # osm_conf=None
    # for file_path in all_files:
    #     print(f"Reading config file {file_path}")
    #     if file_path.endswith("osm.yaml"):
    #         print(f"Reading with yaml reader {file_path}")
    #         # TODO: stop the execution
    #         # p=ProcessSf(file_path)
    #
    #     else:
    #         print(f"Skipping file {file_path}")

    # get the last state with the help of db

    # check all the data sources configuration -> update if it needs to be changed


        # check last state of the data and update it according to the new configuration
            # Extraction layer can start simultaneously for each data_source
            # do the mapping for the given config