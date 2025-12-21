import requests

from NotUsed.db_inst import DBInst
from main_core.core_config import CoreConfig


class Weather(DBInst):

    def __init__(self, conf,schema="public"):
        # check for the db
        super().__init__(conf,schema)


if __name__ == '__main__':
    # create table

    core = CoreConfig().get_value("db_bbox")
    print(core)
    weather = Weather(core, "pgrouting")
    # creating tables
    weather.create_weather_table()
    weather.create_station_table()
    weather.create_weather_observations_table()

    # create station data
    # URL for stations = https://opendata.dwd.de/climate_environment/CDC/observations_germany/climate/hourly/air_temperature/recent/TU_Stundenwerte_Beschreibung_Stationen.txt

    station_ids = ["00399","00403","00400","00410","00420","00424","00427","00430","00433"]
    params = [("dwd_station_id",sid) for sid in station_ids]
    print(params)
    url = "https://api.brightsky.dev/sources"
    res = requests.get(url,params=params)
    data =res.json()

    # store in the db
    weather.insert_brightsky_stations(data["sources"])


    a = weather.get_dwd_station_ids()
    print(a)

    weather.insert_weather_observations()







