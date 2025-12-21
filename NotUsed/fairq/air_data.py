import gzip
import json
import ijson
import requests
from geoalchemy2 import WKTElement
from pyproj import Transformer
from sqlalchemy.dialects.postgresql import insert

from NotUsed.db_inst import DBInst
from main_core.core_config import CoreConfig


class AirData:
    # TODO: check if the iso_time is same for all the files while downloading as it updates two times a day as is written in jvm_router script
    # from all this url get all tge gz files
    _base = "https://werkzeug.dcaiti.tu-berlin.de/fairqberlin/"
    url = f"{_base}inwt_fairq_cache_simulation_time.json"
    paths = []

    skips = [0,100000,200000,300000]

    def __init__(self, db:DBInst):
        self.get_links()
        self.db = db
        self.engine = db.engine
        self.db.update_metadata()
        self.metadata = db.get_metadata()
        self.table = self.metadata.tables.get("pgrouting.air_pollution_grid")
        print(f"table is {self.table}")
        self.transformer = Transformer.from_crs(25833, 4326, always_xy=True)
        self.initialize_db()

    def get_links(self):
        # res = requests.get(self.url)
        # data =  res.json()
        for skip in self.skips:
            # TODO: disabled
            # self.download_gz_file(f"https://werkzeug.dcaiti.tu-berlin.de/fairqberlin/inwt_fairq_cache_skip_{skip}_limit_100000.json.gz",f"./airw_{skip}.json.gz")
            self.paths.append(f"./airw_{skip}.json.gz")

    @staticmethod
    def download_gz_file(url, destination_path):
        """
        Downloads a .json.gz (or any binary) file from the given URL and saves it locally.
        """
        try:
            with requests.get(url, stream=True) as r:
                r.raise_for_status()  # raise error for 4xx/5xx responses
                with open(destination_path, "wb") as f:
                    for chunk in r.iter_content(chunk_size=8192):
                        if chunk:  # filter out keep-alive chunks
                            f.write(chunk)
            print(f"✅ Download complete: {destination_path}")
        except requests.exceptions.RequestException as e:
            print(f"❌ Download failed: {e}")

    def read_gz_json(self,file_path:str):
        print(f"reading file {file_path}")
        with gzip.open(file_path, "rt", encoding="utf-8") as f:
            data = json.load(f)
        return data

    def get_paths(self):
        return self.paths

    def initialize_db(self):

        for path in self.paths:
            print(f"file - {path}")
            #TODO: need for parallel processing as with one db connection takes a lot of time
            self.load_and_store_gz_json(path)


    def load_and_store_gz_json(self, gz_path):
        print(f"📖 Reading and inserting data from {gz_path}")
        count = 0
        with gzip.open(gz_path, "rt", encoding="utf-8") as f, self.engine.begin() as conn:
            # Stream each feature
            for feature in ijson.items(f, "features.item"):
                try:
                    props = feature["properties"]
                    geom = feature["geometry"]
                    x, y = geom["coordinates"]
                    lon, lat = self.transformer.transform(x, y)

                    point = WKTElement(f"POINT({x} {y})", srid=25833)
                    row_to_insert = []
                    row = {
                        "id": props["id"],
                        "forecast_time": props["date_time_forecast_iso8601"],
                        "forecast_range": props["forecast_range_iso8601"],
                        "no2": props.get("no2"),
                        "pm10": props.get("pm10"),
                        "pm25": props.get("pm2.5"),
                        "x_utm": x,
                        "y_utm": y,
                        "lon": lon,
                        "lat": lat,
                        "geom": point,
                    }
                    row_to_insert.append(row)

                    conn.execute(insert(self.table).values(row_to_insert))
                    count += 1
                    if count % 1000 == 0:
                        print(f"Inserted {count} records...")
                except Exception as e:
                    print(f"⚠️ Skipped one feature due to error: {e}")

        print(f"✅ Finished inserting {count} records from {gz_path}")

if __name__ == '__main__':
    core = CoreConfig().get_value("db_bbox")
    db = DBInst(core, "pgrouting")
    db.create_air_pollution_table()

    m = db.get_metadata().tables
    print(m)
    airData = AirData(db)


    # paths = airData.get_paths()
    # data =airData.read_gz_json(paths)
    # print(data["features"][:1][0]["properties"])

