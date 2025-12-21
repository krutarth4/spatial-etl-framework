
import json


class JsonReader:

    def __init__(self, filename):
        self.filename = filename
        self.check_if_json() # Always validate the file

    def read(self):
        with open(self.filename) as json_file:
            try:
                return json.load(json_file)
            except json.decoder.JSONDecodeError as err:
                raise FileNotFoundError(err)
            except Exception as err:
                raise err

    def check_if_json(self):
        if not self.filename.lower().endswith(".json"):
            raise ValueError(f"File must be a JSON file, got: {self.filename}")
        return True

    def write(self, data):
        with open(self.filename, "w") as json_file:
            json.dump(data, json_file, indent=4)


if __name__ == "__test__":
    reader = JsonReader("../data/json/data.json")
    reader.read()