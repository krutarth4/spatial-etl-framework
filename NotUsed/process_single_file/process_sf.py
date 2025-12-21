from NotUsed.mappers.extractMapper import ExtractMapper
from NotUsed.mappers.loadMapper import LoadMapper
from NotUsed.mappers.transformMapper import TransformMapper
from readers.yaml_reader import YamlReader


class ProcessSf(YamlReader):
    """This will be used to process a single file.
    For each single process we can schedule one process thread if wanted all of it will be done here

    There might be a need for a better centralized monitor service which will take care of each process sf


    """

    def __init__(self, filename):
        super().__init__(filename)
        self.conf = self.read()
        # pprint.pprint(self.conf)
        self.central_mapper()


    def high_level_mapper(self,mapping:str):
        match mapping:
            case "metadata":
                return {"metadata":""}
            case "extract":
                return {"extract": ExtractMapper(self.conf["extract"])}
            case "transform":
                return{"transform":TransformMapper(self.conf["transform"])}
            case "load":
                return{"load": LoadMapper(self.conf["load"])}
            case _:
                raise Exception(f"Unknown mapper: {mapping} found.")

    def central_mapper(self):
        keys = self.conf.keys()
        print(f"central mapper keys: {keys}")
        # a=dict(self.high_level_mapper(n) for n in keys )
        a={}
        for n in keys:
            a.update(self.high_level_mapper(n))
        print(a)







if __name__ == "__main__":

    pro = ProcessSf("../../data_source_configs/osm.yaml")
