from enum import StrEnum

from NotUsed.mappers.httpMapper import HttpMapper
from NotUsed.mappers.operationMapper import OperationMapper


class KIND(StrEnum):
    HTTP = "http"
    LOCAL = "local"
    HTTPS = "https"
    OPERATION = "operation"

class ExtractMapper:
    results ={}
    def __init__(self,config):
        self.config = config
        # print(self.config)
        self.mapper()

    def mapper(self):
        for i in range(len(self.config)):
            print("mapper")
            keys = self.config[i].keys()
            # print(keys)
            a =[self.extract_mapper_case(m,i) for m in keys]

    def extract_mapper_case(self,mapping:str, i: int):
        match mapping:

            case "source":
                print("calling extract mapper source case ")
                return self.source_mapper(self.config[i]["source"])

            case _:
                raise Exception(f"Unknown extract mapper: {mapping} found.")

    def kind_mapper(self,conf):
        pass

    def source_mapper(self,source):
        mapper_result=None
        print(f"source_mapper{type(KIND.HTTP)}")
        if source["kind"] == KIND.HTTP:
            print("calling kind mapper")
            h= HttpMapper(source, self.results)
            mapper_result = h.get_res()
            # pprint.pprint(f"printing m apper{ h}")

        elif source["kind"] == KIND.OPERATION:
            print("calling kind mapper for operation")
            mapper_result= OperationMapper(source, self.results).get_res()
            # print(mapper_result)

        else:
            raise Exception(f"Unknown source: {source['kind']} or kind param doesn't exist")
        self.results[source["name"]] = mapper_result
        print(self.results.get(source["name"]).get("pbf_url",{}))