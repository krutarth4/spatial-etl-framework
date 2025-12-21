from NotUsed.mappers.cliMapper import CliMapper
from readers.yaml_reader import YamlReader


class LoadMapper:
    results = {}
    def __init__(self, load):
        self.load = load
        self.mapper()



    def mapper(self):
        for i in range(len(self.load)):
            print("loader config running on keys ")
            keys = self.load[i].keys()
            print(keys)
            a =[self.load_mapper_case(m,i) for m in keys]

    def load_mapper_case(self, mapping:str, i:int):
        match mapping:

            case "tool":
                print("calling extract mapper source case ")
                return self.tool_mapper(self.load[i]["tool"])

            case _:
                raise Exception(f"Unknown extract mapper: {mapping} found.")

    def tool_mapper(self, tool_conf:dict):
        mapper_result=None
        print(f"tool_mapper{tool_conf}")
        if tool_conf.get("kind") == "cli":
            return CliMapper(tool_conf)
            print("calling load mapper tool cli ")




if __name__ == "__main__":
    load = YamlReader.get_yaml_content("../../data_source_configs/osm.yaml")["load"]

    l = LoadMapper(load)
