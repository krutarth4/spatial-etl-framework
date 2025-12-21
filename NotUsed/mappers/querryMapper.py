from enum import StrEnum
from jsonpath_ng.ext import parse


class OPERATOR(StrEnum):
    QUERY = "query"


class QueryMapper:
    res = {}

    def __init__(self, operation, results):
        self.operation = operation
        self.results = results
        self.res[self.operation.name] = self.generic_process(self.operation.operator)

    def generic_process(self, mapping):
        match mapping:
            case "metadata":
                return
            case OPERATOR.QUERY:
                return self.process_query()
            case "transform":
                return
            case "load":
                return
            case _:
                raise Exception(f"Unknown mapper: {mapping} found.")

    def process_query(self):
        query = f'{self.operation.query}'
        expr = parse(query)
        data = self.operation.data
        urls = [m.value for m in expr.find(data)]
        return urls

    def get_result_key_value(self):
        return self.res

    def get_result(self):
        return self.res.get(self.operation.name)
