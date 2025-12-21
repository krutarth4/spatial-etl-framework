from dataclasses import dataclass
from typing import Any, Optional

from NotUsed.mappers.httpMapper import HttpOutMapperDto
from NotUsed.mappers.querryMapper import QueryMapper
from utils.dataType import parse_format
from utils.dictToClass import dict_to_class
from utils.jsonTransform import parse_type


@dataclass
class OperatorDTO:
    operator: str
    format: str
    when: str
    query: str
    name: str
    data: Optional[Any]


@dataclass
class DataInDTO:
    data: Optional[Any]
    format: str
    expect: str


@dataclass
class OperationMapperDTO:
    name: str
    description: str
    kind: str
    input: DataInDTO
    output: HttpOutMapperDto
    operation: OperatorDTO


class OperationMapper:
    res = {}

    def __init__(self, operation, results):
        self.config = dict_to_class(operation, OperationMapperDTO, results)
        self.results: dict = results
        # validation check
        ans = self.operation_mapper(self.config.operation.operator)

        parse_ans = None
        if self.is_data_type_same(ans) and (self.config.output.convert is None or self.config.output.convert == ""):
            parse_ans = ans.get_result()
        else:
            # convert the data base on the convert method parse given
            parse_ans = parse_type(ans.get_result_key_value(), self.config.output.convert)
        self.res[self.config.output.name] = parse_ans
        # print("from operation mapper", self.res)

    def is_data_type_same(self, result, ):

        if isinstance(type(result), parse_format(self.config.output.format)):
            print(f"convert {type(result)}")
            return True
        return False

    def operation_mapper(self, mapping: str):

        match mapping:
            case "filter":
                return
            case "query":
                return QueryMapper(self.config.operation, self.results)
            case "map":
                return
            case "custom":
                return
            case _:
                raise Exception(f"Unknown mapper: {mapping} found.")

    def get_res(self):
        return self.res
