from dataclasses import dataclass
from typing import Optional, Any

from handlers.http_handler import HttpHandler
from utils.dictToClass import dict_to_class

@dataclass
class HttpInMapperDto:
    url: Any
    headers: Optional[dict]
    expect:str
@dataclass
class HttpSaveRawClassDto:
    enabled: bool
    format: str
    path:str
@dataclass
class HttpOutMapperDto:
    name:str
    format: str
    save_raw: Optional[HttpSaveRawClassDto]
    db: Optional[dict]
    convert: Optional[str]

@dataclass
class Operation:
    operator: str        # e.g., "filter", "find", "map"
    format: str          # e.g., "string", "json"
    when: Optional[str]  # condition, e.g., "[query]=Berlin"
    query: Optional[str] # JSONPath or filter expression


@dataclass
class HTTPMapperDTO:
    name: str
    description:str
    kind: str
    format:str
    headers:dict
    save_raw: dict
    input: HttpInMapperDto
    output: HttpOutMapperDto
    operation: Optional[Operation]



class HttpMapper(HttpHandler):
    res={}

    def __init__(self, http_config, results):
        self.conf = dict_to_class(http_config, HTTPMapperDTO, results)
        print(self.conf)
        super().__init__(self.conf)
        self.results= results
        # call http
        # print(http_config)

        self.res[self.conf.output.name] = self.process_http_request()

        # validation for http mapper

    def process_http_request(self):
        res = self.call(self.conf.input.url,  self.conf.output.save_raw.path, self.conf.output.save_raw.enabled, False)
        return res

    def get_res(self):
        return self.res






    def validation(self):
        pass