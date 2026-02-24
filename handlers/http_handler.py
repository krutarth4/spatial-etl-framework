import json
from dataclasses import dataclass
from pathlib import Path
from typing import Optional, Union, TypedDict, Any

import requests
import logging

from handlers.file_handler import FileHandler
from log_manager.logger_manager import LoggerManager
from main_core.safe_class import safe_class



class HttpHandler:
    _BASE_DIR = "../"

    def __init__(self, config=None):
        self.logger = LoggerManager(type(self).__name__)
        self.config = config

    def convert_metadata_in_file_format(self, response) -> dict:
        return {
            "etag": response.get("ETag") or None,
            "last_modified": response.get("Last-Modified") or None,
            "content_length": response.get("Content-Length") or None,
            "content_type": response.get("Content-Type") or None,
            "headers": dict(response),
        }

    def call_remote_metadata(self, uri: str, params: dict| None = None, headers: dict | None = None) -> dict:
        """
        Fetch only response headers using HTTP HEAD.
        Used to check if remote data changed.
        """

        try:
            self.logger.info(f"[Metadata Check] HEAD {uri}")
            response = requests.head(uri, headers=headers, params=params, timeout=10)
            response.raise_for_status()
            if response.is_redirect:
                raise requests.exceptions.HTTPError(response.reason)
            return self.convert_metadata_in_file_format(response.headers)

        except requests.exceptions.RequestException as e:
            self.logger.warning(f"Metadata check failed: {e} -> fallback mechanism check started..")
            return self.get_metadata_from_call(uri, params, headers)

    def call(self, uri: str, destination_path: Optional[Union[str, Path]] = "../", stream: bool = False,
             chunk_size: int = 8192, headers: Optional[dict] = None, params: Optional[Union[dict, list]] = None,
             timeout: tuple[int,int] = (30,300), file_extension: str = "json") -> str:
        uri = uri.strip()
        destination_path = Path(destination_path.strip())

        self.logger.info(f"Requesting URL: {uri}")
        if params:
            self.logger.info(f"Query params: {params}")
        if headers:
            self.logger.info(f"Query headers: {headers}")

        try:
            with (requests.get(
                    uri,
                    params=params,
                    headers=headers,
                    stream=stream,
                    timeout=timeout
            ) as response):
                response.raise_for_status()
                headers = response.headers

                # Always read content ONCE
                if stream:
                    content_chunks = []
                    for chunk in response.iter_content(chunk_size=chunk_size):
                        if chunk:
                            content_chunks.append(chunk)
                    content = b"".join(content_chunks)
                else:
                    content = response.content

                if not destination_path:
                    raise ValueError("destination_path must be provided when save=True")

                data_path, meta_path = self.save_to_file(content,headers, destination_path, file_extension)
                content_type = response.headers.get("Content-Type", "")
                result = {
                    "metadata": response.headers,
                    "content": {},
                    "path": data_path,
                }

                self.logger.info(f"Saving metadata to {result['path']}")
                return result["path"]


        except requests.exceptions.RequestException as e:
            self.logger.error(f"HTTP request failed: {e}")
            raise

    def get_metadata_from_call(self, uri: str, params, headers: dict | None = None, timeout:tuple=(30,120)):
        try:
            with (requests.get(
                    uri,
                    params=params,
                    headers=headers,
                    timeout=timeout,
                    stream=True
            ) as response):
                response.raise_for_status()
                self.logger.info(f"Fallback Metadata check successful: {uri}")
                return self.convert_metadata_in_file_format(response.headers)
        except requests.exceptions.RequestException as e:
            self.logger.error(f"HTTP metadata and fallback mechanism check failed request failed : {e} ")
            self.logger.warning(f"Skipping check for metadata ....")
            return {}

    def save_to_file(self, response,headers, destination_path : Path, extension: str):
        file_handler = FileHandler(destination_path.parent)
        name = destination_path.name
        metadata = self.convert_metadata_in_file_format(headers)
        data_path, meta_path =  file_handler.save_pair(name, response, metadata=metadata, data_extension=extension,
                               metadata_extension="json")
        return data_path, meta_path


if __name__ == "__main__":
    handler = HttpHandler()
    #  save the response in files with data and metadata

    path  = handler.call("https://dummyjson.com/products", destination_path="tmp/result")
    print(path)

    #      check metadata calls
    #  response as result in specific format of dict
    res = handler.call_remote_metadata("https://dummyjson.com/products")
    print(res)
