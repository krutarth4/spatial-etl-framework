import json
import os
from datetime import datetime
from pathlib import Path, PurePath, PosixPath
from typing import Any, Optional, Callable

from log_manager.logger_manager import LoggerManager


class FileHandler:
    """
    Stores data and metadata files with timestamped filenames.
    Example:
        weather_data_2025-11-22T16-22-10.json
        weather_meta_2025-11-22T16-22-10.json
    """
    _META = "meta"
    _DATA = "data"
    _BASE_DIR = "../"
    _STORAGE_ROOT_ENV = "MDP_STORAGE_ROOT"
    data_folders = ("tmp", "data")

    def __init__(self, base_dir: str | Path = _BASE_DIR):
        self.logger = LoggerManager(type(self).__name__).get_logger()
        self.base_dir = self._resolve_base_dir(base_dir)
        self.base_dir.mkdir(parents=True, exist_ok=True)
        self.logger.info(f"Base directory: {self.base_dir.resolve()}")

    def _resolve_base_dir(self, base_dir: str | Path) -> Path:
        repo_root = Path(__file__).resolve().parents[1]
        storage_root = Path(os.getenv(self._STORAGE_ROOT_ENV, str(repo_root))).expanduser()

        raw_path = base_dir if isinstance(base_dir, (Path, PosixPath)) else Path(str(base_dir))

        # Call sites pass both directories and full file paths; normalize to directory.
        looks_like_file = raw_path.suffix != ""
        dir_path = raw_path.parent if looks_like_file else raw_path

        if str(dir_path) in ("", "."):
            dir_path = Path(".")

        if dir_path.is_absolute():
            return dir_path.resolve()

        return (storage_root / dir_path).resolve()

    def _timestamp(self) -> str:
        return datetime.now().strftime("%Y-%m-%dT%H-%M-%S")

    def _file_path(self, prefix: str, name: str, timestamp: str) -> Path:
        extension = ""
        name = name.split(".")
        if prefix == self._DATA:
            if len(name) < 2:
                self.logger.error(
                    f"[FileHandler] Invalid file name: {name}. Please provide file name with the correct extension ")
                return ""
            extension = name[-1]


        elif prefix == self._META:
            extension = "json"

        else:
            self.logger.warning(f"Invalid file name: {name} no extension parsed")
            extension = "txt"

        return self.base_dir / f"{prefix}_{'.'.join(name[:-1])}_{timestamp}.{extension}"

    def _normalized_file_path(self, name: str, ext: str) -> Path:
        return self.base_dir / f"{name}.{ext}"

    # ---------------------------------------------------
    # Save Data & Metadata
    # ---------------------------------------------------

    def save_pair(
            self,
            name: PurePath,
            data: Any,
            metadata: Any,
            data_extension: str = "json",
            metadata_extension: str = "json"
    ) -> tuple[Path, Path]:
        """
        Save both data and metadata files at the same timestamp.
        """
        ts = self._timestamp()

        data_path = self._file_path(self._DATA, name, ts)
        meta_path = self._file_path(self._META, name, ts)

        self._write_file(data_path, data)
        self._write_file(meta_path, metadata)

        # Enforce retention: keep only last 2
        self._enforce_retention(f"{self._DATA}_{name}_*.{data_extension}")
        self._enforce_retention(f"{self._META}_{name}_*.{metadata_extension}")

        return data_path, meta_path

    def get_file_name_and_extension(self):
        pass

    def save_data(
            self,
            path: str,
            data: Any,
            normalize: bool = False
    ):
        #TODO change the path to Path Instance
        path = path.split("/")[-1]
        name  = path.split(".")
        path = ".".join(name[:-1])
        extension = name[-1]

        data_path = ""
        if normalize:
            data_path = self._normalized_file_path(path, extension)

            # Explicitly delete existing normalized file
            if data_path.exists():
                data_path.unlink()
                self.logger.info(f"[FileHandler] Replaced normalized file: {data_path.name}")
        else:
            data_path = self._file_path(self._DATA, path, self._timestamp())
        self._write_file(data_path, data)

    def _write_file(self, path: Path, content: Any):
        suffix = path.suffix.lstrip(".")
        if suffix == "json":
            with open(path, "w", encoding="utf-8") as f:
                if isinstance(content, (dict, list)):
                    json.dump(content, f, indent=2)
                elif isinstance(content, (str, bytes)):
                    if isinstance(content, bytes):
                        content = content.decode("utf-8")
                    f.write(content)
                else:
                    self.logger.error(f"Unsupported JSON content type: {type(content)}")
                    raise TypeError(f"Unsupported JSON content type: {type(content)}")
        else:

            mode = "wb" if isinstance(content, bytes) else "w"
            with open(path, mode) as f:
                if isinstance(content, bytes):
                    f.write(content)
                else:
                    f.write(str(content).encode("utf-8"))

    # ---------------------------------------------------
    # Reading and fetching latest files
    # ---------------------------------------------------

    def _get_latest_file(self, prefix: str, name: str, extension: str):
        pattern = f"{prefix}_{name}_*.{extension}"
        files = list(self.base_dir.glob(pattern))

        # self.logger.info(f"Searching in: {self.base_dir.resolve()}")
        # print("Pattern:", pattern)
        # print("Files present:", list(self.base_dir.iterdir()))
        if not files:
            return None
        files.sort(reverse=True)
        return files[0]

    def get_latest_data_file(self, name: str, extension: str = "json") -> Optional[Path]:
        return self._get_latest_file(self._DATA, name, extension)

    def get_latest_meta_file(self, name: str, extension: str = "json") -> Optional[Path]:
        return self._get_latest_file(self._META, name, extension)

    def read_data(self, name: str, extension: str = "json") -> Optional[Any]:
        file = self.get_latest_data_file(name, extension)
        return self._read(file, extension) if file else None

    def read_metadata(self, name: str, extension: str = "json") -> Optional[Any]:
        file = self.get_latest_meta_file(name, extension)
        return self._read(file, extension) if file else None

    def _read(self, path: Path, extension: str):
        extension = extension.lower().lstrip(".")

        try:
            if extension == "json":
                # JSON is text → read as text, not binary
                with open(path, "r", encoding="utf-8") as f:
                    content = json.load(f)
                    return content
            # Everything else → binary-safe
            with open(path, "r") as f:
                return f.read()

        except Exception as e:
            self.logger.error(f"Failed to read file: {e}")

        # with open(path, "rb") as f:
        #     content = f.read()
        #
        # if extension == "json":
        #     return json.loads(content.decode("utf-8"))
        # return content

    # ---------------------------------------------------
    # Fetch Latest Pair Together
    # ---------------------------------------------------

    def get_latest_pair(self, name: str, extension: str = "json") -> tuple[Optional[Path], Optional[Path]]:
        """
        Get the latest data + metadata that belong to the same timestamp.
        """
        data_file = self.get_latest_data_file(name, extension)
        if not data_file:
            return None, None

        # Extract timestamp from data file
        # filename = data_weather_2025-11-22T16-22-10.json
        ts = data_file.stem.split("_")[-1]

        meta_file = self.base_dir / f"{self._META}_{name}_{ts}.{extension}"

        if not meta_file.exists():
            return data_file, None

        return data_file, meta_file

    def read_latest_pair(self, name: str, extension: str = "json") -> tuple[Any, Any]:
        data_path, meta_path = self.get_latest_pair(name, extension)
        return (
            self._read(data_path, extension) if data_path else None,
            self._read(meta_path, extension) if meta_path else None
        )
    # TODO: change the implementation based on the new naming mechanism
    def _enforce_retention(self, pattern: str, keep: int = 2):
        files = sorted(self.base_dir.glob(pattern))
        excess = len(files) - keep

        if excess > 0:
            for f in files[:excess]:
                try:
                    f.unlink()
                    self.logger.info(f" Deleted old file: {f.name}")
                except Exception as e:
                    self.logger.warning(f"Failed to delete {f.name}: {e}")

    def read_local_file(self, file_name_with_extension: str, read_handler: Callable[[str], Any] = None):
        try:
            content = NotImplemented
            path = self.get_local_file(file_name_with_extension)
            extension = file_name_with_extension.split(".")[-1]
            if read_handler is not None:
                content = read_handler(path) if path else None
            if content is NotImplemented or read_handler is None:
                content = self._read(path, extension) if path else None
        except Exception as e:
            self.logger.error(f"Failed to read local file: {e}")
            return None
        return content

    def get_local_file(self, file_name_with_extension) -> Optional[Path]:
        file_name = file_name_with_extension.split(".")

        pattern = f"*{'.'.join(file_name[:-1])}*{file_name[-1]}"

        files = list(self.base_dir.glob(pattern))
        data_files = [f for f in files if not f.name.startswith(self._META)]
        # self.logger.info(f"Searching in: {self.base_dir.resolve()}")
        # self.logger.info(f"Pattern repr: {repr(pattern)}")
        # self.logger.info(f"Files present: {list(self.base_dir.iterdir())}")
        if not data_files:
            return None
        data_files.sort(reverse=True)
        return data_files[0]
