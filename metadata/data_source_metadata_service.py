import hashlib
import json
from dataclasses import asdict, is_dataclass
from datetime import date, datetime
from enum import Enum
from pathlib import Path

from dacite import from_dict

from data_config_dtos.data_source_config_dto import MetadataConfDTO
from database.db_instancce import DbInstance
from log_manager.logger_manager import LoggerManager
from metadata.data_source_metadata_repository import DataSourceMetadataRepository


class DataSourceMetadataService:
    def __init__(self, db: DbInstance, metadata_conf):
        self.metadata_conf = None
        self.metadata_repository = None
        self.logger = LoggerManager(type(self).__name__)
        if db is None or metadata_conf is None:
            return
        self.metadata_conf = from_dict(MetadataConfDTO, metadata_conf)
        self.metadata_repository = DataSourceMetadataRepository(db, self.metadata_conf.table_schema)

    def create_table(self):
        if self.metadata_repository is None:
            self.logger.warning("Metadata repository not initialized. Skipping metadata table creation")
            return
        if self.metadata_exist():
            self.logger.info("Metadata table already exists")
            return
        self.metadata_repository.create_metadata_table()

    def metadata_exist(self) -> bool:
        if self.metadata_repository is None:
            return False
        return self.metadata_repository.is_metadata_table_present()

    def update(self, key: str, value: dict):
        if self.metadata_repository is None:
            return None
        self._ensure_table_ready()
        return self.metadata_repository.update_metadata(key, **(value or {}))

    def upsert(self, key: str, value: dict):
        if self.metadata_repository is None:
            return None
        self._ensure_table_ready()
        return self.metadata_repository.upsert_metadata(key, value or {})

    def update_run_status(self, source_key: str, status: str, message: str | None = None, success: bool = False):
        if self.metadata_repository is None:
            return None
        self._ensure_table_ready()
        return self.metadata_repository.update_run_status(source_key, status, message, success)

    def register_data_source(self, data_source_conf):
        if self.metadata_repository is None or data_source_conf is None:
            return None

        source_key = getattr(data_source_conf, "name", None)
        if not source_key:
            return None

        config_snapshot = self._to_jsonable(data_source_conf)
        config_hash = self._hash_config(config_snapshot)
        source_conf = getattr(data_source_conf, "source", None)

        payload = {
            "source_name": source_key,
            "description": getattr(data_source_conf, "description", None),
            "source_type": getattr(data_source_conf, "data_type", None)
                           or getattr(source_conf, "fetch", None)
                           or "unknown",
            "file_path": self._extract_source_paths(source_conf),
            "is_active": bool(getattr(data_source_conf, "enable", True)),
            "config_hash": config_hash,
            "config_snapshot": config_snapshot,
        }
        return self.upsert(source_key, payload)

    def mark_run_started(self, source_key: str, message: str | None = None):
        if not source_key:
            return None
        return self.update_run_status(source_key, "running", message or "Run started", success=False)

    def mark_run_finished(self, source_key: str, success: bool, message: str | None = None):
        if not source_key:
            return None
        status = "success" if success else "failed"
        self.update_run_status(source_key, status, message, success=success)
        if success:
            return self.update(source_key, {"last_ingested_at": datetime.utcnow()})
        return None

    def update_runtime_file_paths(self, source_key: str, paths) -> None:
        if not source_key or paths is None:
            return None
        normalized = self._normalize_runtime_paths(paths)
        return self.update(source_key, {"file_path": normalized})

    def append_runtime_file_paths(self, source_key: str, paths) -> None:
        if not source_key or paths is None or self.metadata_repository is None:
            return None
        new_paths = self._normalize_runtime_paths(paths)
        if not new_paths:
            return None

        self._ensure_table_ready()
        existing = self.metadata_repository.get_metadata(source_key)
        existing_paths = []
        if existing is not None and getattr(existing, "file_path", None):
            existing_paths = [self._trim_path_for_metadata(p) for p in (existing.file_path or []) if p]

        merged = list(existing_paths)
        for path in new_paths:
            if path not in merged:
                merged.append(path)

        return self.update(source_key, {"file_path": merged})

    def _hash_config(self, payload: dict | list | None) -> str | None:
        if payload is None:
            return None
        try:
            serialized = json.dumps(payload, sort_keys=True, default=str)
            return hashlib.sha256(serialized.encode("utf-8")).hexdigest()
        except Exception as e:
            self.logger.error(f"Unable to hash datasource config: {e}")
            return None

    def _ensure_table_ready(self):
        try:
            if not self.metadata_exist():
                self.create_table()
        except Exception as e:
            self.logger.error(f"Failed ensuring metadata table exists: {e}")

    def _normalize_runtime_paths(self, paths) -> list[str]:
        if isinstance(paths, (str, Path)):
            return [self._trim_path_for_metadata(paths)]

        result: list[str] = []
        if isinstance(paths, (list, tuple, set)):
            for path in paths:
                if path is None:
                    continue
                path_s = self._trim_path_for_metadata(path)
                if path_s not in result:
                    result.append(path_s)
        return result

    def _extract_source_paths(self, source_conf) -> list[str] | None:
        if source_conf is None:
            return None

        paths: list[str] = []

        file_path = getattr(source_conf, "file_path", None)
        destination = getattr(source_conf, "destination", None)
        if file_path:
            paths.append(self._trim_path_for_metadata(file_path))
        destination_path = self._trim_path_for_metadata(destination) if destination else None
        if destination_path and destination_path not in paths:
            paths.append(destination_path)

        multi_fetch = getattr(source_conf, "multi_fetch", None)
        urls = getattr(multi_fetch, "urls", None) if multi_fetch is not None else None
        if isinstance(urls, list):
            for item in urls:
                if item is None:
                    continue
                item_s = self._trim_path_for_metadata(item)
                if item_s not in paths:
                    paths.append(item_s)
        else:
            multi_input = getattr(urls, "input", None) if urls is not None else None
            if multi_input:
                multi_input_s = self._trim_path_for_metadata(multi_input)
                if multi_input_s not in paths:
                    paths.append(multi_input_s)

        return paths or None

    def _trim_path_for_metadata(self, path) -> str:
        path_s = str(path)
        normalized = path_s.replace("\\", "/")
        tmp_marker = "/tmp/"
        idx = normalized.find(tmp_marker)
        if idx >= 0:
            return normalized[idx + 1:]
        if normalized.startswith("tmp/"):
            return normalized
        if normalized.startswith("./tmp/"):
            return normalized[2:]
        return normalized

    def _to_jsonable(self, value):
        if value is None:
            return None
        if is_dataclass(value):
            return self._to_jsonable(asdict(value))
        if isinstance(value, dict):
            return {str(k): self._to_jsonable(v) for k, v in value.items()}
        if isinstance(value, (list, tuple, set)):
            return [self._to_jsonable(v) for v in value]
        if isinstance(value, Enum):
            return value.value
        if isinstance(value, Path):
            return str(value)
        if isinstance(value, (datetime, date)):
            return value.isoformat()
        if isinstance(value, (str, int, float, bool)):
            return value
        return str(value)
