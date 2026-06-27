"""
Load a single mapper instance from config.yaml by its class_name, ready for benchmarking.

Mirrors the resolution logic in DataSourceMapper.run_data_source_mapper() so the bench
harness exercises the same code path as production — minus the scheduler.
"""
import importlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from dacite import Config, from_dict

from utils.data_source_config_dto import DataSourceDTO
from database.db_instance import DbInstance
from main_core.core_config import CoreConfig
from metadata.data_source_metadata_service import DataSourceMetadataService


def load_mapper(class_name: str):
    """
    Instantiate the mapper identified by class_name (e.g. "weatherMapper", "treeMapper").

    Returns:
        (instance, dto, mapper_class, db_instance, base_conf, metadata_service)

    The instance has scheduler=None so execute() routes straight to run().
    """
    conf = CoreConfig().get_config()

    raw_dto = _find_datasource_dto(conf, class_name)
    if raw_dto is None:
        available = [
            str(ds.get("class_name", ""))
            for ds in conf.get("datasources", [])
            if ds.get("class_name")
        ]
        raise ValueError(
            f"No datasource with class_name={class_name!r} in config.yaml. "
            f"Available: {available}"
        )

    dto = from_dict(DataSourceDTO, raw_dto, config=Config(cast=[dict]))

    db_conf = conf["database"]
    base_conf = conf.get("base", {})
    graph_conf = conf.get("graph", {})

    db = DbInstance(db_conf, base_conf, graph_conf)

    metadata_conf = conf.get("metadata-datasource")
    metadata_service = DataSourceMetadataService(db, metadata_conf)

    mapper_class = _import_mapper_class(class_name)
    instance = mapper_class(dto, db, None, base_conf, metadata_service)

    return instance, dto, mapper_class, db, base_conf, metadata_service


def _find_datasource_dto(conf: dict, class_name: str) -> dict | None:
    for entry in conf.get("datasources", []):
        if not isinstance(entry, dict):
            continue
        if str(entry.get("class_name") or "").strip() == class_name:
            return entry
    return None


def _import_mapper_class(class_name: str):
    module_path = f"data_mappers.{class_name}"
    module = importlib.import_module(module_path)
    ctor_name = class_name[0].upper() + class_name[1:]
    return getattr(module, ctor_name)
