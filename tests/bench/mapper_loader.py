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

from data_config_dtos.data_source_config_dto import DataSourceDTO
from database.db_instancce import DbInstance
from main_core.core_config import CoreConfig
from metadata.data_source_metadata_service import DataSourceMetadataService


def load_mapper(class_name: str):
    """
    Instantiate the mapper identified by class_name (e.g. "weather", "tree").
    Strips a trailing "Mapper" suffix if present, so both "weather" and "weatherMapper"
    are accepted.

    Returns:
        (instance, dto, mapper_class, db_instance, base_conf, metadata_service)

    The instance has scheduler=None so execute() routes straight to run().
    """
    normalised = _strip_mapper_suffix(class_name)

    conf = CoreConfig().get_config()

    raw_dto = _find_datasource_dto(conf, normalised)
    if raw_dto is None:
        available = [
            _strip_mapper_suffix(str(ds.get("class_name", "")))
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

    mapper_class = _import_mapper_class(normalised)
    instance = mapper_class(dto, db, None, base_conf, metadata_service)

    return instance, dto, mapper_class, db, base_conf, metadata_service


def _strip_mapper_suffix(name: str) -> str:
    if name.endswith("Mapper"):
        return name[:-6]
    return name


def _find_datasource_dto(conf: dict, normalised_class_name: str) -> dict | None:
    for entry in conf.get("datasources", []):
        if not isinstance(entry, dict):
            continue
        raw_class = _strip_mapper_suffix(str(entry.get("class_name") or "").strip())
        if raw_class == normalised_class_name:
            return entry
    return None


def _import_mapper_class(normalised: str):
    module_path = f"data_mappers.{normalised}Mapper"
    module = importlib.import_module(module_path)
    ctor_name = normalised[0].upper() + normalised[1:] + "Mapper"
    return getattr(module, ctor_name)
