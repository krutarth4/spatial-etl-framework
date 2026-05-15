from __future__ import annotations

from abc import ABC
from dataclasses import dataclass
from typing import Any, ClassVar, Literal, Optional, Type

from dacite import Config, from_dict


@dataclass
class FeatureIssue:
    """A single validation problem found for one (datasource, feature) pair."""
    datasource_name: str
    feature_key: str
    message: str
    level: Literal["error", "warning"] = "error"

    def __str__(self) -> str:
        tag = "ERROR" if self.level == "error" else "WARN "
        return (
            f"[{tag}] datasource '{self.datasource_name}' "
            f"feature '{self.feature_key}': {self.message}"
        )


class DatasourceFeature(ABC):
    """One slice of a datasource's YAML config.

    Subclasses declare:
      KEY          dotted path inside a datasource block, e.g. "source.multi_fetch"
      SCHEMA       dataclass used to parse this slice (None = raw passthrough)
      DESCRIPTION  what this key does and where it is consumed

    Optional overrides:
      default()                       value when key is absent in the YAML
      parse(raw)                      raw dict → SCHEMA instance
      validate(parsed, datasource)    list of FeatureIssue (errors and/or warnings)

    Adding a new config property = subclass this and register with
    @DatasourceFeatureRegistry.register.
    """

    KEY: ClassVar[str]
    SCHEMA: ClassVar[Optional[Type]] = None
    DESCRIPTION: ClassVar[str] = ""

    @classmethod
    def default(cls) -> Any:
        return None

    @classmethod
    def parse(cls, raw: Any) -> Any:
        if raw is None:
            return cls.default()
        if cls.SCHEMA is None:
            return raw
        return from_dict(cls.SCHEMA, data=raw, config=Config(cast=[dict]))

    @classmethod
    def validate(cls, parsed: Any, datasource_name: str) -> list[FeatureIssue]:
        return []

    @classmethod
    def describe(cls) -> dict[str, Any]:
        return {
            "key": cls.KEY,
            "schema": cls.SCHEMA.__name__ if cls.SCHEMA else None,
            "description": cls.DESCRIPTION.strip(),
        }


def get_dotted(data: dict, dotted_key: str) -> Any:
    """Walk a dotted path into a nested dict; return None if any segment is missing."""
    cur: Any = data
    for part in dotted_key.split("."):
        if not isinstance(cur, dict):
            return None
        cur = cur.get(part)
        if cur is None:
            return None
    return cur
