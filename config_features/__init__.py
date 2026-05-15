"""Pluggable datasource-level configuration features.

Each feature module under `config_features.datasource` owns one slice of a
datasource's YAML config: schema, defaults, validation, and a human-readable
description of what the key is for. The DatasourceFeatureRegistry discovers
them automatically and is called at config-load time.

Adding a new config property = drop a new module that subclasses
DatasourceFeature and registers itself.
"""

from config_features.base import DatasourceFeature, FeatureIssue
from config_features.registry import DatasourceFeatureRegistry

__all__ = ["DatasourceFeature", "FeatureIssue", "DatasourceFeatureRegistry"]
