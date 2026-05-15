from __future__ import annotations

import importlib
import pkgutil
from typing import Any, Type

from config_features.base import DatasourceFeature, FeatureIssue, get_dotted


class DatasourceFeatureRegistry:
    """Discovery + validation entry point for datasource-level features.

    Usage:
        DatasourceFeatureRegistry.load_all()            # discover modules
        errors, warnings = DatasourceFeatureRegistry.validate_all(datasources)
        print(DatasourceFeatureRegistry.describe())     # introspection
    """

    _features: dict[str, Type[DatasourceFeature]] = {}
    _loaded: bool = False

    @classmethod
    def register(cls, feature: Type[DatasourceFeature]) -> Type[DatasourceFeature]:
        if not getattr(feature, "KEY", None):
            raise ValueError(f"{feature.__name__} must declare a non-empty KEY")
        if feature.KEY in cls._features and cls._features[feature.KEY] is not feature:
            raise ValueError(
                f"Duplicate DatasourceFeature key '{feature.KEY}': "
                f"{cls._features[feature.KEY].__name__} vs {feature.__name__}"
            )
        cls._features[feature.KEY] = feature
        return feature

    @classmethod
    def all(cls) -> list[Type[DatasourceFeature]]:
        return list(cls._features.values())

    @classmethod
    def get(cls, key: str) -> Type[DatasourceFeature] | None:
        return cls._features.get(key)

    @classmethod
    def describe(cls) -> list[dict[str, Any]]:
        return [f.describe() for f in cls.all()]

    @classmethod
    def load_all(cls) -> None:
        """Import every module under config_features.datasource so that
        @register decorators run. Idempotent."""
        if cls._loaded:
            return
        from config_features import datasource as pkg  # local to avoid cycles
        for mod in pkgutil.iter_modules(pkg.__path__):
            importlib.import_module(f"{pkg.__name__}.{mod.name}")
        cls._loaded = True

    @classmethod
    def parse_slice(cls, datasource: dict, key: str) -> Any:
        """Parse one feature's slice out of a raw datasource dict."""
        feature = cls.get(key)
        if feature is None:
            raise KeyError(f"No registered feature for key '{key}'")
        return feature.parse(get_dotted(datasource, key))

    @classmethod
    def validate_all(
        cls, datasources: list[dict]
    ) -> tuple[list[FeatureIssue], list[FeatureIssue]]:
        """Run every feature's parser + validator against every datasource.

        Returns (errors, warnings). Parse failures are reported as errors and
        skip that feature's validate() call.
        """
        cls.load_all()
        errors: list[FeatureIssue] = []
        warnings: list[FeatureIssue] = []

        for ds in datasources or []:
            if not isinstance(ds, dict):
                continue
            ds_name = ds.get("name") or "<unnamed>"
            for feature in cls.all():
                raw_slice = get_dotted(ds, feature.KEY)
                try:
                    parsed = feature.parse(raw_slice)
                except Exception as e:  # dacite or custom parser failure
                    errors.append(FeatureIssue(
                        ds_name, feature.KEY, f"parse error: {e}", "error"
                    ))
                    continue
                for issue in feature.validate(parsed, ds_name) or []:
                    (errors if issue.level == "error" else warnings).append(issue)

        return errors, warnings
