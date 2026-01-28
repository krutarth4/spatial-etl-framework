import os
from typing import Any


class GlobalConstants:
    """
    Dynamic static environment/YAML config.
    Any key becomes an attribute.
    """

    _loaded = False
    _data: dict[str, Any] = {}

    @classmethod
    def load(cls, defaults: dict[str, Any] | None = None) -> None:
        if cls._loaded:
            return

        defaults = defaults or {}

        merged: dict[str, Any] = {}

        # 1️⃣ Start with YAML defaults
        for key, value in defaults.items():
            merged[key] = value

        # 2️⃣ Override with environment variables
        for key in merged.keys():
            if key in os.environ:
                merged[key] = os.environ[key]

        # 3️⃣ Also include env-only keys (not in YAML)
        for key, value in os.environ.items():
            if key not in merged:
                merged[key] = value

        # 4️⃣ Attach dynamically as class attributes
        for key, value in merged.items():
            setattr(cls, key, cls._auto_cast(value))

        cls._data = merged
        cls._loaded = True

    @classmethod
    def _auto_cast(cls, value: Any) -> Any:
        if isinstance(value, str):
            v = value.lower()
            if v == "true":
                return True
            if v == "false":
                return False
            if v.isdigit():
                return int(value)
            try:
                return float(value)
            except ValueError:
                return value
        return value

    @classmethod
    def as_dict(cls) -> dict[str, Any]:
        return dict(cls._data)
