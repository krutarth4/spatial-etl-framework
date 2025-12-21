import re
from typing import Any

VAR_PATTERN = re.compile(r"^\$\{(.+)}$")
def resolve_variables(obj: Any, results: dict) -> Any:
    """Recursively resolve ${...} references inside dicts/lists/strings."""
    if isinstance(obj, dict):
        return {k: resolve_variables(v, results) for k, v in obj.items()}
    elif isinstance(obj, list):
        return [resolve_variables(v, results) for v in obj]
    elif isinstance(obj, str):
        match = VAR_PATTERN.match(obj)
        if match:
            path = [p.strip() for p in match.group(1).split(".")]
            # print(f"printing path {path}")
            return get_nested(results, path)
        return obj
    else:
        return obj

def get_nested(d: dict, keys: list[str], default=None):
    """Walk nested dict by keys."""
    # print(f"getting nested dict {d.keys()}")
    for k in keys:
        # print(f"getting nested key {k}")
        if isinstance(d, dict):
            d = d.get(k)
        else:
            print("not found")
            return default
    return d if d is not None else default
