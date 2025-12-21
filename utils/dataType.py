from typing import Any, List, get_args, get_origin


def parse_format(fmt: str) -> Any:
    """
    Convert format string like 'str' or 'list[str]' into a Python type.
    """
    fmt = fmt.strip().lower()
    if fmt == "str":
        return str
    elif fmt in ("list[str]", "lis[str]"):  # handle your typo case too
        return List[str]
    elif fmt == "int":
        return int
    elif fmt == "list[int]":
        return List[int]
    else:
        raise ValueError(f"Unknown format: {fmt}")


def init_variable(fmt: str):
    t = parse_format(fmt)

    if t is str:
        return ""  # empty string
    elif t is int:
        return 0  # default int
    elif get_origin(t) is list:
        return []  # empty list
    else:
        return None
