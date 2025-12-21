from typing import Type, TypeVar, Any

from dacite import from_dict, DaciteError
from dacite.data import Data

from utils.resolveVariable import resolve_variables

T= TypeVar("T")
def dict_to_class(data: Data, data_class: Type[T], result: Any = None)->T:
    try:
        if result is not None:
            resolved = resolve_variables(data, result)
            return from_dict(data_class, resolved)
        else:
            return from_dict(data_class, data)
    except DaciteError as e:
        raise ValueError(f"Could not map data to {data_class.__name__}: {e}") from e