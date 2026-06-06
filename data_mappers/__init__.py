import os
import importlib

package_dir = os.path.dirname(__file__)

for file in os.listdir(package_dir):
    if (
        file.endswith(".py")
        and file not in ("__init__.py")
        and not file.startswith("_")
        # Skip deprecated mappers: they pull in heavy, unused deps (scipy/shapely/
        # rasterio) at import time. They are never referenced from config; if one
        # ever is, _run_one_datasource imports it directly by name.
        and not file.endswith("DeprecatedMapper.py")
    ):
        module_name = file[:-3]
        importlib.import_module(f"{__name__}.{module_name}")