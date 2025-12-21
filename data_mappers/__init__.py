import os
import importlib

package_dir = os.path.dirname(__file__)

for file in os.listdir(package_dir):
    if (
        file.endswith(".py")
        and file not in ("__init__.py")
        and not file.startswith("_")
    ):
        module_name = file[:-3]
        importlib.import_module(f"{__name__}.{module_name}")