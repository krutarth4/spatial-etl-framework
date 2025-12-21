import re
import textwrap
from datetime import datetime
from zoneinfo import ZoneInfo

import yaml

from log_manager.logger_manager import LoggerManager


class YamlReader:
    PYTHON_BLOCK_PATTERN = r"\$\{\{(.*?)\}\}"
    safe_globals = {
        "datetime": datetime,
        "ZoneInfo": ZoneInfo
    }

    # matcher ${{.....}}
    def __init__(self, filepath):
        self.logger = LoggerManager(type(self).__name__).get_logger()
        self.filepath = filepath

    @classmethod
    def get_yaml_content(cls, filepath:str):
        if not cls.is_yaml_file(filepath):
            raise Exception("Invalid yaml file")
        return cls(filepath).read()

    def read(self):
        with open(self.filepath, 'r') as stream:
            raw_text = stream.read()
            try:
                processed_text = self._preprocess_python_blocks(raw_text)
                data= yaml.safe_load(processed_text)
            except yaml.YAMLError as exc:
                self.logger.error("YAML error parsing", exc)
                raise exc
            return self._strip_newlines_recursive(data)

    def _evaluate_python_block(self, code: str):
        code =textwrap.dedent(code).strip()
        lines = code.splitlines()

        # last line = return value expression
        final_expr = lines[-1]

        # remaining lines = exec() part (imports, helpers)
        exec_part = "\n".join(lines[:-1])

        # safe environment (allowed modules only)
        safe_globals = {
            "datetime": datetime,
            "ZoneInfo": ZoneInfo
        }

        local_env = {}

        if exec_part:
            exec(exec_part, safe_globals, local_env)
        try:
            result = eval(final_expr, safe_globals, local_env)
        except Exception as e:
            raise RuntimeError(
                f"Failed evaluating expression: {final_expr}\n"
                f"Locals: {local_env}"
            ) from e
        print(f"result eval is {result}")
        return result

#TODO:  also write for schema validation once correctly general schema available


    def write(self, data:dict):
        with open(self.filepath, 'w') as stream:
            yaml.dump(data, stream, sort_keys=False)

    @staticmethod
    def is_yaml_file(filepath: str) -> bool:
        return filepath.lower().endswith((".yaml", ".yml"))
    #
    # def validateYaml(self):
    #     return True
    def _preprocess_python_blocks(self, raw_text):
        def repl(match):
            code_block = match.group(1)
            value = self._evaluate_python_block(code_block)
            print(str(value))
            if value is None:
                return ""
            return value

        return re.sub(self.PYTHON_BLOCK_PATTERN, repl, raw_text, flags=re.DOTALL)

    def _strip_newlines_recursive(self, obj):
        if isinstance(obj, str):
            return obj.replace("\n", "").strip()
        elif isinstance(obj, dict):
            return {k: self._strip_newlines_recursive(v) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._strip_newlines_recursive(v) for v in obj]
        else:
            return obj

if __name__ == "__test__":
    reader = YamlReader("../data_source_configs/incorrect_yaml_format.yaml") # -> incorrect check for yaml
    yaml_reader=reader.read()
    print(yaml_reader)