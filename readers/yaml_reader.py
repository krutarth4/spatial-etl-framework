import re
import textwrap
from datetime import datetime
from zoneinfo import ZoneInfo

import yaml

from log_manager.logger_manager import LoggerManager


class YamlReader:
    PYTHON_BLOCK_PATTERN = r"(?<!#)\$\{\{(.*?)\}\}"
    safe_globals = {
        "datetime": datetime,
        "ZoneInfo": ZoneInfo
    }

    # matcher ${{.....}}
    def __init__(self, filepath):
        self._python_block_results = {}
        self.logger = LoggerManager(type(self).__name__).get_logger()
        self.filepath = filepath

    @classmethod
    def get_yaml_content(cls, filepath: str):
        if not cls.is_yaml_file(filepath):
            raise Exception("Invalid yaml file")
        return cls(filepath).read()

    def read(self):
        with open(self.filepath, 'r') as stream:
            raw_text = stream.read()
            try:
                processed_text = self._preprocess_python_blocks(raw_text)
                data = yaml.safe_load(processed_text)
                # print(data)
                data = self._resolve_python_blocks(data)
            except yaml.YAMLError as exc:
                self.logger.error("YAML error parsing", exc)
                raise exc
            return self._strip_newlines_recursive(data)

    def _evaluate_python_block(self, code: str):
        code = textwrap.dedent(code).strip()
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
        self.logger.info(f"YAML reader Python eval is {result}")

        return result

    # TODO:  also write for schema validation once correctly general schema available

    def write(self, data: dict):
        with open(self.filepath, 'w') as stream:
            yaml.dump(data, stream, sort_keys=False)

    @staticmethod
    def is_yaml_file(filepath: str) -> bool:
        return filepath.lower().endswith((".yaml", ".yml"))

    #

    def _preprocess_python_blocks(self, raw_text):
        def repl(match):
            code_block = match.group(1)
            value = self._evaluate_python_block(code_block)
            self.logger.info(str(value))
            placeholder = f"__PYTHON_BLOCK_{len(self._python_block_results)}__"
            self._python_block_results[placeholder] = value
            return placeholder

        return re.sub(self.PYTHON_BLOCK_PATTERN, repl, raw_text, flags=re.DOTALL)

    def _resolve_python_blocks(self, obj):
        # Case 1: exact placeholder → restore original Python value
        if isinstance(obj, str):
            key = obj.strip()
            if key in self._python_block_results:
                return self._python_block_results[key]

        # Case 2: embedded placeholder inside a string → string interpolation
        if isinstance(obj, str):
            result = obj
            for placeholder, value in self._python_block_results.items():
                # IMPORTANT: skip exact-placeholder case
                if result.strip() == placeholder:
                    continue

                if placeholder in result:
                    if not isinstance(value, str):
                        raise TypeError(
                            f"Cannot embed {type(value).__name__} into string: {obj}"
                        )
                    result = result.replace(placeholder, value)
            return result

        # Case 3: dict
        if isinstance(obj, dict):
            return {
                k: self._resolve_python_blocks(v)
                for k, v in obj.items()
            }

        # Case 4: list
        if isinstance(obj, list):
            return [
                self._resolve_python_blocks(v)
                for v in obj
            ]

        return obj

    def _strip_newlines_recursive(self, obj, parent_key=None):
        # Keys that should preserve their exact formatting including newlines
        # Used for SQL templates, queries, and other multiline formatted strings
        PRESERVE_NEWLINES_KEYS = {"sql", "query", "sql_template", "command", "script"}

        if isinstance(obj, str):
            # Don't strip newlines for SQL/query fields - preserve exact formatting
            if parent_key in PRESERVE_NEWLINES_KEYS:
                return obj  # Keep newlines as-is for SQL templates
            return obj.replace("\n", "").strip()  # Strip for other strings
        elif isinstance(obj, dict):
            return {k: self._strip_newlines_recursive(v, parent_key=k) for k, v in obj.items()}
        elif isinstance(obj, list):
            return [self._strip_newlines_recursive(v, parent_key=parent_key) for v in obj]
        else:
            return obj



if __name__ == "__test__":
    reader = YamlReader("../data_source_configs/incorrect_yaml_format.yaml")  # -> incorrect check for yaml
    yaml_reader = reader.read()
    print(yaml_reader)
