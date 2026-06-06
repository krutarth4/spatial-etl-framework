import ast
import os
import re
import textwrap
from datetime import datetime
from zoneinfo import ZoneInfo

import yaml
from dotenv import load_dotenv

from log_manager.logger_manager import LoggerManager

load_dotenv()


class YamlReader:
    PYTHON_BLOCK_PATTERN = r"(?<!#)\$\{\{(.*?)\}\}"

    # Centralized allow-list of names exposed to `${{ }}` config blocks.
    # Configs are NOT allowed to `import` anything directly — if a new package
    # is genuinely needed, add it here (one place, reviewed) and reference it by
    # name in the config. Keeping this internal means configs stay declarative
    # and can't pull in arbitrary modules.
    SAFE_IMPORTS = {
        "datetime": datetime,
        "ZoneInfo": ZoneInfo,
        "os": os,
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
            data = self._strip_newlines_recursive(data)
            return self._resolve_tmp_dir(data)

    def _evaluate_python_block(self, code: str):
        code = textwrap.dedent(code).strip()

        # Configs may not import anything directly. Allowed packages must be
        # registered in SAFE_IMPORTS and referenced by name instead.
        self._reject_imports(code)

        lines = code.splitlines()

        # last line = return value expression
        final_expr = lines[-1]

        # remaining lines = exec() part (helpers only — no imports)
        exec_part = "\n".join(lines[:-1])

        # safe environment (only the centrally allow-listed names)
        safe_globals = dict(self.SAFE_IMPORTS)

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

    @staticmethod
    def _reject_imports(code: str) -> None:
        """Disallow imports inside config `${{ }}` blocks.

        Catches `import x`, `from x import y`, and `__import__(...)`. Anything a
        config legitimately needs should be added to SAFE_IMPORTS instead.
        """
        try:
            tree = ast.parse(code)
        except SyntaxError:
            # Let the downstream exec/eval surface the real syntax error.
            return
        for node in ast.walk(tree):
            if isinstance(node, (ast.Import, ast.ImportFrom)):
                raise ValueError(
                    "`import` is not allowed in config ${{ }} blocks. "
                    "Register the package in YamlReader.SAFE_IMPORTS and "
                    "reference it by name instead."
                )
            if (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Name)
                and node.func.id == "__import__"
            ):
                raise ValueError(
                    "`__import__` is not allowed in config ${{ }} blocks. "
                    "Register the package in YamlReader.SAFE_IMPORTS instead."
                )

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

    # Cache root sentinel. Any config path whose first segment is `tmp` is treated
    # as living under the (relocatable) tmp/cache directory. The literal `tmp` root
    # is rewritten to TMP_DIR (env var, e.g. set in .env) or `./tmp` when unset.
    # Paths that don't start with `tmp` (e.g. `data/...`, absolute paths) are left
    # untouched, so non-cache outputs are unaffected.
    @staticmethod
    def _tmp_base() -> str:
        return os.getenv("TMP_DIR") or "./tmp"

    def _resolve_tmp_dir(self, obj):
        base = self._tmp_base()
        return self._apply_tmp_base(obj, base)

    def _apply_tmp_base(self, obj, base):
        if isinstance(obj, str):
            return self._rewrite_tmp_path(obj, base)
        if isinstance(obj, dict):
            return {k: self._apply_tmp_base(v, base) for k, v in obj.items()}
        if isinstance(obj, list):
            return [self._apply_tmp_base(v, base) for v in obj]
        return obj

    @staticmethod
    def _rewrite_tmp_path(value: str, base: str) -> str:
        for prefix in ("./tmp/", "tmp/"):
            if value.startswith(prefix):
                return f"{base}/{value[len(prefix):]}"
        if value in ("tmp", "./tmp"):
            return base
        return value

    def _strip_newlines_recursive(self, obj, parent_key=None):
        # Keys that should preserve their exact formatting including newlines
        # Used for SQL templates, queries, and other multiline formatted strings
        PRESERVE_NEWLINES_KEYS = {"sql", "query", "sql_template", "command", "script", "select_sql", "create_sql", "refresh_sql", "filter", "condition", "expression"}

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
