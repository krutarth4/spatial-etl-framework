"""
Static code metrics for a mapper file — computed via AST, no import of the mapper.

Metrics:
  loc_total      — total lines in file
  loc_code       — non-blank, non-comment lines
  import_count   — number of imported modules (direct + from-import)
  method_count   — def nodes at the top-level class body
  override_count — how many of those methods are also defined in DataSourceABC
"""
import ast
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
_ABC_PATH = ROOT / "main_core" / "data_source_abc.py"


def _extract_imports(filepath: Path) -> list[str]:
    """Return all module names imported (direct or from-import). Copied from test_dependency_surface.py."""
    try:
        tree = ast.parse(filepath.read_text(encoding="utf-8"))
    except SyntaxError:
        return []
    modules: list[str] = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.append(alias.name)
        elif isinstance(node, ast.ImportFrom) and node.module:
            modules.append(node.module)
    return modules


def _abc_method_names() -> set[str]:
    """Names of all methods defined (at any level) in DataSourceABC."""
    try:
        tree = ast.parse(_ABC_PATH.read_text(encoding="utf-8"))
    except (FileNotFoundError, SyntaxError):
        return set()
    names: set[str] = set()
    for node in ast.walk(tree):
        if isinstance(node, (ast.FunctionDef, ast.AsyncFunctionDef)):
            names.add(node.name)
    return names


def analyze(mapper_path: Path) -> dict:
    """
    Return static metrics dict for the given mapper file.

    Keys: loc_total, loc_code, import_count, method_count, override_count
    """
    text = mapper_path.read_text(encoding="utf-8")
    lines = text.splitlines()

    loc_total = len(lines)
    loc_code = sum(
        1 for ln in lines
        if ln.strip() and not ln.strip().startswith("#")
    )
    import_count = len(_extract_imports(mapper_path))

    try:
        tree = ast.parse(text)
    except SyntaxError:
        return {
            "loc_total": loc_total,
            "loc_code": loc_code,
            "import_count": import_count,
            "method_count": 0,
            "override_count": 0,
        }

    # Take the first (outermost) class definition — the mapper class
    mapper_cls_node = next(
        (n for n in ast.walk(tree) if isinstance(n, ast.ClassDef)),
        None,
    )

    method_count = 0
    override_count = 0
    if mapper_cls_node is not None:
        abc_names = _abc_method_names()
        for item in mapper_cls_node.body:
            if isinstance(item, (ast.FunctionDef, ast.AsyncFunctionDef)):
                method_count += 1
                if item.name in abc_names:
                    override_count += 1

    return {
        "loc_total": loc_total,
        "loc_code": loc_code,
        "import_count": import_count,
        "method_count": method_count,
        "override_count": override_count,
    }
