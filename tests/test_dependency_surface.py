"""
Dependency surface tests for the spatial-ETL framework.

Two questions this file answers:
  1. MAPPER SURFACE AREA  — given a class_name in config.yaml, what internal
     framework modules does that mapper import beyond DataSourceABCImpl?
     A mapper that only touches [data_source_abc_impl, database_tables, utils]
     is "thin"; one that directly imports core/ or main_core/ internals is "fat"
     and fragile when core changes.

  2. CORE BLAST RADIUS — if a core module changes, which mapper files and
     framework modules transitively import it?  A wide blast radius signals a
     candidate for an interface boundary.

Run:
    python3 -m pytest tests/test_dependency_surface.py -v
    python3 -m pytest tests/test_dependency_surface.py -v -k blast        # blast radius only
    python3 -m pytest tests/test_dependency_surface.py -v -k surface      # mapper surface only
    python3 tests/test_dependency_surface.py                               # plain script, prints report
"""

import ast
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]

# ── helpers ──────────────────────────────────────────────────────────────────

FIRST_PARTY = {
    "core", "main_core", "data_mappers", "data_config_dtos",
    "handlers", "readers", "graph", "database", "database_tables",
    "communication", "log_manager", "utils", "yaml_helper",
    "materialized_views", "metadata", "proj",
}

# Packages a "well-behaved" mapper is allowed to import directly.
# Anything outside this list is considered a framework-core dependency
# and counted in the surface-area score.
ALLOWED_MAPPER_IMPORTS = {
    "main_core.data_source_abc_impl",
    "main_core.data_source_abc",
    "database_tables",          # staging / enrichment / mapping table definitions
    "utils",                    # generic helpers
    "log_manager",              # logging
    "data_config_dtos",         # DTOs only
}


def _extract_imports(filepath: Path) -> list[str]:
    """Return all module names imported (direct or from-import) in a Python file."""
    try:
        tree = ast.parse(filepath.read_text(encoding="utf-8"))
    except SyntaxError:
        return []
    modules = []
    for node in ast.walk(tree):
        if isinstance(node, ast.Import):
            for alias in node.names:
                modules.append(alias.name)
        elif isinstance(node, ast.ImportFrom):
            if node.module:
                modules.append(node.module)
    return modules


def _first_party_imports(filepath: Path) -> list[str]:
    """Filter imports down to first-party framework modules."""
    result = []
    for mod in _extract_imports(filepath):
        top = mod.split(".")[0]
        if top in FIRST_PARTY:
            result.append(mod)
    return sorted(set(result))


def _mapper_files() -> list[Path]:
    return sorted((ROOT / "data_mappers").glob("*.py"))


def _framework_python_files() -> list[Path]:
    """All first-party .py files (excluding mappers themselves)."""
    files = []
    for pkg in FIRST_PARTY - {"data_mappers"}:
        pkg_dir = ROOT / pkg
        if pkg_dir.is_dir():
            files.extend(pkg_dir.rglob("*.py"))
        elif (ROOT / f"{pkg}.py").exists():
            files.append(ROOT / f"{pkg}.py")
    return files


# ── surface-area helpers ──────────────────────────────────────────────────────

def _surface_score(mapper_path: Path) -> dict:
    """
    Return a dict describing how 'fat' a mapper is.
    Score = number of first-party imports that are NOT in ALLOWED_MAPPER_IMPORTS.
    """
    imports = _first_party_imports(mapper_path)
    extra = []
    for imp in imports:
        allowed = any(imp == a or imp.startswith(a + ".") for a in ALLOWED_MAPPER_IMPORTS)
        if not allowed:
            extra.append(imp)
    return {
        "mapper": mapper_path.stem,
        "all_first_party": imports,
        "extra_core_imports": extra,
        "score": len(extra),
    }


# ── blast-radius helpers ──────────────────────────────────────────────────────

# Core modules whose blast radius we care about.
CORE_MODULES = [
    "main_core.data_source_abc_impl",
    "main_core.data_source_abc",
    "main_core.mapping_sql_builder",
    "main_core.core_config",
    "main_core.data_source_mapper",
    "database.db_instancce",
    "database.db_repository",
    "core.init_scheduler",
]


def _module_name(path: Path) -> str:
    """Convert an absolute path inside ROOT to a dotted module name."""
    rel = path.relative_to(ROOT)
    parts = list(rel.with_suffix("").parts)
    return ".".join(parts)


def _blast_radius(core_module: str) -> dict:
    """
    Which files (mappers + framework) directly import `core_module`?
    Returns counts split by: mappers vs framework files.
    """
    all_files = _mapper_files() + _framework_python_files()
    mapper_hits, framework_hits = [], []
    for f in all_files:
        if f.stem == "__init__":
            continue
        if core_module in _extract_imports(f):
            rel = str(f.relative_to(ROOT))
            if f.parent.name == "data_mappers":
                mapper_hits.append(rel)
            else:
                framework_hits.append(rel)
    return {
        "core_module": core_module,
        "mapper_dependents": sorted(mapper_hits),
        "framework_dependents": sorted(framework_hits),
        "total": len(mapper_hits) + len(framework_hits),
    }


# ── pytest: mapper surface area ───────────────────────────────────────────────

@pytest.mark.parametrize("mapper_path", _mapper_files(), ids=lambda p: p.stem)
def test_surface_mapper_imports_abc_impl(mapper_path):
    """Every mapper must import DataSourceABCImpl (its only required base)."""
    if mapper_path.stem == "__init__":
        pytest.skip("not a mapper")
    imports = _first_party_imports(mapper_path)
    # testMapper is exempt — it's a skeleton used for manual debugging
    if mapper_path.stem == "testMapper":
        pytest.skip("testMapper is a dev scaffold, not a real mapper")
    assert any(
        "data_source_abc_impl" in imp or "data_source_abc" in imp
        for imp in imports
    ), f"{mapper_path.stem} does not import DataSourceABC* — is it a real mapper?"


@pytest.mark.parametrize("mapper_path", _mapper_files(), ids=lambda p: p.stem)
def test_surface_score_reported(mapper_path, capsys):
    """
    Always passes — prints each mapper's surface score so CI logs show it.
    A high score (≥3) is a WARNING that the mapper is tightly coupled to core.
    """
    if mapper_path.stem in ("__init__", "testMapper"):
        pytest.skip()
    info = _surface_score(mapper_path)
    score = info["score"]
    level = "OK" if score == 0 else ("WARN" if score < 3 else "HIGH")
    print(
        f"\n[surface] {info['mapper']:30s}  score={score:2d}  [{level}]"
        + (f"  extra={info['extra_core_imports']}" if info["extra_core_imports"] else "")
    )
    # Soft assertion: flag but don't fail — gives you a baseline to tighten later
    if score >= 5:
        pytest.fail(
            f"{info['mapper']} has surface score {score} (≥5) — "
            f"it directly imports {info['extra_core_imports']}. "
            "Consider extracting an interface or utility."
        )


# ── pytest: core blast radius ─────────────────────────────────────────────────

@pytest.mark.parametrize("core_module", CORE_MODULES)
def test_blast_radius_reported(core_module, capsys):
    """
    Always passes — prints each core module's blast radius so CI logs show it.
    A blast radius >8 means that module is a hotspot: changes ripple everywhere.
    """
    info = _blast_radius(core_module)
    total = info["total"]
    level = "OK" if total <= 4 else ("WARN" if total <= 8 else "HOTSPOT")
    print(
        f"\n[blast]   {core_module:45s}  dependents={total:2d}  [{level}]"
        f"\n           mappers   : {info['mapper_dependents']}"
        f"\n           framework : {info['framework_dependents']}"
    )
    # data_source_abc_impl is intentionally the universal base — every mapper inherits it.
    # Its blast radius tracks growth, not a current problem.
    expected_hotspot = core_module == "main_core.data_source_abc_impl"
    if total > 15 or (not expected_hotspot and total > 12):
        pytest.fail(
            f"{core_module} has blast radius {total} (threshold exceeded). "
            "It may need an interface boundary to reduce coupling."
        )


@pytest.mark.parametrize("core_module", CORE_MODULES)
def test_blast_radius_mapper_count(core_module):
    """
    The number of mappers that directly import each core module is tracked.
    This test records the current count and fails only if it GROWS beyond the
    threshold — act as a ratchet, tighten the threshold as you refactor.
    """
    info = _blast_radius(core_module)
    mapper_count = len(info["mapper_dependents"])

    # Current observed maximums — adjust these thresholds as you improve coupling.
    # To tighten: lower the value after a refactor. To document: leave a comment.
    thresholds = {
        "main_core.data_source_abc_impl": 12,   # every mapper inherits this
        "main_core.data_source_abc":       2,    # only impl + one test mapper
        "main_core.mapping_sql_builder":   3,
        "main_core.core_config":           1,    # mappers should NOT need CoreConfig
        "main_core.data_source_mapper":    0,    # orchestrator, not for mappers
        "database.db_instancce":           0,    # mappers get DB via __init__ injection
        "database.db_repository":          2,
        "core.init_scheduler":             0,    # scheduler passed via __init__
    }
    limit = thresholds.get(core_module, 99)
    assert mapper_count <= limit, (
        f"{core_module} is now imported by {mapper_count} mapper(s) "
        f"(threshold={limit}): {info['mapper_dependents']}. "
        "Either update the threshold or fix the coupling."
    )


# ── standalone report (python3 tests/test_dependency_surface.py) ──────────────

def _print_full_report():
    print("\n" + "═" * 70)
    print("  MAPPER SURFACE AREA REPORT")
    print("  (score = # of non-allowed first-party imports)")
    print("═" * 70)
    rows = []
    for p in _mapper_files():
        if p.stem in ("__init__", "testMapper"):
            continue
        rows.append(_surface_score(p))
    rows.sort(key=lambda r: -r["score"])
    for r in rows:
        level = "OK" if r["score"] == 0 else ("WARN" if r["score"] < 3 else "HIGH")
        print(f"  {r['mapper']:30s}  score={r['score']:2d}  [{level}]")
        if r["extra_core_imports"]:
            for imp in r["extra_core_imports"]:
                print(f"    └─ {imp}")

    print("\n" + "═" * 70)
    print("  CORE BLAST RADIUS REPORT")
    print("  (dependents = files that directly import each core module)")
    print("═" * 70)
    blasts = [_blast_radius(m) for m in CORE_MODULES]
    blasts.sort(key=lambda r: -r["total"])
    for r in blasts:
        level = "OK" if r["total"] <= 4 else ("WARN" if r["total"] <= 8 else "HOTSPOT")
        print(f"  {r['core_module']:45s}  total={r['total']:2d}  [{level}]")
        if r["mapper_dependents"]:
            print(f"    mappers   : {', '.join(Path(p).stem for p in r['mapper_dependents'])}")
        if r["framework_dependents"]:
            print(f"    framework : {', '.join(r['framework_dependents'])}")
    print()


if __name__ == "__main__":
    _print_full_report()
