# Datasource Config Features — Reference

A pluggable, scalable pattern for owning one slice of a datasource's YAML
configuration. Each feature is a single Python module that declares its
schema, defaults, validation rules, and a human-readable description of
what the key is for and where it is consumed.

**Goal:** adding a new config property = drop a new file. No edits to
`CoreConfig`, no edits to the central DTO, no scattered validation.

---

## Why this pattern exists

Before features, adding a new datasource config key required:

1. Editing [data_config_dtos/data_source_config_dto.py](../data_config_dtos/data_source_config_dto.py) to extend a dataclass.
2. Hand-coding consumption logic inside [main_core/data_source_abc_impl.py](../main_core/data_source_abc_impl.py) or a mapper.
3. Documenting the key separately — purpose and consumption site lived in reader memory and `docs/` only.
4. Writing ad-hoc validation, often discovered only at runtime.

The DatasourceFeature pattern collapses all four into one self-contained module.

---

## Where things live

```
config_features/
  __init__.py
  base.py                 # DatasourceFeature ABC + FeatureIssue + get_dotted
  registry.py             # DatasourceFeatureRegistry (discovery + validation)
  datasource/
    __init__.py
    source_multi_fetch.py # first vertical slice
    <your_new_feature>.py # drop new modules here
```

The registry auto-discovers every module under `config_features/datasource/`
via `pkgutil.iter_modules`. Importing the module runs its
`@DatasourceFeatureRegistry.register` decorator, which is the only thing
needed to plug a feature in.

---

## The DatasourceFeature contract

Defined in [config_features/base.py](../config_features/base.py).

| Member | Required | Purpose |
|---|---|---|
| `KEY` | yes | Dotted path inside a datasource block, e.g. `"source.multi_fetch"`. Must be unique. |
| `SCHEMA` | no | Dataclass used to parse this slice. `None` → raw passthrough. |
| `DESCRIPTION` | recommended | Free-text. Surfaces in `describe()`. State what the key does, where it is consumed, and notable gotchas. |
| `default()` | no | Class method returning the value when the key is absent. Default returns `None`. |
| `parse(raw)` | no | Class method `raw dict → SCHEMA`. Default uses `dacite.from_dict` with `cast=[dict]`. Override only if you need special handling (e.g. polymorphic union resolution). |
| `validate(parsed, datasource_name)` | no | Class method returning a list of `FeatureIssue`. Errors abort load; warnings only log. Default returns `[]`. |

`FeatureIssue` carries `datasource_name`, `feature_key`, `message`, and
`level` (`"error"` or `"warning"`). Its `__str__` formats consistently
with the existing `TriggerIssue` style in
[validators/job_trigger_validator.py](../validators/job_trigger_validator.py).

---

## How validation runs

`CoreConfig.__init__` calls `_validate_datasource_features()` after job
triggers are validated. See [main_core/core_config.py](../main_core/core_config.py).

For each datasource in the loaded YAML, and for each registered feature:

1. Read the slice via `get_dotted(datasource, feature.KEY)`.
2. Call `feature.parse(slice)`. Parse failures become errors and skip step 3.
3. Call `feature.validate(parsed, datasource_name)`. Issues are collected.

After all datasources are processed:

- Warnings are logged via `self.logger.warning`.
- If any error exists, `CoreConfig` raises `ValueError` listing every issue. The pipeline does not start with a broken config.

---

## Writing a new feature — full example

Suppose you want to add `mapping.cache` to control per-mapping-table
result caching.

### 1. Add (or reuse) a DTO

If a fitting dataclass already exists in
[data_config_dtos/data_source_config_dto.py](../data_config_dtos/data_source_config_dto.py),
reuse it. Otherwise add a new one there:

```python
@dataclass
class MappingCacheDTO:
    enable: bool
    ttl_seconds: int = 3600
    key_columns: Optional[list[str]] = None
```

### 2. Drop a feature module

Create `config_features/datasource/mapping_cache.py`:

```python
from config_features.base import DatasourceFeature, FeatureIssue
from config_features.registry import DatasourceFeatureRegistry
from data_config_dtos.data_source_config_dto import MappingCacheDTO


@DatasourceFeatureRegistry.register
class MappingCacheFeature(DatasourceFeature):
    KEY = "mapping.cache"
    SCHEMA = MappingCacheDTO
    DESCRIPTION = """
    Caches mapping-table results between runs to skip re-computing
    expensive joins. ttl_seconds controls invalidation. key_columns
    determines the cache key shape; defaults to the mapping's base_column.

    Consumed by: MappingInsertBuilder during the mapping phase.
    Skipped when enable is false or mapping.enable is false.
    """

    @classmethod
    def validate(cls, parsed, datasource_name):
        issues = []
        if parsed is None or not parsed.enable:
            return issues
        if parsed.ttl_seconds <= 0:
            issues.append(FeatureIssue(
                datasource_name, cls.KEY,
                f"ttl_seconds must be > 0, got {parsed.ttl_seconds}",
                "error",
            ))
        if parsed.key_columns is not None and not parsed.key_columns:
            issues.append(FeatureIssue(
                datasource_name, cls.KEY,
                "key_columns is set but empty; omit the key to use defaults",
                "warning",
            ))
        return issues
```

### 3. That's it

No changes to `CoreConfig`, no changes to `DataSourceMapper`, no manual
registration. The next run of `python3 run.py` will:

- Discover the module.
- Parse `mapping.cache` from every datasource block.
- Surface errors that previously would have failed mid-run.

When a consumer (e.g. `MappingInsertBuilder`) needs the parsed value, it
can call:

```python
from config_features.registry import DatasourceFeatureRegistry

cache_cfg = DatasourceFeatureRegistry.parse_slice(ds_raw_dict, "mapping.cache")
```

…or continue reading via the existing `DataSourceDTO` if the DTO carries
the field.

---

## Introspection — what features are registered?

Programmatic:

```python
from config_features.registry import DatasourceFeatureRegistry
DatasourceFeatureRegistry.load_all()
for entry in DatasourceFeatureRegistry.describe():
    print(entry["key"], "→", entry["schema"])
    print(entry["description"])
    print("---")
```

This is also the recommended shape for a future `/debug/config-features`
FastAPI endpoint so the live pipeline can self-document.

---

## Current vertical slice — `source.multi_fetch`

The first feature to migrate is
[config_features/datasource/source_multi_fetch.py](../config_features/datasource/source_multi_fetch.py).

It owns the `source.multi_fetch` block (already typed by `SourceMultiFetchDTO`)
and enforces, at config-load time, the validation that previously only
ran inline in `DataSourceABCImpl.multi_fetch()` after the pipeline had
already started:

| Strategy | Pre-flight checks |
|---|---|
| `expand_params` | Requires at least one of `expand` or `params`. |
| `url_template` | Requires `url_template`; `template_params` must be a non-empty mapping of name → list, all lists equal length. |
| `explicit_url_list` | Requires non-empty `urls` (list or `SourceInputDTO`). |
| any | Strategy name must be one of the `SourceMultiFetchStrategy` values. |
| any | `fetch_workers`, `request_timeout`, `retry_attempts` below sensible minimums → warning. |

---

## Naming and organisation conventions

- **One file per feature.** Even small ones. Avoid grouping unrelated keys in one module.
- **Filename mirrors KEY** with dots → underscores: `source.multi_fetch` → `source_multi_fetch.py`.
- **DESCRIPTION** should answer three questions: *what the key does*, *where it is consumed*, *when it is skipped*.
- **Schemas live in `data_config_dtos/`,** not in the feature module. Features own *behaviour*; the central DTO file remains the canonical type catalog.
- **No side effects at import time** other than the `@register` decorator. No DB calls, no FS reads.

---

## Lifecycle hooks — future direction

Today a feature owns schema + defaults + validation. The next phase
will optionally let a feature own *runtime behaviour* too via lifecycle
hooks such as `on_extract`, `on_pre_db`, `on_mapping`. When that lands,
`DataSourceABCImpl` will dispatch to features at each phase instead of
hard-coding consumption, and the central `DataSourceDTO` can shrink
as fields move into their owning feature modules. The current
framework is intentionally additive so this migration is safe and
incremental.

---

## Quick checklist for a new feature

1. Identify the YAML key (`a.b.c`) it owns and confirm no existing feature claims it.
2. Reuse or add a DTO in [data_config_dtos/data_source_config_dto.py](../data_config_dtos/data_source_config_dto.py).
3. Create `config_features/datasource/<key_underscored>.py`.
4. Subclass `DatasourceFeature`, set `KEY`, `SCHEMA`, `DESCRIPTION`.
5. Decorate with `@DatasourceFeatureRegistry.register`.
6. Implement `validate()` for any preconditions that today fail only at runtime.
7. Run `python3 run.py` (or invoke `CoreConfig()` in a shell) — registry picks it up automatically.
