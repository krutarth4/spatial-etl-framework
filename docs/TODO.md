# Spatial ETL Framework — Open-Source TODO

Outstanding work to make this project genuinely ready as an open-source release. Each item links to the file/line it refers to so the fix is unambiguous.

Last updated: 2026-04-23

---

## 1. Security — must-fix before public release

### 1.1 Remove hard-coded `admin123` credentials
The string `admin123` appears in six places in the working tree. It's fine as a local dev convenience, but not in a repo we invite contributors to read:

- [core/command_runner.py:156](../core/command_runner.py#L156) — `os.environ["PGPASSWORD"] = "admin123"` in the `__main__` block.
- [core/command_runner.py:171](../core/command_runner.py#L171) — `-W admin123` hard-coded in an `osm2pgrouting` command.
- [utils/processRunner.py:102](../utils/processRunner.py#L102) — same pattern in the `__main__` demo.
- [test.config.yaml:30](../test.config.yaml#L30) — `password: &pass "admin123"`.
- [config_test.yaml:11](../config_test.yaml#L11) — `password: admin123`.
- [Readme.md:93](../Readme.md#L93) — the Quick Start tells readers to run `docker run ... -e POSTGRES_PASSWORD=admin123`.

Action:
1. Delete the `__main__` dev snippets from `command_runner.py` and `processRunner.py` (or move them to a gitignored `scripts/dev/` folder) — they are leftover developer-specific smoke tests with paths like `../raw/ernst_extract.osm` that don't exist.
2. Replace both test YAMLs with values sourced from env vars, or collapse them into a single template pointing to `.env.example`.
3. Update the Quick Start to use a placeholder (`POSTGRES_PASSWORD="$(openssl rand -hex 16)"`) and explain the `.env` workflow.
4. Consolidate: `config.yaml` already uses `CHANGE_ME` sentinels and `core_config._apply_env_overrides()` swaps them at load — document this clearly in the README so nobody edits real credentials into `config.yaml`.

### 1.2 Harden the YAML `exec` / `eval` path
[readers/yaml_reader.py:43-72](../readers/yaml_reader.py#L43-L72) evaluates Python expressions embedded in YAML via `${{ ... }}` by calling `exec()` then `eval()`.

- The "safe" globals only exclude `datetime` and `ZoneInfo`, but Python's built-ins remain reachable — a crafted YAML can still access `__import__`, `open`, `subprocess`, etc. (`eval` does not sandbox by restricting globals alone.)
- Document clearly in the README and in a module-level docstring that **`config.yaml` is executable code — only load YAMLs from trusted sources.** Today this is buried.
- Tighten the execution environment: pass `{"__builtins__": {}}` as the globals with only the explicit functions allow-listed, or switch to a tiny expression DSL if the only real use-case is formatting a timestamp.
- Wrap the regex match with a size limit on `code_block` so a 100 MB YAML cannot hang the process.

### 1.3 Debug API exposure and authentication
[core/main.py](../core/main.py) wires a FastAPI app with nine debug endpoints:
- `GET /debug/datasources`, `GET /debug/datasources/{mapper_endpoint}`
- `GET /debug/mappers`, `GET /debug/mappers/{endpoint}/{target}`
- `GET /debug/mappers/{endpoint}/mapping-visualization`
- `GET /debug/mappers/{endpoint}/nearest-way`
- `GET /debug/mappers/{endpoint}/way-inspector`

[config.yaml:16](../config.yaml#L16) binds the server to `host: 0.0.0.0` — on any host that's reachable from outside, this exposes raw DB contents to anyone.

Action:
- Default `host` to `127.0.0.1` and make `0.0.0.0` an opt-in for containerised deploys.
- Add a minimal auth guard (header token from env var, or HTTP basic auth) gating `/debug/*`. Skip with an explicit `DEBUG_AUTH_DISABLED=1` for local work.
- Document that the debug API is not meant for production and add a warning banner on the `/` root response.

### 1.4 CORS policy
[core/main.py:13-18](../core/main.py#L13-L18) pins `allow_origins=["http://localhost:4200"]` but `allow_methods=["*"], allow_headers=["*"]`. Tighten headers to the minimum needed (`Content-Type`, `Authorization`) and enumerate methods once the debug-only surface is confirmed (only GETs today).

### 1.5 SQL identifier safety
[main_core/mapping_sql_builder.py](../main_core/mapping_sql_builder.py) and related builders interpolate `table_name` / `table_schema` / column names from the YAML config directly into SQL via f-strings. This is fine **as long as the config is trusted**, which — per §1.2 — we need to make explicit.

Action:
- Add a validator in `data_config_dtos/` that rejects identifiers not matching `^[A-Za-z_][A-Za-z0-9_]*$` at config-load time. Current generators rely on the honour system; failing fast at load is cheap insurance.
- Prefer `psycopg.sql.Identifier(...)` for any identifier that ends up in a query. Keep f-string interpolation only for the purely static SQL scaffolding.

### 1.6 Secrets-in-logs audit
- `core_config._apply_env_overrides` logs nothing, good. But `CommandRunner` and `ProcessRunner` log the full argv via `" ".join(command)` — if a password is ever passed on a command line (e.g. `-W <pw>`), it ends up in the log file. Scrub or redact the argv before logging.
- `database/db_configuration.py:122` builds the SQLAlchemy URL with inline password. Ensure that any code path that exceptions-out does not print the URL directly.

### 1.7 Remove the commented-out auth stub
[proj/app_celery.py:20-41](../proj/app_celery.py#L20-L41) has a commented auth example with a hardcoded username/password pair. Delete or move to a docs example so the pattern does not get cargo-culted.

### 1.8 Dependency scanning and pinning
- [requirements.txt](../requirements.txt) partially pins (APScheduler, SQLAlchemy, psycopg, uvicorn, PyYAML) but leaves `rasterio`, `pandas`, `celery`, `ijson`, `pyproj`, `pygments`, `geopandas`, `osmium`, `shapely`, `requests`, `scipy`, `pyarrow`, `fastapi` floating. Add pins everywhere and check in `requirements.lock.txt` (via `pip-tools` or `uv pip compile`).
- Add Dependabot config at `.github/dependabot.yml` covering `pip`, `docker`, and `github-actions`.
- Pin the Docker base image digest in [Dockerfile](../Dockerfile) (`FROM python:3.11-slim@sha256:...`) to make builds reproducible.
- Add `pip-audit` / `safety` and `bandit` to CI (see §4).

---

## 2. Consistency bugs the README / code disagrees on

- **Python version**: README badge says "Python 3.13+" ([Readme.md:6](../Readme.md#L6)) but [Dockerfile:1](../Dockerfile#L1) is `python:3.11-slim`. Pick one, update both.
- **Hot-reload cadence**: README says "~2 s" ([Readme.md:76](../Readme.md#L76)) but [config.yaml:23](../config.yaml#L23) sets `poll_seconds: 30`. Align the docs to the default or vice versa.
- **`data_folder`** is declared in `config.yaml` as `./data_source_configs/`, but no such directory exists in the repo. Either add an example datasource YAML in that folder or remove the key and document that datasources live inline in `config.yaml`.

---

## 3. Tests

There are **zero tests** in the tree (`find -name 'test_*.py' -o -name 'tests'` returns nothing outside `.venv`). This is the single biggest credibility gap for a framework intended to be reused.

Minimum first slice:
- **Unit tests for `main_core/mapping_sql_builder.py`** — each strategy (`knn`, `nearest_k`, `within_distance`, `aggregate_within_distance`, `intersection`, `attribute_join`, `sql_template`) deserves one test that asserts the generated SQL against a golden string. No DB required.
- **Unit tests for `readers/yaml_reader.py`** — cover `${{ ... }}` evaluation, error cases, and the `PRESERVE_NEWLINES_KEYS` behaviour.
- **Unit tests for `main_core/core_config._apply_env_overrides`** — env vars override YAML correctly, bad `DB_PORT` is warned not crashed.
- **Integration tests with Testcontainers-Postgres (PostGIS)** — run one KNN mapping end-to-end against a fixture datasource with three rows.
- **Schema validation for every built-in datasource YAML** — iterate and just load it through `CoreConfig` / `DataSourceConfigDto` to catch malformed configs at test time.

Add `pytest`, `pytest-asyncio`, `testcontainers[postgresql]` to a `requirements-dev.txt`.

---

## 4. CI / CD

No `.github/workflows/` directory exists. Add:

- **`ci.yml`** — on push/PR: install deps, run `ruff`/`black --check`, run `pytest`, build the Docker image.
- **`security.yml`** — `gitleaks` (secret scan), `bandit` (AST security lints), `pip-audit` (CVE check), `hadolint` (Dockerfile), `yamllint` (config correctness).
- **`release.yml`** — tag-triggered: build and push a multi-arch image to GHCR, draft a GitHub Release auto-populated from the changelog.

Enable branch protection on `main`: required status checks = `ci`, `security`; at least one approving review.

---

## 5. Open-source community files

These are standard expectations at the repo root and are all missing:

- **`LICENSE`** — the [Readme.md:5](../Readme.md#L5) badge advertises GPLv3 but no `LICENSE` file is in the repo. Ship [https://www.gnu.org/licenses/gpl-3.0.txt](https://www.gnu.org/licenses/gpl-3.0.txt) as `LICENSE`.
- **`CONTRIBUTING.md`** — how to set up a dev env, coding style, branch naming, commit message format, how to add a datasource / a mapping strategy / a new test.
- **`SECURITY.md`** — supported versions, how to report vulnerabilities privately (email or GitHub Security Advisories), expected response time.
- **`CHANGELOG.md`** — keep-a-changelog format. Seed with `v0.1.0` = "first public release".
- **`.github/ISSUE_TEMPLATE/bug_report.md`** and **`feature_request.md`**.
- **`.github/PULL_REQUEST_TEMPLATE.md`**.
- Track the existing **[CODE_OF_CONDUCT.md](../CODE_OF_CONDUCT.md)** — it's currently untracked in `git status`. Commit it.

---

## 6. Documentation polish

The [docs/](./) folder is already healthy. Remaining gaps:

- **`docs/ARCHITECTURE.md`** — expand the ASCII pipeline diagram from the README into a real explainer: module responsibilities, how `Application` owns the scheduler, how `DataSourceABCImpl` composes with `MappingSqlBuilder`, the hot-reload contract.
- **`docs/DEPLOYMENT.md`** — production posture: run behind a reverse proxy, set `host: 127.0.0.1`, enable auth on `/debug/*`, backups, resource limits.
- **`docs/SECURITY_MODEL.md`** — the `config.yaml`-is-executable warning from §1.2, the identifier-trust assumption from §1.5, and the debug-API threat model. Even a one-pager is enough.
- **Cross-link audit** — after `Readme.md` was recently rewritten, confirm every relative link in `docs/*.md` still resolves (a handful still reference MDP-era paths such as `modular-data-pipeline/...`).
- **Screenshots for the debug API** — the endpoints at [core/main.py](../core/main.py) are the project's main selling point; one screenshot in the README would do more than the table.

---

## 7. Runtime robustness

- [run.py:39](../run.py#L39) reloads by calling `os.execv` — this skips graceful shutdown: open DB connections, the APScheduler, and any in-flight mapper runs are abandoned. Either trap `SIGTERM` in `Application` and shut down cleanly first, or prefer Uvicorn's built-in `reload=True` path (already handled at [run.py:64-65](../run.py#L64-L65)).
- [run.py:10](../run.py#L10) calls `load_dotenv` before imports that read env vars — good. But confirm that imports happening at module load time (e.g. `from core.application import Application`) do not capture env vars earlier than this; a defensive reload at the top of `Application.__init__` would be cheaper than debugging a stale-env bug later.
- Structured logging: [log_manager/logger_manager.py](../log_manager/logger_manager.py) sets up file logging — confirm it emits JSON in production and wire it to stdout when running under Docker so `docker logs` works without a volume mount.

---

## 8. Repo hygiene

- `.dockerignore` is only 69 bytes — verify it excludes at least `.venv/`, `.git/`, `tests/`, `docs/`, `.env`, `logs/`, `tmp/`, `raw/`. A fat build context slows every image build.
- [config_test.yaml](../config_test.yaml) vs [test.config.yaml](../test.config.yaml) — two "test config" files exist, both with `admin123`. Keep one, delete the other.
- Single capital-R Readme: rename to the conventional `README.md` so GitHub's anchor/section auto-generation works and third-party tooling picks it up. `Readme.md` works on macOS's case-insensitive FS but is inconsistent on Linux.

---

## Suggested execution order

1. **Security quick wins** — §1.1, §1.7, §1.8 (mostly deletions and pinning; one afternoon).
2. **OSS meta-files** — §5 (LICENSE first, then CONTRIBUTING / SECURITY / CHANGELOG / templates).
3. **CI skeleton** — §4 `ci.yml` + `security.yml` (locks in regressions from the next steps).
4. **Debug-API hardening** — §1.3, §1.4, §7 bullet 1.
5. **YAML-eval hardening + docs** — §1.2 and §6 `SECURITY_MODEL.md`.
6. **SQL identifier validator** — §1.5 (small change with high safety return).
7. **First test slice** — §3 (SQL builder golden tests + YAML reader unit tests).
8. **Docs polish** — §2 consistency fixes, §6 new docs.
9. **Hygiene pass** — §8.
