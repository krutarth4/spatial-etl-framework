# Contributing to Spatial ETL Framework

Thank you for your interest in contributing. This document covers how to set up a local development environment, add a new datasource, and submit changes.

---

## Local development setup

**Prerequisites:** Python 3.11+, PostgreSQL 16 with PostGIS 3.4 (or Docker).

```bash
git clone https://github.com/your-org/spatial-etl-framework.git
cd spatial-etl-framework

python3 -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
```

Copy the example env file and fill in your database credentials:

```bash
cp .env.example .env
# edit DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
```

Start a PostGIS container (skip if you have a local instance):

```bash
docker run --name postgis \
  -e POSTGRES_USER=postgres -e POSTGRES_PASSWORD=admin123 -e POSTGRES_DB=mydb \
  -p 5432:5432 -d postgis/postgis:16-3.4
```

Run the pipeline:

```bash
python3 run.py
```

The debug API is available at `http://localhost:8000/docs`.

---

## Periodic scheduling

`config.yaml` ships with `scheduler.enable: false`. This means datasources run once at startup and are not rescheduled. To test cron/interval triggers locally, set it to `true`.

---

## Adding a new datasource

Most datasources need only a YAML config file in `data_source_configs/`. For non-standard source formats (gz, XML, custom binary) or complex SQL transforms, add a mapper class in `data_mappers/`.

Full walkthrough: [docs/configure-data-source-step-by-step.md](docs/configure-data-source-step-by-step.md)

Real-world examples: [docs/example-tree-mapper.md](docs/example-tree-mapper.md), [docs/example-air-quality-mapper.md](docs/example-air-quality-mapper.md), and others in [docs/](docs/).

---

## Code style

The project uses [Ruff](https://docs.astral.sh/ruff/) for linting and formatting. Run it before committing:

```bash
pip install ruff
ruff check .
ruff format .
```

Configuration is in `pyproject.toml`. Line length is 100. The linter runs on `data_mappers/`, `core/`, `main_core/`, `handlers/`, `readers/`, and `database/`.

---

## Running tests

```bash
python3 -m pytest tests/
```

The dependency surface test (`tests/test_dependency_surface.py`) checks that the base mapper class and config loader import cleanly without a live database. Run it after any changes to `main_core/` or `database/`.

---

## What to contribute

- **New datasources** — open data feeds for cities other than Berlin (weather, air quality, cycling infrastructure, elevation, noise, etc.)
- **New mapping strategies** — spatial-join types not yet covered in `main_core/strategies/`
- **New format readers** — file types not yet handled in `readers/` (NetCDF, HDF5, COG rasters, GTFS, etc.)
- **Documentation** — real migration examples from your own dataset in `docs/`
- **Bug reports** — open a GitHub issue with the datasource config, error traceback, and PostGIS version

---

## Pull request process

1. Fork the repository and create a branch from `main`.
2. Make your changes. Keep commits focused — one logical change per commit.
3. Run `ruff check .` and `ruff format .` and fix any issues.
4. Open a pull request with a short description of what the change does and why.
5. For new datasources, include a sample of the source data or a link to the public API.

---

## Code of conduct

All contributors are expected to follow the project's [Code of Conduct](CODE_OF_CONDUCT.md). Report unacceptable behavior to **krutarthparwal.ai@gmail.com**.
