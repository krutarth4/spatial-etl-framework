# Docker Configuration

This page covers running the spatial-etl-framework pipeline container standalone — paired with a PostGIS sidecar. For the full MDP stack (router + frontend), see the MDP repository's docker-compose-init.yaml.

---

## Environment Variables

These are passed via `environment:` in compose or `-e` on `docker run`.

### Database connection (required)

| Variable | Description |
|----------|-------------|
| `DB_HOST` | Postgres hostname (e.g. `db` inside compose, `localhost` outside) |
| `DB_PORT` | Postgres port (default `5432`) |
| `DB_NAME` | Database name |
| `DB_USER` | Postgres user |
| `DB_PASSWORD` | Postgres password |

### Datasource selection (optional)

| Variable | Description |
|----------|-------------|
| `ETL_ONLY` | Comma-separated datasource names to run. Overrides per-datasource `enable:` flags in config. Mutually exclusive with `ETL_DISABLE`. |
| `ETL_DISABLE` | Comma-separated datasource names to skip. Everything else runs. |

Both variables accept whitespace around commas. Names not found in config produce a warning, not a hard failure. Equivalent CLI flags are `--only` and `--disable`.

```bash
# Only run elevation and weather
docker run -e ETL_ONLY=elevation,weather spatial-etl

# Run everything except trees
docker run -e ETL_DISABLE=tree spatial-etl
```

### Run mode (optional)

| Variable | Default | Description |
|----------|---------|-------------|
| `EXPERIMENTATION` | `false` | Set to `"true"` to run `experimentation/run_experiment.py` instead of `run.py`. One-shot ETL run, no debug server, no scheduler. |

```bash
# One-shot experiment run
docker run -e EXPERIMENTATION=true spatial-etl
```

---

## Ports

| Port | Purpose |
|------|---------|
| `8000` | FastAPI debug server — `/docs` (Swagger), `/debug/*`, `/health` |

The `/health` endpoint is a lightweight in-memory check — use it for container healthchecks. Do **not** use `/debug/datasources` as a healthcheck probe; it runs `COUNT(*)` on every staging table and will time out under heavy ETL load.

---

## Volumes

Mount these for live development (edit without rebuilding the image):

| Host path | Container path | Purpose |
|-----------|---------------|---------|
| `./config.yaml` | `/app/config.yaml` | Main config — changes picked up in ~2 s (hot-reload) |
| `./data_mappers/` | `/app/data_mappers` | Mapper Python files |
| `./data_source_configs/` | `/app/data_source_configs` | Per-datasource YAML configs |
| `./database_tables/` | `/app/database_tables` | SQLAlchemy table model classes |
| `./main_core/` | `/app/main_core` | Core framework code |
| `./mapping_sql/` | `/app/mapping_sql` | SQL templates for mapping strategies |
| `./mv_configs/` | `/app/mv_configs` | Materialized view definitions |
| `./tmp/` | `/app/tmp` | Downloaded files + comm state JSON |
| `./logs/` | `/app/logs` | Pipeline run logs |
| `./experimentation/logs/` | `/app/experimentation/logs` | Experiment run logs (when `EXPERIMENTATION=true`) |
| `./experimentation/tmp/` | `/app/experimentation/tmp` | Experiment run comm state |

---

## Minimal `docker-compose.yml` (standalone)

A minimal setup with just PostGIS and the pipeline:

```yaml
services:
  db:
    image: postgis/postgis:16-3.4
    environment:
      POSTGRES_USER: postgres
      POSTGRES_PASSWORD: admin123
      POSTGRES_DB: mydb
    ports:
      - "5432:5432"
    healthcheck:
      test: ["CMD-SHELL", "pg_isready -U postgres -d mydb"]
      interval: 5s
      timeout: 3s
      retries: 10

  pipeline:
    build: .
    ports:
      - "8000:8000"
    depends_on:
      db:
        condition: service_healthy
    environment:
      DB_HOST: db
      DB_PORT: 5432
      DB_NAME: mydb
      DB_USER: postgres
      DB_PASSWORD: admin123
    volumes:
      - ./config.yaml:/app/config.yaml
      - ./data_mappers:/app/data_mappers
      - ./data_source_configs:/app/data_source_configs
      - ./tmp:/app/tmp
      - ./logs:/app/logs

volumes: {}
```

Run:
```bash
docker compose up --build
```

---

## Run Modes

| Mode | How |
|------|-----|
| Normal run (scheduler + debug API on `:8000`) | `python3 run.py` |
| Only specific datasources | `python3 run.py --only elevation,weather` |
| Skip specific datasources | `python3 run.py --disable tree` |
| Via env var (same as `--only`) | `ENABLE_DATASOURCES=elevation,weather python3 run.py` |
| Via env var (same as `--disable`) | `DISABLE_DATASOURCES=tree python3 run.py` |
| One-shot experiment run (no server) | `EXPERIMENTATION=true python3 run.py` |

The pipeline hot-reloads when `config.yaml` or any file in `data_source_configs/` changes — no restart needed during development.

---

## Healthcheck

```yaml
healthcheck:
  test: ["CMD", "python3", "-c", "import urllib.request; urllib.request.urlopen('http://localhost:8000/health', timeout=5)"]
  interval: 20s
  timeout: 10s
  retries: 10
  start_period: 180s
```

`start_period: 180s` gives the pipeline time to finish the first ETL run before health is checked. Reduce this if you're only running lightweight datasources.

---

## Postgres Tuning

For production or large datasets, tune these Postgres parameters:

```yaml
# Add to db service command:
command: >
  postgres
  -c shared_buffers=3GB
  -c work_mem=128MB
  -c maintenance_work_mem=2GB
  -c effective_cache_size=6GB
  -c max_wal_size=16GB
  -c wal_buffers=64MB
  -c checkpoint_timeout=15min
  -c checkpoint_completion_target=0.9
  -c wal_compression=on
  -c synchronous_commit=off
  -c fsync=off
  -c jit=off
```

`synchronous_commit=off` and `fsync=off` significantly speed up bulk ingestion but reduce crash durability. Safe for ephemeral dev environments; turn them back on for production data.

Use a **named volume** for Postgres data on macOS — it stores data inside the Docker VM's native filesystem (fast), instead of a macOS bind mount which goes through VirtioFS (slow for a DB):

```yaml
volumes:
  - pgdata:/var/lib/postgresql/data   # fast (named volume, Docker VM ext4)
  # NOT: - ./data:/var/lib/postgresql/data  ← slow on macOS
```

---

## Resource Guidelines

| Component | Minimum | Recommended (large datasets) |
|-----------|---------|------------------------------|
| Pipeline RAM | 4 GB | 16–20 GB |
| Pipeline CPUs | 2 | 4–6 (parallel file processing) |
| Postgres RAM | 2 GB | 8 GB |
| Postgres CPUs | 2 | 4–5 |
| Postgres `shared_buffers` | 256 MB | 3 GB |
| Postgres `work_mem` | 32 MB | 128 MB |

The pipeline container is the main memory consumer for large raster or vector datasets. Elevation processing (raster tiles) and pleasant-bicycling (4M-row CSV) each benefit from the higher RAM allocation.
