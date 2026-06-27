# Getting Started

This guide gets you from zero to a running pipeline with one datasource enriching a road graph. It takes about 15 minutes.

---

## Prerequisites

- Python 3.11+
- PostgreSQL 16 with PostGIS 3.4 — or Docker (easiest path, shown below)
- Git

---

## 1. Clone and install

```bash
git clone https://github.com/your-org/spatial-etl-framework.git
cd spatial-etl-framework

python3 -m venv .venv
source .venv/bin/activate       # Windows: .venv\Scripts\activate
pip install -r requirements.txt
```

---

## 2. Start a PostGIS database

The fastest way is Docker:

```bash
docker run --name postgis \
  -e POSTGRES_USER=postgres \
  -e POSTGRES_PASSWORD=admin123 \
  -e POSTGRES_DB=mydb \
  -p 5432:5432 \
  -d postgis/postgis:16-3.4
```

Wait a few seconds for it to be ready:

```bash
docker exec postgis pg_isready -U postgres -d mydb
# postgis:5432 - accepting connections
```

If you already have a local PostgreSQL instance with PostGIS, skip this step.

---

## 3. Configure the database connection

```bash
cp .env.example .env
```

Edit `.env`:

```dotenv
DB_HOST=localhost
DB_PORT=5432
DB_NAME=mydb
DB_USER=postgres
DB_PASSWORD=admin123
```

---

## 4. Configure the pipeline schema

Open `config.yaml` and set `base_schema` to the PostgreSQL schema you want the pipeline to create tables in. The default is `exp_null` — you can keep it or change it:

```yaml
env_variables:
  base_schema: &db_schema myschema   # change from exp_null if you prefer
```

The framework creates this schema automatically on first run.

---

## 5. About `ways_base` (road graph)

All enrichment datasources join their data onto a table called `ways_base` — a PostGIS table of road segments where each row is one edge in the road graph. The framework creates this table empty at startup.

**For a standalone ETL run without routing:**

You can populate `ways_base` directly with any road geometry you have:

```sql
-- minimal schema (extend as needed)
CREATE TABLE myschema.ways_base (
    id              BIGSERIAL PRIMARY KEY,
    way_id          BIGINT,
    way_link_index  INTEGER,
    geometry_25833  geometry(LineString, 25833),
    geometry        geometry(LineString, 4326)
);

-- insert from an existing OSM table (example)
INSERT INTO myschema.ways_base (way_id, way_link_index, geometry_25833, geometry)
SELECT osm_id, 0, ST_Transform(way, 25833), way
FROM planet_osm_roads
WHERE highway IS NOT NULL;
```

If you are using the full MDP stack (Java router + osm2pgrouting), the router populates `ways_base` automatically via the `graph` datasource and CommService. See [docs/example-graph-mapper.md](example-graph-mapper.md).

**To run without a road graph at all** (ETL only, no spatial join): set `mapping.enable: false` in your datasource config.

---

## 6. Enable a datasource

The pipeline discovers every `*.yaml` file in `data_source_configs/` automatically. Enable the weather station datasource as a smoke test:

```bash
# it ships disabled — enable it
```

Open `data_source_configs/weather_station_bright_sky.yaml` and set `enable: true`. This datasource fetches DWD station data from the public Bright Sky API — no API key needed.

---

## 7. Enable periodic scheduling

`config.yaml` ships with the scheduler off:

```yaml
scheduler:
  enable: false
```

With the scheduler off, datasources run once at startup. That is fine for a first run. To enable cron/interval re-scheduling, set it to `true`.

---

## 8. Run the pipeline

```bash
python3 run.py
```

You should see log output like:

```
INFO  InitScheduler   — Scheduler disabled — not creating scheduler instance
INFO  CoreConfig      — Loaded 1 datasource configs
INFO  WeatherStationMapper — Starting run
INFO  WeatherStationMapper — Fetched 8 records
INFO  WeatherStationMapper — Inserted into staging: 8 rows
INFO  WeatherStationMapper — Enrichment complete
INFO  WeatherStationMapper — Mapping complete
```

The debug API is now available at [http://localhost:8000/docs](http://localhost:8000/docs).

---

## 9. Verify the data

```bash
psql -U postgres -d mydb \
  -c "SELECT COUNT(*) FROM exp_null.dwd_station_locations_staging;"

psql -U postgres -d mydb \
  -c "SELECT COUNT(*) FROM exp_null.dwd_station_locations_enrichment;"
```

Or use the debug API:

```
GET http://localhost:8000/debug/datasources
GET http://localhost:8000/debug/datasources/weather-station
```

---

## 10. Add your own datasource

Now that the pipeline is running, add your own data source:

1. Create `data_source_configs/my_data.yaml` — declare the URL, schedule, and mapping strategy
2. Optionally add `data_mappers/myDataMapper.py` — only needed for non-standard file formats or custom SQL
3. Save the file — the watcher detects the change within `poll_seconds` and automatically restarts with `--only <your_datasource>` so only that datasource re-runs (not the whole pipeline). Adding or editing a mapper `.py` triggers a full restart instead.

Full guide: [configure-data-source-step-by-step.md](configure-data-source-step-by-step.md)

Real examples with annotated code: [example-tree-mapper.md](example-tree-mapper.md), [example-air-quality-mapper.md](example-air-quality-mapper.md), [example-elevation-mapper.md](example-elevation-mapper.md)

---

## Troubleshooting

| Symptom | Fix |
|---------|-----|
| `connection refused` on startup | PostGIS not running — check `docker ps` or your local Postgres |
| `schema "exp_null" does not exist` | The framework auto-creates the schema; check DB_USER has CREATE SCHEMA permission |
| Staging table empty after run | Check `source_filter()` or `read_file_content()` — use `GET /debug/datasources/{name}` to inspect |
| `ways_base` has 0 rows, mapping skipped | Populate `ways_base` before enabling `mapping.enable: true` (see step 5 above) |
| Job fires once and never again | `scheduler.enable: false` in `config.yaml` — set to `true` for periodic scheduling |
| Wrong timezone on job fire times | Edit `scheduler.timezone` in `config.yaml` (e.g. `"UTC"`, `"America/New_York"`) |
| `geometry_25833` column not found | Your `ways_base` uses a different CRS/column — update `mapping_defaults.config.base_geometry_column` in `config.yaml` |
