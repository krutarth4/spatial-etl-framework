# Example: Elevation Grid Links (Mapper + Config)

**Mapper:** [`data_mappers/elevationGridMapper.py`](../data_mappers/elevationGridMapper.py)  
**Config:** [`data_source_configs/elevation_grids_links.yaml`](../data_source_configs/elevation_grids_links.yaml)

This datasource fetches a Berlin open-data Atom feed (XML), parses the download URLs for DEM tiles out of it, and saves them to a JSON file that the `elevation` datasource then consumes as its fetch list. It is the first half of a two-datasource pipeline and demonstrates:

- Using `source_filter()` to parse XML instead of returning DB records
- `after_filter_hook` to save filter output to a file (not to the database)
- Disabling storage and mapping (`mapping.enable: false`, `storage.persistent: false`) when a datasource's only job is to produce a file for another datasource

---

## Mapper

```python
class ElevationGridMapper(DataSourceABCImpl):

    def source_filter(self, data: list) -> list:
        return self.extract_entry_links(data)

    @staticmethod
    def extract_entry_links(xml_path):
        root = ET.fromstringlist(xml_path)   # data is a list of XML string chunks

        ns = {"atom": "http://www.w3.org/2005/Atom"}
        entry = root.find("atom:entry", ns)
        if entry is None:
            return []

        # Extract all <link href="..."> that end in .zip
        links = [
            link.attrib["href"]
            for link in entry.findall("atom:link", ns)
            if "href" in link.attrib and link.attrib["href"].endswith(".zip")
        ]
        return links
```

**`source_filter` receives XML chunks, not a dict**  
The framework passes the raw file content to `source_filter`. For `response_type: xml`, this arrives as a list of string chunks. `ET.fromstringlist(xml_path)` parses them into an ElementTree — note that the parameter name `xml_path` is misleading; it's actually the list of content chunks passed by the framework.

**Returns a list of URLs, not a list of dicts**  
`source_filter` normally returns records to insert into the staging table. Here it returns a list of URL strings. Because `storage.persistent: false` and `before_load_hook.enable: false`, nothing is inserted into the DB. The `after_filter_hook` in the config catches the return value and saves it to a JSON file instead.

---

## Config

```yaml
name: elevation_grids_links
enable: true
class_name: elevationGrid
data_type: dynamic

source:
  mode: single
  fetch: http
  url: https://gdi.berlin.de/data/dgm1/atom/0.atom
  response_type: xml
  save_local: true
  destination: tmp/elevation_grid/elevation.xml
  check_metadata:
    enable: true
    keys: ["last_modified"]
  header:
    Accept: "application/atom+xml"
```

`response_type: xml` and `Accept: application/atom+xml` tell the framework to download the feed as XML and pass content to `source_filter`.

---

### after_filter_hook — save URLs to a file

```yaml
after_filter_hook:
  save: true
  destination: data/grid/elevation_grid_links.json
```

After `source_filter` returns the list of URLs, the `after_filter_hook` serialises the list to `elevation_grid_links.json`. The `elevation` datasource reads this file via `multi_fetch.urls.input`. This is the handoff between the two datasources.

The `destination` path must match the `urls.input` path in `elevation.yaml`:

```yaml
# elevation.yaml
multi_fetch:
  strategy: explicit_url_list
  urls:
    input: data/grid/elevation_grid_links.json   # ← same path
```

---

### No storage, no mapping

```yaml
mapping:
  enable: false
storage:
  persistent: false
before_load_hook:
  enable: false
post-database-processing:
  enable: false
```

This datasource produces a file, not database rows. Disabling `mapping`, `persistent`, and `before_load_hook` prevents the framework from creating staging tables or attempting to insert the URL list into the database.

---

### Schedule

```yaml
job:
  trigger:
    type:
      name: interval
      config:
        hours: 168   # 7 days
```

Matches the `elevation` datasource's `expires_after: 168h`. The URL list is refreshed weekly; new DEM tiles are published at the same cadence.

---

## How the two datasources connect

```
Atom feed (XML)
        │
        ▼  source_filter() — parses XML → extracts .zip URLs
        │
after_filter_hook saves → data/grid/elevation_grid_links.json
                                        │
                         elevation.yaml reads it
                                        │
                                        ▼
                         multi_fetch downloads each .zip URL
                                        │
                                        ▼
                              ElevationMapper processes each zip
```

---

## Key patterns to reuse

| Pattern | Where | When to use |
|---------|-------|-------------|
| XML parsing in `source_filter` | `extract_entry_links` | Source is XML; you need to extract specific elements |
| `after_filter_hook` to write a file | `destination: data/grid/…json` | This datasource's output is consumed by another via a file, not the DB |
| Disable storage when no DB insert | `storage.persistent: false` + `mapping.enable: false` | Datasource is a helper/discovery step, not an ETL step |
| Two-datasource producer-consumer chain | `elevation_grids_links` → `elevation` | First datasource discovers what to download; second downloads it |
