# JSONPath Query Conventions

The line:

``` yaml
query: "$.features[*].properties.urls.pbf"
```

is written in **JSONPath** convention. JSONPath is to JSON what XPath is
to XML → a mini "query language" to navigate nested structures.

------------------------------------------------------------------------

## 🔹 JSONPath Building Blocks

### 1. `$` → root object

-   Always starts at the very top.\
-   Equivalent to "the whole JSON document".

``` json
{ "foo": 123 }
```

-   `$` = the whole object
-   `$.foo` = 123

------------------------------------------------------------------------

### 2. `.` → child operator

-   Dot-notation to access an object's property.

``` json
{ "a": { "b": 42 } }
```

-   `$.a` = `{ "b": 42 }`
-   `$.a.b` = `42`

------------------------------------------------------------------------

### 3. `[*]` → wildcard array

-   Selects **all elements** of an array.

``` json
{ "nums": [10, 20, 30] }
```

-   `$.nums[*]` = `[10, 20, 30]`

------------------------------------------------------------------------

### 4. `..` → recursive descent

-   Search at **any depth** (like `**` in globbing).

``` json
{ "foo": { "bar": { "baz": 99 } } }
```

-   `$..baz` = `99`

------------------------------------------------------------------------

### 5. `[]` → array index / slice / filter

-   `[0]` → first item\
-   `[1:3]` → slice\
-   `[?(@.key=="value")]` → filter

Example:

``` json
{
  "users": [
    {"id": 1, "role": "admin"},
    {"id": 2, "role": "user"}
  ]
}
```

-   `$.users[0].id` = `1`\
-   `$.users[*].role` = `["admin", "user"]`\
-   `$.users[?(@.role=="admin")].id` = `[1]`

------------------------------------------------------------------------

### 6. `@` → current object

Used inside filters.

------------------------------------------------------------------------

## 🔹 Your Query Explained

``` yaml
query: "$.features[*].properties.urls.pbf"
```

Applied to your GeoJSON:

``` json
{
  "features": [
    {
      "properties": {
        "urls": {
          "pbf": "https://download.geofabrik.de/asia/afghanistan-latest.osm.pbf"
        }
      }
    },
    {
      "properties": {
        "urls": {
          "pbf": "https://download.geofabrik.de/asia/india-latest.osm.pbf"
        }
      }
    }
  ]
}
```

Step by step:

1.  `$` → start at root object\
2.  `.features` → go into the `features` array\
3.  `[*]` → take **every element** in that array\
4.  `.properties.urls.pbf` → drill down into `properties → urls → pbf`

👉 Result = list of all PBF download URLs:

``` json
[
  "https://download.geofabrik.de/asia/afghanistan-latest.osm.pbf",
  "https://download.geofabrik.de/asia/india-latest.osm.pbf"
]
```

------------------------------------------------------------------------

## 🔑 Summary of JSONPath Conventions

-   `$` = root\
-   `.` = child\
-   `[*]` = all items in array\
-   `[0]`, `[1:3]` = array index/slice\
-   `?()` = filter with condition\
-   `@` = current object\
-   `..` = recursive descent (all levels)
