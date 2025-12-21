# Docker container for postgis extension

* TODO: add the hstore, postgis extension to docker container at start

run the conatiner 
```
docker run --name postgres -e POSTGRES_PASSWORD=admin123 -e POSTGRES_USER=postgres -p 5432:5432 -d postgres

```
install officiiaal postgres container and install postgis or any other plugin oon top of it too make it more light and no need to have unwanted plugins installed 

```aiignore
# check postgres version 
# make sure to have a bash activated inside the container
docker exec -it my_postgres_container bash
psql -U postgres -c "SELECT version();"
---------------------------
apt-get update
apt-get install -y postgis postgresql-{postgres version}-postgis-3
```

TODO:
* at initial startup create the database and schema as default with the extensions installed


# Configure db driver 

currently not supported psycopg2 so we install the version 3 available in binary for the python version 3.13

` pip install "psycopg[binary]"`

for the url mention "postgresql+psycopg" if still want to use psycopg2 install all the required packages and change the url
to "postgresql+psycopg2"


# For osm2pgrouting - main base table ways and ways_node

` osm2pgrouting -f ./raw/map_extract.osm -d osm_bbox_berlin -U postgres -W admin123 -p 5433 -c mapconfig.xml --prefix routing --tags --addnodes --schema pgrouting`



# for the preperation osm2pgsql 

` osm2pgsql -c -d osm_bbox_berlin -p berlin --number-processes=4 -U postgres -P 5433 -W -H localhost ./raw/map_extract.osm -r 'osm' -S default.style --latlong`

File can be changed to store certain tags and remove certain tags 

NOTE: Alternatively use Imposum 3 as it is way faster and effecient 
![Screenshot 2025-10-23 at 22.41.15.png](../../../../../../var/folders/rl/g_q25sf94wg5dyb4py3cvd280000gn/T/TemporaryItems/NSIRD_screencaptureui_ATwRQ3/Screenshot%202025-10-23%20at%2022.41.15.png)

[https://github.com/makinacorpus/ImpOsm2pgRouting](https://github.com/makinacorpus/ImpOsm2pgRouting) look for benchmark between different routing machines, valhala, graphopper and other routing machines



# For osmium CLi tool -> to extract a small area from berlin osm file

`osmium extract -b 13.30760,52.50644,13.33860,52.51802 --strategy=complete_ways -o ernst_extract.osm berlin.osm`