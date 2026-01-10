from main_core.data_source_abc_impl import DataSourceABCImpl
import geopandas as gpd

class TreeMapper(DataSourceABCImpl):

    def read_file_content(self, path):
        """
        Read a GeoPackage file and return a list of feature dicts.
        Each item contains properties + geometry.
        """
        # Read all layers (usually one for WFS exports)
        layers = gpd.list_layers(path)

        if layers.empty:
            return []

        records = []

        for layer_name in layers["name"]:
            gdf = gpd.read_file(path, layer=layer_name)

            # Convert GeoDataFrame to list of dicts
            for _, row in gdf.iterrows():
                record = row.drop(labels=["geometry"]).to_dict()

                # Add geometry as GeoJSON-like dict (safe & portable)
                geom = row.geometry
                record["geometry"] = geom.__geo_interface__ if geom is not None else None

                records.append(record)
        print(f"Total records: {len(records)}")
        return records