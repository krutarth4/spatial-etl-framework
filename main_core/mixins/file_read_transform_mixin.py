"""File reading, format auto-detection, and the transform pipeline.

Reads from self:  self.data_source_config, self.logger
Calls into:       LifecycleHooksMixin  (before/after_filter_pipeline,
                                        pre/post_filter_processing, source_filter)
"""
from pathlib import Path

from handlers.file_handler import FileHandler
from main_core.safe_class import safe_class


@safe_class
class FileReadTransformMixin:
    """Parses raw files into record dicts and applies the filter pipeline."""

    # ── Public entry points ───────────────────────────────────────────────

    # Overridable: mappers may override this method to parse custom file formats
    def read_file_content(self, path):
        """Parse a single file and return records.

        Default: auto-detects format by extension and delegates to _auto_read().
        Override to handle custom formats (zip, proprietary binary, etc.).

        Returns:
            list[dict]  — records to persist
            NotImplemented — extension not recognised; FileHandler fallback will run
        """
        return self._auto_read(path)

    def transform(self, path):
        """Read a file and run the full filter pipeline.

        Full-list path (default — when read_file_content returns a list):
            read_files()              → raw records (list)
            before_filter_pipeline()  → hook
            pre_filter_processing()   → hook (e.g. build KDTree)
            source_filter()           → reshape / flatten (Overridable)
            post_filter_processing()  → hook (e.g. save to file)
            after_filter_pipeline()   → hook
            returns list

        Chunked path (when read_file_content returns a generator / iterator):
            Returns a generator that yields source_filter(chunk) per batch.
            Hooks that require the full dataset are skipped in this path;
            each chunk is freed by GC after load() processes it.
        """
        result = self.read_files(path)
        if not isinstance(result, list):
            # Return a generator — transform() itself is NOT a generator function
            # so the caller receives the generator object (not an empty generator).
            return self._iter_transform_chunks(result)
        # Full-list path: unchanged behaviour
        self.logger.info(f"result contains currently {len(result)}")
        self.before_filter_pipeline(result, path)
        self.pre_filter_processing(result)
        result = self.source_filter(result)
        self.post_filter_processing(result)
        self.after_filter_pipeline(result, path)
        return result

    def _iter_transform_chunks(self, chunks):
        """Generator that applies source_filter to each chunk from a chunked reader."""
        for chunk in chunks:
            yield self.source_filter(chunk)

    def read_files(self, path: Path | str):
        """Wrap FileHandler.read_local_file() and normalise output.

        Returns a flat list[dict] for standard reads, or passes through a
        generator/iterator unchanged when read_file_content() returns one
        (chunked streaming mode).
        """
        try:
            path = Path(path)
            file_handler = FileHandler(path.parent)
            res = file_handler.read_local_file(path.name, self.read_file_content)
            # Generators/iterators: pass through for chunked processing
            if hasattr(res, "__next__"):
                return res
            result = []
            if isinstance(res, list):
                result.extend(res)
            elif isinstance(res, dict):
                result.append(res)
            elif isinstance(res, str):
                result.append(res)
            else:
                self.logger.error(
                    f"File {path} not readable or the format specified by "
                    f"read_file_content is not correct"
                )
            return result
        except Exception as e:
            self.logger.error(f"Error occurred while reading the files {e}")
            return []

    # ── Internal helpers ──────────────────────────────────────────────────

    def _resolve_extension(self, path: str) -> str:
        """Return the effective file extension for format detection.

        Prefers source.response_type from config (e.g. 'json.gz' → 'gz') over
        the file suffix, so compressed or server-renamed files are handled correctly.
        """
        response_type = None
        try:
            response_type = self.data_source_config.source.response_type
        except AttributeError:
            self.logger.warning(
                "[_resolve_extension] data_source_config.source.response_type not accessible; "
                "falling back to file suffix"
            )
        if response_type:
            ext = response_type.strip().lower().split(".")[-1]
            self.logger.debug(
                f"[_resolve_extension] response_type='{response_type}' → ext='{ext}' path={path}"
            )
            return ext
        ext = Path(path).suffix.lstrip(".").lower()
        self.logger.debug(
            f"[_resolve_extension] No response_type; using file suffix ext='{ext}' path={path}"
        )
        return ext

    def _auto_read(self, path: str):
        """Auto-detect file format by extension and return records.

        Supported natively: .gpkg / .shp / .geojson, .parquet, .csv / .tsv,
                            .xlsx / .xls, .json
        Deferred to FileHandler: .gz, .zip, .xml, .pbf  (returns NotImplemented)
        """
        ext = self._resolve_extension(path)
        reader_cfg = getattr(getattr(self.data_source_config, "source", None), "reader", None)
        self.logger.info(
            f"[_auto_read] Reading path={path} | ext='{ext}' | reader_cfg={reader_cfg}"
        )

        if ext in ("gpkg", "shp", "geojson"):
            import geopandas as gpd
            engine = reader_cfg.engine if (reader_cfg and reader_cfg.engine) else "pyogrio"
            self.logger.debug(f"[_auto_read] Spatial read: engine='{engine}' path={path}")
            try:
                gdf = gpd.read_file(path, engine=engine)
            except Exception as e:
                self.logger.error(
                    f"[_auto_read] geopandas.read_file failed for {path}: {e}", exc_info=True
                )
                return NotImplemented
            if gdf.empty:
                self.logger.warning(
                    f"[_auto_read] geopandas returned empty GeoDataFrame for {path}"
                )
                return []
            self.logger.debug(
                f"[_auto_read] Loaded {len(gdf)} features from {path} | CRS={gdf.crs}"
            )
            if gdf.crs is None:
                self.logger.warning(f"[_auto_read] No CRS defined in {path}; skipping reprojection")
            elif reader_cfg and reader_cfg.target_crs:
                src_epsg = gdf.crs.to_epsg()
                if src_epsg != reader_cfg.target_crs:
                    self.logger.info(
                        f"[_auto_read] Reprojecting EPSG:{src_epsg} → EPSG:{reader_cfg.target_crs}"
                    )
                    gdf = gdf.to_crs(reader_cfg.target_crs)
                else:
                    self.logger.debug(
                        f"[_auto_read] CRS already EPSG:{src_epsg}, no reprojection needed"
                    )
            gdf = gdf.drop(columns=["geometry"], errors="ignore")
            records = gdf.to_dict(orient="records")
            self.logger.info(f"[_auto_read] Returning {len(records)} records from {path}")
            return records

        if ext == "parquet":
            import pandas as pd
            self.logger.debug(f"[_auto_read] Parquet read: {path}")
            try:
                chunk_size = reader_cfg.chunk_size if (reader_cfg and reader_cfg.chunk_size) else None
                if chunk_size:
                    self.logger.info(f"[_auto_read] Parquet streaming: chunk_size={chunk_size} path={path}")
                    return self._iter_parquet_chunks(path, chunk_size)
                df = pd.read_parquet(path)
                self.logger.info(
                    f"[_auto_read] Loaded parquet {path}: {len(df)} rows, "
                    f"columns={list(df.columns)}"
                )
                return df.to_dict(orient="records")
            except Exception as e:
                self.logger.error(
                    f"[_auto_read] pd.read_parquet failed for {path}: {e}", exc_info=True
                )
                return NotImplemented

        if ext in ("csv", "tsv"):
            import pandas as pd
            self.logger.debug(f"[_auto_read] CSV/TSV read: {path}")
            try:
                chunk_size = reader_cfg.chunk_size if (reader_cfg and reader_cfg.chunk_size) else None
                if chunk_size:
                    self.logger.info(f"[_auto_read] CSV streaming: chunk_size={chunk_size} path={path}")
                    return self._iter_csv_chunks(path, chunk_size)
                df = pd.read_csv(path)
                self.logger.info(
                    f"[_auto_read] Loaded csv {path}: {len(df)} rows, "
                    f"columns={list(df.columns)}"
                )
                return df.to_dict(orient="records")
            except Exception as e:
                self.logger.error(
                    f"[_auto_read] pd.read_csv failed for {path}: {e}", exc_info=True
                )
                return NotImplemented

        if ext in ("xlsx", "xls"):
            import pandas as pd
            self.logger.debug(f"[_auto_read] Excel read: {path}")
            try:
                df = pd.read_excel(path)
                self.logger.info(
                    f"[_auto_read] Loaded excel {path}: {len(df)} rows, "
                    f"columns={list(df.columns)}"
                )
                return df.to_dict(orient="records")
            except Exception as e:
                self.logger.error(
                    f"[_auto_read] pd.read_excel failed for {path}: {e}", exc_info=True
                )
                return NotImplemented

        if ext == "json":
            self.logger.debug(f"[_auto_read] JSON read: {path}")
            try:
                try:
                    import orjson
                    with open(path, "rb") as f:
                        result = orjson.loads(f.read())
                except ImportError:
                    self.logger.debug("[_auto_read] orjson not available, falling back to stdlib json")
                    import json
                    with open(path, "r", encoding="utf-8") as f:
                        result = json.load(f)
                record_count = len(result) if isinstance(result, list) else 1
                self.logger.info(
                    f"[_auto_read] Loaded json {path}: type={type(result).__name__}, "
                    f"top-level count={record_count}"
                )
                return result
            except Exception as e:
                self.logger.error(
                    f"[_auto_read] JSON parse failed for {path}: {e}", exc_info=True
                )
                return NotImplemented

        # gz, zip, xml, pbf — defer to FileHandler._read() fallback
        self.logger.debug(
            f"[_auto_read] No handler for ext='{ext}', deferring to FileHandler for {path}"
        )
        return NotImplemented

    @staticmethod
    def _iter_csv_chunks(path: str, chunk_size: int):
        """Yield list[dict] batches from a CSV file without loading it fully into RAM."""
        import pandas as pd
        reader = pd.read_csv(path, chunksize=chunk_size)
        for chunk_df in reader:
            yield chunk_df.to_dict(orient="records")

    @staticmethod
    def _iter_parquet_chunks(path: str, chunk_size: int):
        """Yield list[dict] batches from a Parquet file without loading it fully into RAM."""
        try:
            import pyarrow.parquet as pq
            pf = pq.ParquetFile(path)
            for batch in pf.iter_batches(batch_size=chunk_size):
                yield batch.to_pydict() if False else batch.to_pandas().to_dict(orient="records")
        except ImportError:
            # pyarrow not available: fall back to pandas full load
            import pandas as pd
            df = pd.read_parquet(path)
            for i in range(0, len(df), chunk_size):
                yield df.iloc[i:i + chunk_size].to_dict(orient="records")
