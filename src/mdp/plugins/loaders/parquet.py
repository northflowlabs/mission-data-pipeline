"""Parquet loader: persists telemetry parameters to Apache Parquet files.

One Parquet file is written per parameter, named ``<output_dir>/<param>.parquet``.
Schema uses ``pyarrow`` for efficient columnar storage with Snappy compression.
"""

from __future__ import annotations

import json
from pathlib import Path

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, Field

from mdp.core.base import Loader
from mdp.core.registry import registry
from mdp.models.dataset import TelemetryDataset, new_wide_times

_UNITS_KEY = b"mdp_units"


class ParquetLoaderConfig(BaseModel):
    output_dir: Path = Field(description="Directory where Parquet files will be written")
    compression: str = "snappy"
    wide_format: bool = Field(
        default=False,
        description="If True, write a single wide-format file instead of one per parameter",
    )
    partition_by_apid: bool = Field(
        default=False,
        description="Partition output directories by APID value",
    )
    overwrite: bool = True


def _same_units(earlier: dict[str, str], later: dict[str, str]) -> dict[str, str]:
    for name, unit in later.items():
        if earlier.get(name, unit) != unit:
            raise ValueError(f"Parameter '{name}' mixes units: {earlier[name]} and {unit}")
    return {**earlier, **later}


@registry.loader("parquet")
class ParquetLoader(Loader[ParquetLoaderConfig]):
    """Write TelemetryDataset parameters to Parquet files."""

    config_class = ParquetLoaderConfig

    def __init__(self, config: ParquetLoaderConfig) -> None:
        super().__init__(config)
        self._written: set[Path] = set()
        self._wide_times: dict[str, set[float]] = {}

    def begin_run(self) -> None:
        self._written.clear()
        self._wide_times.clear()

    def setup(self) -> None:
        self.config.output_dir.mkdir(parents=True, exist_ok=True)

    def load(self, dataset: TelemetryDataset) -> None:
        if self.config.wide_format:
            self._write_wide(dataset)
        else:
            self._write_per_parameter(dataset)

    def _write_per_parameter(self, dataset: TelemetryDataset) -> None:
        for name in dataset.parameter_names():
            df = dataset.to_dataframe(name)
            if df.empty:
                continue

            if self.config.partition_by_apid and "apid" in df.columns:
                for apid_val, group_df in df.groupby("apid"):
                    subdir = self.config.output_dir / f"apid={apid_val:04X}"
                    subdir.mkdir(parents=True, exist_ok=True)
                    self._write_df(group_df, subdir / f"{name}.parquet")
            else:
                self._write_df(df, self.config.output_dir / f"{name}.parquet")

    def _write_wide(self, dataset: TelemetryDataset) -> None:
        df = dataset.to_wide_dataframe()
        if df.empty:
            return
        new_wide_times(df, self._wide_times)
        self._write_df(
            df.reset_index().rename(columns={"index": "time_tai"}),
            self.config.output_dir / "telemetry.parquet",
            units=df.attrs.get("units"),
        )

    def _write_df(self, df: pd.DataFrame, path: Path, units: dict[str, str] | None = None) -> None:
        df = df.copy()
        df.attrs = {}
        table = pa.Table.from_pandas(df, preserve_index=False)
        # overwrite replaces earlier runs; batches of the same run accumulate.
        keep_existing = path in self._written or not self.config.overwrite
        if path.exists() and keep_existing:
            existing = pq.read_table(path)
            # An integer column can meet floats in a later batch; permissive widens it.
            table = pa.concat_tables([existing, table], promote_options="permissive")
            # The stored pandas types describe the first batch, not the widened column
            kept = {k: v for k, v in (table.schema.metadata or {}).items() if k != b"pandas"}
            table = table.replace_schema_metadata(kept)
            old_units = (existing.schema.metadata or {}).get(_UNITS_KEY)
            if old_units:
                units = _same_units(json.loads(old_units), units or {})
        if units:
            metadata = {**(table.schema.metadata or {}), _UNITS_KEY: json.dumps(units)}
            table = table.replace_schema_metadata(metadata)
        pq.write_table(table, path, compression=self.config.compression)
        self._written.add(path)
