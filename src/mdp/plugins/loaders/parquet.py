"""Parquet loader — persists telemetry parameters to Apache Parquet files.

One Parquet file is written per parameter, named ``<output_dir>/<param>.parquet``.
Schema uses ``pyarrow`` for efficient columnar storage with Snappy compression.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated

import pandas as pd
import pyarrow as pa
import pyarrow.parquet as pq
from pydantic import BaseModel, Field

from mdp.core.base import Loader
from mdp.core.registry import registry
from mdp.models.dataset import TelemetryDataset


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


@registry.loader("parquet")
class ParquetLoader(Loader[ParquetLoaderConfig]):
    """Write TelemetryDataset parameters to Parquet files."""

    config_class = ParquetLoaderConfig

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
        self._write_df(df.reset_index().rename(columns={"index": "time_tai"}),
                       self.config.output_dir / "telemetry.parquet")

    def _write_df(self, df: pd.DataFrame, path: Path) -> None:
        table = pa.Table.from_pandas(df, preserve_index=False)
        if path.exists() and not self.config.overwrite:
            existing = pq.read_table(path)
            table = pa.concat_tables([existing, table])
        pq.write_table(table, path, compression=self.config.compression)
