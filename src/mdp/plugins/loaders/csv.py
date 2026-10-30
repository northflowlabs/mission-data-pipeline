"""CSV loader: writes telemetry parameters to CSV files.

One file per parameter, or a single wide-format CSV with all parameters
merged on TAI time.
"""

from __future__ import annotations

from pathlib import Path

import pandas as pd
from pydantic import BaseModel, Field

from mdp.core.base import Loader
from mdp.core.registry import registry
from mdp.models.dataset import TelemetryDataset


class CsvLoaderConfig(BaseModel):
    output_dir: Path = Field(description="Directory where CSV files will be written")
    wide_format: bool = False
    delimiter: str = ","
    float_format: str = "%.17g"  # keeps every float64 exactly, at any magnitude
    overwrite: bool = True


@registry.loader("csv")
class CsvLoader(Loader[CsvLoaderConfig]):
    """Write TelemetryDataset parameters to CSV files."""

    config_class = CsvLoaderConfig

    def __init__(self, config: CsvLoaderConfig) -> None:
        super().__init__(config)
        self._written: set[Path] = set()

    def begin_run(self) -> None:
        self._written.clear()

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
            out_path = self.config.output_dir / f"{name}.csv"
            # overwrite replaces earlier runs; batches of the same run accumulate.
            keep_existing = out_path in self._written or not self.config.overwrite
            mode = "a" if keep_existing and out_path.exists() else "w"
            df.to_csv(
                out_path,
                mode=mode,
                header=mode == "w",
                index=False,
                sep=self.config.delimiter,
                float_format=self.config.float_format,
            )
            self._written.add(out_path)

    def _write_wide(self, dataset: TelemetryDataset) -> None:
        df = dataset.to_wide_dataframe()
        if df.empty:
            return
        out_path = self.config.output_dir / "telemetry.csv"
        units = df.attrs.get("units", {})
        wide = df.reset_index().rename(columns={"index": "time_tai"})
        wide = wide.rename(columns={name: f"{name} [{unit}]" for name, unit in units.items()})
        keep_existing = out_path in self._written or not self.config.overwrite
        if keep_existing and out_path.exists():
            existing = pd.read_csv(out_path, sep=self.config.delimiter)
            wide = pd.concat([existing, wide], ignore_index=True)
        wide.to_csv(
            out_path,
            index=False,
            sep=self.config.delimiter,
            float_format=self.config.float_format,
        )
        self._written.add(out_path)
