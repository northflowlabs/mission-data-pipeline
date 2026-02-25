"""CSV loader — writes telemetry parameters to CSV files.

One file per parameter, or a single wide-format CSV with all parameters
merged on TAI time.
"""

from __future__ import annotations

from pathlib import Path

from pydantic import BaseModel, Field

from mdp.core.base import Loader
from mdp.core.registry import registry
from mdp.models.dataset import TelemetryDataset


class CsvLoaderConfig(BaseModel):
    output_dir: Path = Field(description="Directory where CSV files will be written")
    wide_format: bool = False
    delimiter: str = ","
    float_format: str = "%.9f"
    overwrite: bool = True


@registry.loader("csv")
class CsvLoader(Loader[CsvLoaderConfig]):
    """Write TelemetryDataset parameters to CSV files."""

    config_class = CsvLoaderConfig

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
            mode = "w" if self.config.overwrite or not out_path.exists() else "a"
            header = mode == "w"
            df.to_csv(
                out_path,
                mode=mode,
                header=header,
                index=False,
                sep=self.config.delimiter,
                float_format=self.config.float_format,
            )

    def _write_wide(self, dataset: TelemetryDataset) -> None:
        df = dataset.to_wide_dataframe()
        if df.empty:
            return
        out_path = self.config.output_dir / "telemetry.csv"
        df.reset_index().rename(columns={"index": "time_tai"}).to_csv(
            out_path,
            index=False,
            sep=self.config.delimiter,
            float_format=self.config.float_format,
        )
