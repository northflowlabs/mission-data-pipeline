"""CSV telemetry extractor — reads pre-processed telemetry CSV files.

Expected columns (configurable):
  - ``time``       — TAI seconds since J2000 (float)
  - ``apid``       — Application Process Identifier (int)
  - ``seq_count``  — packet sequence count (int)
  - ``<param_N>``  — any number of parameter columns

This extractor does not parse raw binary — it reads structured CSV exports
as commonly produced by ground segment SCICON or MCS tools.
"""

from __future__ import annotations

from pathlib import Path
from typing import Annotated, Iterator

import pandas as pd
from pydantic import BaseModel, Field

from mdp.core.base import Extractor
from mdp.core.registry import registry
from mdp.models.dataset import TelemetryDataset
from mdp.models.parameter import EngineeringParameter


class CsvExtractorConfig(BaseModel):
    path: Path = Field(description="Path to the CSV telemetry file")
    batch_size: Annotated[int, Field(gt=0)] = 1000
    time_column: str = "time"
    apid_column: str = "apid"
    seq_count_column: str = "seq_count"
    parameter_columns: list[str] | None = Field(
        default=None,
        description="Explicit list of parameter columns. If None, all remaining columns are used.",
    )
    delimiter: str = ","
    source_id: str | None = None


@registry.extractor("csv")
class CsvTelemetryExtractor(Extractor[CsvExtractorConfig]):
    """Read engineering-unit telemetry parameters from a CSV file in batches."""

    config_class = CsvExtractorConfig

    def extract(self) -> Iterator[TelemetryDataset]:
        path = self.config.path
        if not path.exists():
            raise FileNotFoundError(f"CSV file not found: {path}")

        reader = pd.read_csv(
            path,
            sep=self.config.delimiter,
            chunksize=self.config.batch_size,
        )

        for chunk in reader:
            yield self._chunk_to_dataset(chunk)

    def _chunk_to_dataset(self, chunk: pd.DataFrame) -> TelemetryDataset:
        cfg = self.config
        required = {cfg.time_column, cfg.apid_column, cfg.seq_count_column}
        missing = required - set(chunk.columns)
        if missing:
            raise ValueError(f"CSV is missing required columns: {missing}")

        param_cols = cfg.parameter_columns or [
            c for c in chunk.columns if c not in required
        ]

        dataset = TelemetryDataset(
            metadata={
                "source": str(cfg.path),
                "extractor": "csv",
                "rows": len(chunk),
            }
        )

        for _, row in chunk.iterrows():
            tai = float(row[cfg.time_column])
            apid = int(row[cfg.apid_column])
            seq = int(row[cfg.seq_count_column])

            for col in param_cols:
                raw = row[col]
                if pd.isna(raw):
                    continue
                param = EngineeringParameter(
                    name=col,
                    apid=apid,
                    seq_count=seq,
                    sample_time_tai=tai,
                    raw_value=raw,
                    eng_value=raw,
                )
                dataset.add_parameter(param)

        return dataset
