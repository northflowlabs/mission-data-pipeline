"""CSV telemetry extractor: reads pre-processed telemetry CSV files.

Expected columns (configurable):
  - ``time``: TAI seconds since J2000.0, 2000-01-01T12:00:00 TT (float)
  - ``apid``: Application Process Identifier (int)
  - ``seq_count``: packet sequence count (int)
  - ``<param_N>``: any number of parameter columns

This extractor does not parse raw binary: it reads structured CSV exports
as commonly produced by ground segment SCICON or MCS tools.

Cells are read as text and converted by ``parameter_types``, so an integer keeps every
digit and "007" stays what it is. Without a declared type, a cell written as a plain
integer or a number becomes one, and anything else, such as "007", stays text.

A row must have as many cells as the header. A row of another width is never shifted
or padded: it is skipped and counted in ``wrong_width_rows``. A trailing delimiter is
ignored when it appears on the header and on the row, and a file whose rows all end in
one, under a header that does not, is read the same way.
"""

from __future__ import annotations

import csv
import re
from collections.abc import Iterator
from pathlib import Path
from typing import Annotated

from pydantic import BaseModel, Field

from mdp.core.base import Extractor
from mdp.core.registry import registry
from mdp.models.dataset import TelemetryDataset
from mdp.models.parameter import EngineeringParameter, ParameterType

_PLAIN_INTEGER = re.compile(r"-?(0|[1-9][0-9]*)")
_PLAIN_NUMBER = re.compile(r"-?((0|[1-9][0-9]*)(\.[0-9]*)?|\.[0-9]+)([eE][+-]?[0-9]+)?")
_TRUE = {"1", "true"}
_FALSE = {"0", "false"}


class CsvExtractorConfig(BaseModel):
    path: Path = Field(description="Path to the CSV telemetry file")
    batch_size: Annotated[int, Field(gt=0)] = 1000
    time_column: str = "time"
    apid_column: str = "apid"
    seq_count_column: str = "seq_count"
    parameter_columns: list[str] | None = Field(
        default=None,
        min_length=1,
        description="Explicit list of parameter columns. If None, all remaining columns are used.",
    )
    parameter_types: dict[str, ParameterType] = Field(
        default_factory=dict,
        description="Declared type per column. Undeclared columns follow how the cell is written.",
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

        with open(path, encoding="utf-8-sig", newline="") as fh:
            reader = csv.reader(fh, delimiter=self.config.delimiter)
            header = next(reader, None)
            if header is None:
                return
            if header and not header[-1].strip():
                header = header[:-1]
            self._check_columns(header)

            batch: list[list[str]] = []
            for row in reader:
                if not row:
                    continue
                batch.append(row)
                if len(batch) >= self.config.batch_size:
                    yield self._batch_to_dataset(header, batch)
                    batch = []
            if batch:
                yield self._batch_to_dataset(header, batch)

    def _check_columns(self, header: list[str]) -> None:
        cfg = self.config
        missing = {cfg.time_column, cfg.apid_column, cfg.seq_count_column} - set(header)
        if missing:
            raise ValueError(f"CSV is missing required columns: {missing}")
        unknown = set(cfg.parameter_columns or []) - set(header)
        if unknown:
            raise ValueError(f"CSV has no parameter columns: {unknown}")

    def _batch_to_dataset(self, header: list[str], batch: list[list[str]]) -> TelemetryDataset:
        cfg = self.config
        required = {cfg.time_column, cfg.apid_column, cfg.seq_count_column}
        param_cols = cfg.parameter_columns or [c for c in header if c not in required]

        dataset = TelemetryDataset(
            metadata={
                "source": str(cfg.path),
                "extractor": "csv",
                "rows": len(batch),
            }
        )
        if cfg.source_id is not None:
            dataset.metadata["source_id"] = cfg.source_id

        wrong_width_rows = 0
        skipped_rows = 0
        empty_cells = 0
        for cells in batch:
            if len(cells) == len(header) + 1 and not cells[-1].strip():
                cells = cells[:-1]
            if len(cells) != len(header):
                wrong_width_rows += 1
                continue
            row = dict(zip(header, cells, strict=True))

            try:
                tai = float(row[cfg.time_column])
                apid = int(row[cfg.apid_column])
                seq = int(row[cfg.seq_count_column])
            except ValueError:
                skipped_rows += 1
                continue

            for col in param_cols:
                text = row[col].strip()
                if not text:
                    empty_cells += 1
                    continue
                value = _convert(text, cfg.parameter_types.get(col), col)
                param = EngineeringParameter(
                    name=col,
                    apid=apid,
                    seq_count=seq,
                    sample_time_tai=tai,
                    raw_value=value,
                    eng_value=value,
                )
                dataset.add_parameter(param)

        dataset.metadata["wrong_width_rows"] = wrong_width_rows
        dataset.metadata["skipped_rows"] = skipped_rows
        dataset.metadata["empty_cells"] = empty_cells
        return dataset


def _convert(text: str, declared: ParameterType | None, column: str) -> int | float | bool | str:
    if declared in (ParameterType.UINT, ParameterType.INT, ParameterType.ENUMERATED):
        return int(text)
    if declared in (ParameterType.FLOAT, ParameterType.DOUBLE):
        return float(text)
    if declared == ParameterType.BOOLEAN:
        if text.lower() in _TRUE:
            return True
        if text.lower() in _FALSE:
            return False
        raise ValueError(f"Column '{column}': '{text}' is not a boolean")
    if declared is not None:
        return text

    if _PLAIN_INTEGER.fullmatch(text):
        return int(text)
    return float(text) if _PLAIN_NUMBER.fullmatch(text) else text
