"""Example 02: real satellite telemetry from CSV to HDF5.

Reads telemetry acquired on board ESA's OPS-SAT CubeSat, published in the OPSSAT-AD
dataset (see examples/data/README.md).

  1. Shape the published long-format rows into the columns CsvTelemetryExtractor reads
  2. Read the CSV in batches (CsvTelemetryExtractor)
  3. Store the three channels in HDF5 for use with xarray / h5py

Run::

    python -m examples.02_csv_to_hdf5
"""

from __future__ import annotations

import tempfile
from pathlib import Path

import h5py
import pandas as pd

from mdp.core.pipeline import Pipeline, PipelineConfig
from mdp.models.time import utc_to_tai_j2000
from mdp.observability.logging import configure_logging
from mdp.plugins.extractors.csv import CsvExtractorConfig, CsvTelemetryExtractor
from mdp.plugins.loaders.hdf5 import HDF5Loader, HDF5LoaderConfig

EXCERPT = Path(__file__).parent / "data" / "opssat_ad_excerpt.csv"

TAI_MINUS_UTC_S = 37  # leap seconds in force in 2022


def prepare_csv(source: Path, target: Path) -> list[str]:
    """Write one row per timestamp with time, apid, seq_count and a column per channel."""
    rows = pd.read_csv(source, parse_dates=["timestamp"])
    wide = rows.pivot(index="timestamp", columns="channel", values="value").sort_index()
    channels = list(wide.columns)

    times = [t.tz_localize(None).to_pydatetime() for t in wide.index]
    wide.insert(0, "time", [utc_to_tai_j2000(t, TAI_MINUS_UTC_S) for t in times])
    wide.insert(1, "apid", 0)  # the published data carries no APIDs
    wide.insert(2, "seq_count", range(len(wide)))
    wide.to_csv(target, index=False)
    return channels


def main() -> None:
    configure_logging(level="INFO", fmt="console")

    with tempfile.TemporaryDirectory(prefix="mdp_example_") as tmpdir:
        tmp = Path(tmpdir)
        csv_file = tmp / "opssat_telemetry.csv"
        hdf5_file = tmp / "opssat_telemetry.h5"

        channels = prepare_csv(EXCERPT, csv_file)

        pipeline = Pipeline(
            config=PipelineConfig(name="csv-to-hdf5"),
            extractor=CsvTelemetryExtractor(
                CsvExtractorConfig(
                    path=csv_file,
                    batch_size=100,
                    parameter_columns=channels,
                    source_id="OPS-SAT",
                )
            ),
            loader=HDF5Loader(
                HDF5LoaderConfig(
                    path=hdf5_file,
                    mode="w",
                    compression="gzip",
                    compression_opts=6,
                )
            ),
        )

        result = pipeline.run()
        print(result.summary())

        if result.ok:
            with h5py.File(hdf5_file, "r") as hf:
                print("\nHDF5 structure:")
                hf.visititems(lambda name, obj: print(f"  {name}  {obj}"))


if __name__ == "__main__":
    main()
