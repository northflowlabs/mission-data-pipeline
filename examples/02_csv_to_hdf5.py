"""Example 02 — CSV telemetry to HDF5 with APID filtering.

Scenario
--------
A mission analysis tool has exported telemetry as a CSV file.
We want to:
  1. Read the CSV (CsvTelemetryExtractor)
  2. Apply a polynomial calibration to one parameter
  3. Store results in HDF5 for use with xarray / h5py

Run::

    python -m examples.02_csv_to_hdf5
"""

from __future__ import annotations

import csv
import math
import tempfile
from pathlib import Path

from mdp.core.pipeline import Pipeline, PipelineConfig
from mdp.observability.logging import configure_logging
from mdp.plugins.extractors.csv import CsvExtractorConfig, CsvTelemetryExtractor
from mdp.plugins.loaders.hdf5 import HDF5Loader, HDF5LoaderConfig
from mdp.plugins.transformers.calibration import (
    CalibrationConfig,
    CalibrationEntry,
    CalibrationMethod,
    CalibrationTransformer,
)


# --------------------------------------------------------------------------- #
#  Generate a synthetic CSV file                                               #
# --------------------------------------------------------------------------- #


def generate_csv(path: Path, rows: int = 300) -> None:
    with open(path, "w", newline="") as fh:
        writer = csv.writer(fh)
        writer.writerow(["time", "apid", "seq_count", "star_tracker_temp", "reaction_wheel_rpm"])
        for i in range(rows):
            writer.writerow([
                round(i * 1.0, 3),
                0x300,
                i,
                round(2200 + 100 * math.sin(i / 30.0), 2),
                round(5000 + 200 * math.cos(i / 20.0), 2),
            ])
    print(f"[gen] Wrote {rows} CSV rows to {path}")


# --------------------------------------------------------------------------- #
#  Main                                                                        #
# --------------------------------------------------------------------------- #


def main() -> None:
    configure_logging(level="INFO", fmt="console")

    with tempfile.TemporaryDirectory(prefix="mdp_example_") as tmpdir:
        tmp = Path(tmpdir)
        csv_file = tmp / "attitude_telem.csv"
        hdf5_file = tmp / "attitude_telem.h5"

        generate_csv(csv_file)

        extractor = CsvTelemetryExtractor(
            CsvExtractorConfig(
                path=csv_file,
                batch_size=100,
                time_column="time",
                apid_column="apid",
                seq_count_column="seq_count",
                parameter_columns=["star_tracker_temp", "reaction_wheel_rpm"],
                source_id="ATTITUDE_CONTROL",
            )
        )

        calibration = CalibrationTransformer(
            CalibrationConfig(
                calibrations=[
                    CalibrationEntry(
                        parameter_name="star_tracker_temp",
                        method=CalibrationMethod.POLYNOMIAL,
                        coefficients=[-273.15, 0.1],
                        unit="degC",
                    ),
                ]
            )
        )

        loader = HDF5Loader(
            HDF5LoaderConfig(
                path=hdf5_file,
                mode="w",
                compression="gzip",
                compression_opts=6,
            )
        )

        pipeline = Pipeline(
            config=PipelineConfig(name="csv-to-hdf5"),
            extractor=extractor,
            transformers=[calibration],
            loader=loader,
        )

        result = pipeline.run()
        print(result.summary())

        if result.ok:
            import h5py
            with h5py.File(hdf5_file, "r") as hf:
                print("\nHDF5 structure:")
                hf.visititems(lambda name, obj: print(f"  {name}  {obj}"))


if __name__ == "__main__":
    main()
