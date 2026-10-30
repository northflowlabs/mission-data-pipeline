"""Example 01: real spacecraft telemetry to Parquet.

Reads 101 CCSDS Space Packets from the Level 0 telemetry of the NASA CYGNSS F7
satellite, recorded on 25 March 2022 (see examples/data/README.md).

  1. Extract the packets of APID 1313
  2. Read each packet's UTC time from its secondary header
  3. Decommutate the GPS week and GPS seconds from the user data
  4. Convert GPS seconds to hours of the GPS week with a polynomial
  5. Store the result as Parquet files, one per parameter

The two clocks in each packet agree, which is how these offsets were confirmed.

Run this script from the project root::

    python -m examples.01_binary_ingest
"""

from __future__ import annotations

import tempfile
from datetime import datetime, timedelta
from pathlib import Path

import pandas as pd
from pydantic import BaseModel

from mdp.core.base import Transformer
from mdp.core.pipeline import Pipeline, PipelineConfig
from mdp.models.dataset import TelemetryDataset
from mdp.models.parameter import ParameterType
from mdp.observability.hooks import HookManager
from mdp.observability.logging import configure_logging
from mdp.plugins.extractors.binary import BinaryExtractorConfig, BinaryPacketExtractor
from mdp.plugins.loaders.parquet import ParquetLoader, ParquetLoaderConfig
from mdp.plugins.transformers.calibration import (
    CalibrationConfig,
    CalibrationEntry,
    CalibrationMethod,
    CalibrationTransformer,
)
from mdp.plugins.transformers.decom import (
    DecomConfig,
    DecomTransformer,
    ParameterDefinition,
)

TELEMETRY = Path(__file__).parent / "data" / "CYGNSS_F7_L0_2022_086_10_15_V01_F__first101pkts.tlm"

GPS_APID = 1313
SEC_HDR_LENGTH = 10  # bytes 6 to 15 of each packet

# TAI-UTC was 37 s in 2022 and 32 s at J2000.
J2000 = datetime(2000, 1, 1, 12)
TAI_SINCE_J2000_OFFSET_S = 37 - 32


class CygnssTimeConfig(BaseModel):
    pass


class CygnssTimeTransformer(Transformer[CygnssTimeConfig]):
    """Set source_time_tai from the UTC fields of the CYGNSS secondary header.

    The fields start at packet byte 8 and pack year (12 bits), day of year (9),
    hour (5), minute (6), second (6) and microsecond (20) after six leading bits.
    """

    config_class = CygnssTimeConfig

    def transform(self, dataset: TelemetryDataset) -> TelemetryDataset:
        dataset.packets[:] = [
            p.model_copy(update={"source_time_tai": _tai_seconds(p.secondary_header)})
            for p in dataset.packets
        ]
        return dataset


def _tai_seconds(secondary_header: bytes) -> float:
    bits = int.from_bytes(secondary_header[2:10], "big")
    field = lambda start, width: (bits >> (64 - start - width)) & ((1 << width) - 1)  # noqa: E731
    utc = datetime(field(6, 12), 1, 1) + timedelta(
        days=field(18, 9) - 1,
        hours=field(27, 5),
        minutes=field(32, 6),
        seconds=field(38, 6),
        microseconds=field(44, 20),
    )
    return (utc - J2000).total_seconds() + TAI_SINCE_J2000_OFFSET_S


def main() -> None:
    configure_logging(level="INFO", fmt="console")

    # Offsets count from the start of the user data, after the secondary header.
    # These multi-byte fields are little-endian.
    parameters = [
        ParameterDefinition(
            name="gps_week",
            apid=GPS_APID,
            byte_offset=4,
            bit_length=16,
            param_type=ParameterType.UINT,
            little_endian=True,
        ),
        ParameterDefinition(
            name="gps_seconds",
            apid=GPS_APID,
            byte_offset=6,
            bit_length=64,
            param_type=ParameterType.DOUBLE,
            unit="s",
            little_endian=True,
        ),
    ]

    hooks = HookManager()

    @hooks.on("batch.loaded")
    def report_batch(dataset: TelemetryDataset, result: object) -> None:
        print(f"[hook] loaded {dataset.sample_count()} samples from {len(dataset)} packets")

    with tempfile.TemporaryDirectory(prefix="mdp_example_") as tmpdir:
        out_dir = Path(tmpdir) / "parquet"

        pipeline = Pipeline(
            config=PipelineConfig(name="cygnss-gps-time"),
            extractor=BinaryPacketExtractor(
                BinaryExtractorConfig(
                    path=TELEMETRY,
                    batch_size=4,
                    apid_filter=[GPS_APID],
                    sec_hdr_length=SEC_HDR_LENGTH,
                    source_id="CYGNSS-F7",
                )
            ),
            transformers=[
                CygnssTimeTransformer(CygnssTimeConfig()),
                DecomTransformer(DecomConfig(parameters=parameters)),
                CalibrationTransformer(
                    CalibrationConfig(
                        calibrations=[
                            CalibrationEntry(
                                parameter_name="gps_seconds",
                                method=CalibrationMethod.POLYNOMIAL,
                                coefficients=[0.0, 1 / 3600],
                                unit="h",
                            )
                        ]
                    )
                ),
            ],
            loader=ParquetLoader(ParquetLoaderConfig(output_dir=out_dir)),
            hooks=hooks,
        )

        result = pipeline.run()
        print(result.summary())

        if result.ok:
            for path in sorted(out_dir.glob("*.parquet")):
                df = pd.read_parquet(path)
                print(f"{path.stem:12s} {len(df)} rows, first value {df['eng_value'].iloc[0]}")


if __name__ == "__main__":
    main()
