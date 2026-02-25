"""Example 01 — Binary telemetry ingest to Parquet.

Scenario
--------
A ground station has produced a raw binary telemetry file containing
CCSDS Space Packets from a housekeeping (HK) APID (0x100).
We want to:
  1. Extract the raw packets (BinaryPacketExtractor)
  2. Decommutate three parameters: obc_temp, bus_voltage, bat_current
  3. Apply polynomial calibrations to convert raw DN to engineering units
  4. Store the result as Parquet files, one per parameter

Run this script from the project root::

    python -m examples.01_binary_ingest

It will create synthetic test data in /tmp/mdp_example/ and process it.
"""

from __future__ import annotations

import struct
import tempfile
from pathlib import Path

from mdp.core.pipeline import Pipeline, PipelineConfig
from mdp.models.parameter import ParameterType
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


# --------------------------------------------------------------------------- #
#  1. Generate synthetic binary telemetry                                      #
# --------------------------------------------------------------------------- #


def generate_hk_packets(output_path: Path, n: int = 200) -> None:
    """Write n synthetic HK packets to a binary file.

    Packet user_data layout (12 bytes):
      offset 0  — uint16: obc_temp_dn        (raw ADC counts, 0–4095)
      offset 2  — uint16: bus_voltage_dn     (raw ADC counts, 0–4095)
      offset 4  — uint16: bat_current_dn     (raw ADC counts, 0–4095)
      offset 6  — float32: mission_time_s    (mission elapsed time)
      offset 10 — uint16: checksum (placeholder)
    """
    import struct
    import math

    HK_APID = 0x100
    with open(output_path, "wb") as fh:
        for i in range(n):
            obc_temp_dn = int(2048 + 200 * math.sin(i / 20.0))
            bus_voltage_dn = int(3000 + 50 * math.sin(i / 50.0))
            bat_current_dn = int(1500 + 300 * math.cos(i / 15.0))
            mission_time = float(i * 4.0)

            user_data = struct.pack(">HHHfH",
                obc_temp_dn,
                bus_voltage_dn,
                bat_current_dn,
                mission_time,
                0xABCD,
            )
            secondary_header = struct.pack(">I", i)
            data_field = secondary_header + user_data
            data_length = len(data_field) - 1

            word0 = (0b000 << 13) | (0 << 12) | (1 << 11) | (HK_APID & 0x07FF)
            word1 = (0b11 << 14) | (i & 0x3FFF)
            header = struct.pack(">HHH", word0, word1, data_length)
            fh.write(header + data_field)

    print(f"[gen] Wrote {n} HK packets to {output_path}")


# --------------------------------------------------------------------------- #
#  2. Parameter definitions (MIB-style)                                        #
# --------------------------------------------------------------------------- #


PARAM_DEFS = [
    ParameterDefinition(
        name="obc_temp_dn",
        apid=0x100,
        byte_offset=0,
        bit_length=16,
        param_type=ParameterType.UINT,
        unit="DN",
        description="On-board computer temperature (raw ADC)",
    ),
    ParameterDefinition(
        name="bus_voltage_dn",
        apid=0x100,
        byte_offset=2,
        bit_length=16,
        param_type=ParameterType.UINT,
        unit="DN",
        description="Primary bus voltage (raw ADC)",
    ),
    ParameterDefinition(
        name="bat_current_dn",
        apid=0x100,
        byte_offset=4,
        bit_length=16,
        param_type=ParameterType.UINT,
        unit="DN",
        description="Battery current (raw ADC)",
    ),
]

# --------------------------------------------------------------------------- #
#  3. Calibration definitions                                                  #
# --------------------------------------------------------------------------- #

CALIBRATIONS = [
    CalibrationEntry(
        parameter_name="obc_temp_dn",
        method=CalibrationMethod.POLYNOMIAL,
        coefficients=[-55.0, 0.04394531],
        unit="degC",
    ),
    CalibrationEntry(
        parameter_name="bus_voltage_dn",
        method=CalibrationMethod.POLYNOMIAL,
        coefficients=[0.0, 0.008056640625],
        unit="V",
    ),
    CalibrationEntry(
        parameter_name="bat_current_dn",
        method=CalibrationMethod.TABLE,
        table_raw=[0.0, 1024.0, 2048.0, 3072.0, 4095.0],
        table_eng=[-2.0, -1.0, 0.0, 1.0, 2.0],
        unit="A",
    ),
]


# --------------------------------------------------------------------------- #
#  4. Wire the pipeline                                                        #
# --------------------------------------------------------------------------- #


def main() -> None:
    configure_logging(level="INFO", fmt="console")

    with tempfile.TemporaryDirectory(prefix="mdp_example_") as tmpdir:
        tmp = Path(tmpdir)
        raw_file = tmp / "hk_telemetry.bin"
        output_dir = tmp / "parquet_out"

        generate_hk_packets(raw_file, n=200)

        extractor = BinaryPacketExtractor(
            BinaryExtractorConfig(
                path=raw_file,
                batch_size=50,
                apid_filter=[0x100],
                source_id="GROUND_STATION_1",
            )
        )

        decom = DecomTransformer(DecomConfig(parameters=PARAM_DEFS))

        calibration = CalibrationTransformer(
            CalibrationConfig(calibrations=CALIBRATIONS)
        )

        loader = ParquetLoader(
            ParquetLoaderConfig(output_dir=output_dir, compression="snappy")
        )

        pipeline = Pipeline(
            config=PipelineConfig(name="hk-ingest", stop_on_error=True),
            extractor=extractor,
            transformers=[decom, calibration],
            loader=loader,
        )

        result = pipeline.run()
        print(result.summary())

        if result.ok:
            print("\nOutput files:")
            for f in sorted(output_dir.glob("*.parquet")):
                import pyarrow.parquet as pq
                tbl = pq.read_table(f)
                print(f"  {f.name}: {tbl.num_rows} rows, cols={tbl.schema.names}")


if __name__ == "__main__":
    main()
