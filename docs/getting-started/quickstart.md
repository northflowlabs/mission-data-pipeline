# Quick start

## Parse one CCSDS Space Packet

```python
from pathlib import Path

from mdp.models.packet import TelemetryPacket

# Real Level 0 telemetry from the NASA CYGNSS F7 satellite, in examples/data
raw = Path("examples/data/CYGNSS_F7_L0_2022_086_10_15_V01_F__first101pkts.tlm").read_bytes()

# from_bytes reads the first packet; CYGNSS has a 10-byte secondary header
packet = TelemetryPacket.from_bytes(raw, sec_hdr_length=10)
print(f"APID: {packet.apid}  seq: {packet.seq_count}  user data: {len(packet.user_data)} bytes")
```

`sec_hdr_length` is mission specific. MDP does not assume a fixed secondary header length.

## Run a pipeline

This pipeline reads the packets of APID 1313 from real CYGNSS telemetry, decommutates the 64-bit GPS seconds, converts them to hours of the GPS week and writes Parquet.

```python
from pathlib import Path

from mdp.core.pipeline import Pipeline, PipelineConfig
from mdp.models.parameter import ParameterType
from mdp.plugins.extractors.binary import BinaryExtractorConfig, BinaryPacketExtractor
from mdp.plugins.loaders.parquet import ParquetLoader, ParquetLoaderConfig
from mdp.plugins.transformers.calibration import (
    CalibrationConfig,
    CalibrationEntry,
    CalibrationMethod,
    CalibrationTransformer,
)
from mdp.plugins.transformers.decom import DecomConfig, DecomTransformer, ParameterDefinition

pipeline = Pipeline(
    config=PipelineConfig(name="cygnss-gps-time"),
    extractor=BinaryPacketExtractor(
        BinaryExtractorConfig(
            path=Path("examples/data/CYGNSS_F7_L0_2022_086_10_15_V01_F__first101pkts.tlm"),
            batch_size=256,
            apid_filter=[1313],  # packets carrying GPS time
            sec_hdr_length=10,
        )
    ),
    transformers=[
        DecomTransformer(
            DecomConfig(
                parameters=[
                    ParameterDefinition(
                        name="gps_seconds",
                        apid=1313,
                        byte_offset=6,
                        bit_length=64,
                        param_type=ParameterType.DOUBLE,
                        unit="s",
                        little_endian=True,
                    ),
                ]
            )
        ),
        CalibrationTransformer(
            CalibrationConfig(
                calibrations=[
                    CalibrationEntry(
                        parameter_name="gps_seconds",
                        method=CalibrationMethod.POLYNOMIAL,
                        coefficients=[0.0, 1 / 3600],  # seconds to hours of the GPS week
                        unit="h",
                    ),
                ]
            )
        ),
    ],
    loader=ParquetLoader(ParquetLoaderConfig(output_dir=Path("output/"))),
)

result = pipeline.run()
print(result.summary())
```

`pipeline.run()` returns a `PipelineResult` with the status, elapsed time, batch and packet counts, per-stage results and any errors. `result.ok` is True when the run succeeded.

## Run the bundled examples

Both examples read real spacecraft telemetry from `examples/data`. Run them from the project root:

```bash
python -m examples.01_binary_ingest
python -m examples.02_csv_to_hdf5
```

See [Examples](../examples.md).
