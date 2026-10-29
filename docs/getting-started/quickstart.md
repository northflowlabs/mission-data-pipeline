# Quick start

## Parse one CCSDS Space Packet

```python
from mdp.models.packet import TelemetryPacket

# 6-byte primary header + 4-byte secondary header + 4-byte user data
raw = bytes.fromhex("08010000000700000000DEADBEEF")
packet = TelemetryPacket.from_bytes(raw, sec_hdr_length=4)
print(f"APID: 0x{packet.apid:04X}  seq: {packet.seq_count}  data: {packet.user_data.hex()}")
```

`sec_hdr_length` is mission specific. MDP does not assume a fixed secondary header length.

## Run a pipeline

This pipeline reads housekeeping packets from APID 0x100, decommutates one 16-bit parameter, applies a polynomial calibration and writes Parquet.

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
    config=PipelineConfig(name="hk-ingest"),
    extractor=BinaryPacketExtractor(
        BinaryExtractorConfig(
            path=Path("telemetry.bin"),
            batch_size=256,
            apid_filter=[0x100],
            sec_hdr_length=4,
        )
    ),
    transformers=[
        DecomTransformer(
            DecomConfig(
                parameters=[
                    ParameterDefinition(
                        name="obc_temp",
                        apid=0x100,
                        byte_offset=0,
                        bit_length=16,
                        param_type=ParameterType.UINT,
                        unit="DN",
                    ),
                ]
            )
        ),
        CalibrationTransformer(
            CalibrationConfig(
                calibrations=[
                    CalibrationEntry(
                        parameter_name="obc_temp",
                        method=CalibrationMethod.POLYNOMIAL,
                        coefficients=[-55.0, 0.044],
                        unit="degC",
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

Both examples generate their own synthetic input and need no external files. Run them from the project root:

```bash
python -m examples.01_binary_ingest
python -m examples.02_csv_to_hdf5
```

See [Examples](../examples.md).
