<a href="https://northflow.no"><img src=".github/assets/northflow-mark.svg" alt="Northflow" width="40" height="40"></a>

# Mission Data Pipeline (MDP)

MDP is a typed Python ETL framework for spacecraft telemetry that implements the CCSDS Space Packet Protocol (133.0-B-2).

[Northflow Space and Research](https://research.northflow.no) · [Examples](examples/) · [Contributing](CONTRIBUTING.md) · [Licence](#licence)

[![CI](https://github.com/northflowlabs/mission-data-pipeline/actions/workflows/ci.yml/badge.svg)](https://github.com/northflowlabs/mission-data-pipeline/actions/workflows/ci.yml)
[![Licence: MIT](https://img.shields.io/badge/licence-MIT-0e6b63)](LICENSE)

MDP fits into ground segment workflows, research pipelines and CI-driven data processing.

## Context

Ground segment software commonly presents one of two constraints:
**monolithic MCS tools** that couple processing logic to proprietary GUIs, or **ad-hoc scripts** without defined interfaces, observability, or reproducibility guarantees.

MDP sits between the two: a composable Python library with standard telemetry data structures and a defined ETL contract. It runs in any Python environment, headless in CI, and extends through typed plugins, with no proprietary runtime dependencies.

MDP follows the data-layer patterns Northflow uses in the [Hypothesis Generation Engine (HGE)](https://research.northflow.no/systems/hge) for reproducible processing of observational data.

```
Raw binary (CCSDS) ──► BinaryExtractor
                            │
                        DecomTransformer       ← MIB-style parameter definitions
                            │
                        CalibrationTransformer ← polynomial / table-lookup
                            │
                        ParquetLoader / HDF5Loader / CsvLoader
```

## Features

- **Typed end-to-end**: Pydantic v2 models for packets, frames, parameters, and all pipeline configurations
- **CCSDS-native**: `CCSDSPrimaryHeader`, `TelemetryPacket`, `TelemetryFrame` with correct bit-field parsing per CCSDS 133.0-B-2 and 132.0-B-3
- **Modular ETL**: register custom `Extractor`, `Transformer`, or `Loader` stages with a single decorator; no subclassing framework required
- **Observable**: structured logging via structlog (console and JSON output modes), in-process pipeline metrics, lifecycle event hooks
- **Multiple output sinks**: Apache Parquet (pyarrow, Snappy compression), HDF5 (gzip, append-safe, xarray-compatible), CSV
- **CLI**: `mdp inspect`, `mdp run`, `mdp stages` for operator-facing tooling
- **Async-capable**: extractors expose both synchronous and async generator interfaces
- **Zero proprietary dependencies**: pure Python, no cloud services, no vendor lock-in

## Installation

**From source (development):**

```bash
git clone https://github.com/northflowlabs/mission-data-pipeline.git
cd mission-data-pipeline
pip install -e ".[dev]"
```

**Requirements:** Python ≥ 3.10, NumPy, Pandas, PyArrow, h5py, Pydantic v2, structlog, Click, Rich

## Quick Start

### Parse a raw CCSDS Space Packet

```python
from pathlib import Path

from mdp.models.packet import TelemetryPacket

# Real Level 0 telemetry from the NASA CYGNSS F7 satellite, in examples/data
raw = Path("examples/data/CYGNSS_F7_L0_2022_086_10_15_V01_F__first101pkts.tlm").read_bytes()

# from_bytes reads the first packet; CYGNSS has a 10-byte secondary header
packet = TelemetryPacket.from_bytes(raw, sec_hdr_length=10)
print(f"APID: {packet.apid}  seq: {packet.seq_count}  user data: {len(packet.user_data)} bytes")
```

### Run a full ingest pipeline

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

## CLI

```bash
# List all registered pipeline stage plugins
mdp stages

# Inspect a raw binary telemetry file
mdp inspect examples/data/CYGNSS_F7_L0_2022_086_10_15_V01_F__first101pkts.tlm --max-packets 50 --apid 1313

# Run a pipeline from JSON config files
mdp run \
  --extractor binary \
  --extractor-config extractor.json \
  --transformer decom \
  --transformer-config decom=decom.json \
  --loader parquet \
  --loader-config loader.json \
  --pipeline-name hk-ingest

# Dry run: extract and transform only, no output written
mdp run --extractor csv --extractor-config cfg.json --dry-run

# Print version
mdp version
```

`extractor.json`, `decom.json` and `loader.json` hold the fields of each stage's config model:

```json
{"path": "examples/data/CYGNSS_F7_L0_2022_086_10_15_V01_F__first101pkts.tlm", "sec_hdr_length": 10, "apid_filter": [1313]}
```

```json
{"parameters": [{"name": "gps_seconds", "apid": 1313, "byte_offset": 6, "bit_length": 64, "param_type": "double", "unit": "s", "little_endian": true}]}
```

```json
{"output_dir": "output"}
```

Binary packets carry no parameters until `decom` decodes them. A run that gives the loader nothing to write exits with status 1.

## Architecture

```
src/mdp/
├── core/
│   ├── base.py          # Abstract Extractor, Transformer, Loader + StageResult
│   ├── pipeline.py      # Pipeline orchestrator (batch loop, error handling)
│   └── registry.py      # @registry.extractor / .transformer / .loader decorators
│
├── models/
│   ├── packet.py        # CCSDSPrimaryHeader, TelemetryPacket (CCSDS 133.0-B-2)
│   ├── frame.py         # TelemetryFrame, TMFramePrimaryHeader (CCSDS 132.0-B-3)
│   ├── parameter.py     # RawParameter, EngineeringParameter, ParameterRecord
│   └── dataset.py       # TelemetryDataset, in-memory batch container
│
├── observability/
│   ├── logging.py       # configure_logging(): structlog, console or JSON
│   ├── metrics.py       # PipelineMetrics, StageMetric accumulators
│   └── hooks.py         # EventHook, HookManager (pub/sub lifecycle events)
│
├── plugins/
│   ├── extractors/
│   │   ├── binary.py    # CCSDS raw binary stream reader, sync-marker support
│   │   └── csv.py       # CSV engineering-unit parameter reader
│   ├── transformers/
│   │   ├── decom.py     # Bit-level decommutation with MIB-style definitions
│   │   ├── filter.py    # APID whitelist / blacklist filter
│   │   └── calibration.py  # Polynomial and table-lookup calibration
│   └── loaders/
│       ├── parquet.py   # Apache Parquet (pyarrow, Snappy), per-parameter or wide
│       ├── hdf5.py      # HDF5 (gzip, append-safe, xarray-compatible layout)
│       └── csv.py       # CSV, per-parameter or wide-format
│
└── cli/
    └── main.py          # `mdp` Click CLI entry point
```

### Design principles

- **Stages are typed and self-describing.** Every stage declares a Pydantic config model. Configs are validated at construction, not at runtime.
- **The dataset is the contract.** `TelemetryDataset` is the single interface between all stages. Stages do not communicate through side channels.
- **Observability is structural, not bolted on.** Metrics and hooks are integrated at the pipeline level. No logging is scattered through business logic.
- **Plugins are first-class.** The `@registry` decorator is the only integration point. The core pipeline has no knowledge of plugin implementations.

## Data Model

### `CCSDSPrimaryHeader`

Implements the 6-byte CCSDS Space Packet primary header per **CCSDS 133.0-B-2, Section 4.1**:

| Bits | Field | Range |
|------|-------|-------|
| 3 | `version` | 0 (fixed) |
| 1 | `type_flag` | 0=TM, 1=TC |
| 1 | `sec_hdr_flag` | secondary header present |
| 11 | `apid` | 0x000 to 0x7FF |
| 2 | `seq_flags` | see `PacketSequenceFlags` |
| 14 | `seq_count` | 0 to 16383 (wraps) |
| 16 | `data_length` | data field length − 1 |

Supports `from_bytes()` / `to_bytes()` round-trip with full validation.

### `TelemetryPacket`

| Field | Type | Description |
|---|---|---|
| `header` | `CCSDSPrimaryHeader` | Parsed 6-byte primary header |
| `secondary_header` | `bytes` | Raw secondary header (length mission-specific) |
| `user_data` | `bytes` | Application user data field |
| `source_time_tai` | `float \| None` | TAI seconds since J2000 epoch |
| `ground_receipt_time` | `float \| None` | Ground receipt time (UNIX epoch) |
| `source_id` | `str \| None` | Ground station or spacecraft identifier |

`from_bytes(raw, sec_hdr_length=N)` accepts a mission-specific secondary header length rather than assuming a fixed value.

MDP does not decode secondary headers, because their format is mission-specific. Set `source_time_tai` from yours in a transformer ahead of `decom`, as `examples/01_binary_ingest.py` does; without it, the `time_tai` of decommutated samples holds the sequence count as an ordering key.

### `TelemetryDataset`

The mutable batch container passed between pipeline stages:

```python
dataset.packets                          # list[TelemetryPacket]
dataset.parameters                       # dict[str, ParameterRecord]
dataset.packets_by_apid(0x100)           # filtered packet list
dataset.to_dataframe("obc_temp")         # tidy pd.DataFrame, sorted by TAI
dataset.to_wide_dataframe()              # wide pd.DataFrame, indexed by TAI
dataset.merge(other_dataset)             # non-destructive merge
```

## Extending MDP

### Register a custom Extractor

```python
from pydantic import BaseModel
from mdp.core.base import Extractor
from mdp.core.registry import registry
from mdp.models.dataset import TelemetryDataset

class UDPConfig(BaseModel):
    host: str
    port: int = 10000
    sec_hdr_length: int = 4

@registry.extractor("udp_stream")
class UDPExtractor(Extractor[UDPConfig]):
    config_class = UDPConfig

    def extract(self):
        import socket
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        sock.bind((self.config.host, self.config.port))
        while True:
            data, _ = sock.recvfrom(65535)
            ds = TelemetryDataset()
            # parse `data` into TelemetryPacket(s) and add to ds
            yield ds
```

### Attach event hooks for monitoring

```python
from mdp.core.pipeline import Pipeline, PipelineConfig
from mdp.observability.hooks import HookManager

hooks = HookManager()

@hooks.on("batch.loaded")
def on_loaded(dataset, result):
    print(f"{result.stage_name} wrote {dataset.sample_count()} samples")

@hooks.on("stage.error")
def on_error(stage_name, exc):
    print(f"Pipeline stage '{stage_name}' failed: {exc}")

pipeline = Pipeline(PipelineConfig(name="hk"), extractor, loader=loader, hooks=hooks)
```

The pipeline fires `pipeline.start(name)`, `batch.extracted(dataset)`, `batch.transformed(dataset)`, `batch.loaded(dataset, stage_result)`, `stage.error(stage_name, exception)` and `pipeline.complete(result)`. A handler that raises is ignored, so a failing handler cannot stop the pipeline.

## Examples

| Script | Description |
|---|---|
| [`examples/01_binary_ingest.py`](examples/01_binary_ingest.py) | NASA CYGNSS Level 0 packets → packet time → Decom → Polynomial calibration → Parquet |
| [`examples/02_csv_to_hdf5.py`](examples/02_csv_to_hdf5.py) | ESA OPS-SAT telemetry from CSV → HDF5 |

Run directly:

```bash
python -m examples.01_binary_ingest
python -m examples.02_csv_to_hdf5
```

Both examples read real spacecraft telemetry from [`examples/data`](examples/data/README.md), which also gives its sources and licences.

## Testing

```bash
# Full test suite
pytest

# With coverage report
pytest -v --cov=mdp --cov-report=term-missing

# Single module
pytest tests/test_models_packet.py -v
```

Current coverage: **90% overall**.

## Roadmap

- [ ] `sec_hdr_length` per-APID configuration in `BinaryExtractorConfig`
- [ ] YAML-based pipeline definition (`mdp run --config pipeline.yaml`)
- [ ] `KafkaExtractor`: real-time packet ingestion from Kafka topics
- [ ] `SpacePyTransformer`: orbit/attitude parameter enrichment via SpacePy
- [ ] `InfluxDBLoader`: time-series sink for Grafana/monitoring dashboards
- [ ] Async pipeline execution mode for concurrent extractors
- [ ] OpenTelemetry metrics bridge for production observability stacks

## Reference Standards

This library implements or aligns with the following CCSDS and ECSS standards:

| Standard | Title | Scope in MDP |
|---|---|---|
| **CCSDS 133.0-B-2** | Space Packet Protocol | `CCSDSPrimaryHeader`, `TelemetryPacket` |
| **CCSDS 132.0-B-3** | TM Space Data Link Protocol | `TMFramePrimaryHeader`, `TelemetryFrame` |
| **CCSDS 301.0-B-4** | Time Code Formats | TAI/J2000 epoch convention throughout |
| **ECSS-E-ST-70-41C** | TM/TC Packet Utilization | Parameter decommutation model, MIB conventions |

## Contributing

See [CONTRIBUTING.md](CONTRIBUTING.md) for development setup, coding standards, and pull request guidelines.

## About Northflow

[Northflow Technologies AS](https://northflow.no), Stavanger, Norway, builds and runs products and platforms in Europe. Its research, [Northflow Space and Research](https://research.northflow.no), turns open satellite and scientific data into forecasts and analysis.

## Licence

MDP is licensed under the [MIT License](LICENSE).

Copyright 2026 Northflow Technologies AS
