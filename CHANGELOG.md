# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Planned
- `sec_hdr_length` per-APID override map in `BinaryExtractorConfig`
- YAML-based pipeline definition file (`mdp run --config pipeline.yaml`)
- `KafkaExtractor` — real-time packet ingestion from Kafka topics
- `SpacePyTransformer` — orbit/attitude parameter enrichment via SpacePy
- `InfluxDBLoader` — time-series sink for Grafana/monitoring dashboards
- Async pipeline execution mode for concurrent extractors
- OpenTelemetry metrics bridge

---

## [0.1.0] — 2026-02-25

### Added

#### Core
- `Extractor`, `Transformer`, `Loader` abstract base classes with typed Pydantic configs
- `Pipeline` orchestrator with batch-loop execution, configurable error handling, dry-run mode, and max-batch limiting
- `StageRegistry` with `@registry.extractor`, `@registry.transformer`, `@registry.loader` decorators for plugin self-registration
- `StageResult` and `PipelineResult` typed result containers with `.summary()` output

#### Models
- `CCSDSPrimaryHeader` — full 6-byte CCSDS Space Packet primary header per CCSDS 133.0-B-2, with `from_bytes()` / `to_bytes()` round-trip and validation
- `TelemetryPacket` — immutable Pydantic v2 model wrapping a parsed CCSDS packet; `from_bytes(raw, sec_hdr_length=N)` with mission-configurable secondary header length
- `TMFramePrimaryHeader` and `TelemetryFrame` — TM Transfer Frame models per CCSDS 132.0-B-3
- `RawParameter`, `EngineeringParameter`, `ParameterRecord` — typed parameter value models
- `TelemetryDataset` — mutable in-memory batch container with tidy and wide DataFrame export, non-destructive merge

#### Plugins — Extractors
- `BinaryPacketExtractor` — CCSDS raw binary stream reader with configurable batch size, APID filter, sync-marker scanning (`0x1ACFFC1D`), and `sec_hdr_length`
- `CsvTelemetryExtractor` — CSV engineering-unit parameter reader with configurable column mapping and chunked reading

#### Plugins — Transformers
- `DecomTransformer` — bit-level decommutation using MIB-style `ParameterDefinition` tables; supports uint8/16/32/64, int8/16/32/64, float32, float64, boolean, string, binary
- `ApidFilterTransformer` — APID whitelist or blacklist packet filter
- `CalibrationTransformer` — polynomial (`c0 + c1·x + c2·x² + ...`) and piecewise-linear table-lookup calibration with per-parameter unit assignment

#### Plugins — Loaders
- `ParquetLoader` — Apache Parquet output via pyarrow; Snappy compression; per-parameter or wide-format; optional APID partitioning; append mode
- `HDF5Loader` — HDF5 output via h5py; gzip compression; append-safe resizable datasets; xarray-compatible group layout; unit attributes
- `CsvLoader` — CSV output; per-parameter or wide-format; append mode

#### Observability
- `configure_logging()` — structlog-based structured logging with console (Rich) and JSON output modes; optional caller info
- `PipelineMetrics` — thread-safe in-process accumulator for batch counts, packet counts, per-stage invocations, throughput, and error counts
- `HookManager` — pub/sub event hook system with built-in pipeline lifecycle events (`pipeline.start`, `pipeline.complete`, `batch.extracted`, `batch.transformed`, `batch.loaded`, `stage.error`)

#### CLI (`mdp`)
- `mdp version` — print version
- `mdp stages` — list all registered plugin stages
- `mdp inspect <file>` — display packet summary table from a raw binary telemetry file
- `mdp run` — execute a pipeline from JSON config files with optional dry-run and max-batch limiting

#### Examples
- `examples/01_binary_ingest.py` — synthetic HK binary → decom → polynomial calibration → Parquet
- `examples/02_csv_to_hdf5.py` — CSV telemetry → polynomial calibration → HDF5

#### Infrastructure
- `pyproject.toml` with `[dev]` and `[docs]` extras, ruff, mypy strict, pytest-cov
- GitHub Actions CI matrix (Python 3.10, 3.11, 3.12) with lint, type check, test, and build steps
- pre-commit hooks (ruff, mypy, standard file checks)
- mkdocs-material documentation site configuration

### Standards compliance
- CCSDS 133.0-B-2 (Space Packet Protocol) — primary header model and parser
- CCSDS 132.0-B-3 (TM Space Data Link Protocol) — transfer frame model and parser
- CCSDS 301.0-B-4 (Time Code Formats) — TAI/J2000 epoch convention
- ECSS-E-ST-70-41C (TM/TC Packet Utilization) — decommutation model and MIB conventions

---

[Unreleased]: https://github.com/northflowlabs/mission-data-pipeline/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/northflowlabs/mission-data-pipeline/releases/tag/v0.1.0
