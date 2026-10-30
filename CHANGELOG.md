# Changelog

All notable changes to this project will be documented in this file.

The format follows [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).
This project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

---

## [Unreleased]

### Changed
- `HDF5LoaderConfig.mode` defaults to `"w"`: a rerun replaces the file, as with Parquet and CSV. `"a"` keeps earlier runs
- `BinaryPacketExtractor` raises `ValueError` on an invalid header or a truncated packet. It used to stop or skip without saying so. With `frame_sync` it moves on and counts `skipped_frames`
- CCSDS headers with a version other than 0 are rejected
- `time_tai` is empty for samples whose packet has no `source_time_tai`. It used to hold the sequence count
- `to_wide_dataframe()` raises if a parameter has no sample time, or several samples at one time, instead of dropping samples
- `CsvLoaderConfig.float_format` defaults to `%.17g`, which keeps small values

### Fixed
- `decom` reads integer and boolean fields at any bit position, with the new `bit_offset`, and sign-extends signed fields of any length
- `TelemetryDataset.merge()` leaves both inputs unchanged
- `batch.loaded` no longer fires after a failed load, and a failed batch is not counted
- HDF5 parameters keep their columns the same length when a batch changes value type
- Parquet, CSV and HDF5 loaders keep every batch of a run. Each batch used to overwrite the last with the default settings, so only the final batch survived
- `Pipeline` fires the hooks the documentation describes; pass a `HookManager` as `hooks`
- `mdp run` exits with status 1 when a run gives the loader nothing to write, instead of reporting success with no output
- `configure_logging()` no longer fails on the first log call, and `fmt="json"` now produces JSON lines
- `mdp stages` and `mdp run` now see the built-in stages

### Added
- `mdp run --transformer-config NAME=FILE` configures transformers such as `decom` from the command line
- `Pipeline.metrics`, and `PipelineResult.total_samples`
- `examples/data`: real telemetry from NASA CYGNSS and ESA OPS-SAT, which the examples, the README and the tests now use in place of generated data
- Documentation site under `docs/`, matching the `mkdocs.yml` navigation

### Planned
- `sec_hdr_length` per-APID override map in `BinaryExtractorConfig`
- YAML-based pipeline definition file (`mdp run --config pipeline.yaml`)
- `KafkaExtractor`: real-time packet ingestion from Kafka topics
- `SpacePyTransformer`: orbit/attitude parameter enrichment via SpacePy
- `InfluxDBLoader`: time-series sink for Grafana/monitoring dashboards
- Async pipeline execution mode for concurrent extractors
- OpenTelemetry metrics bridge

---

## [0.1.0] - 2026-02-25

### Added

#### Core
- `Extractor`, `Transformer`, `Loader` abstract base classes with typed Pydantic configs
- `Pipeline` orchestrator with batch-loop execution, configurable error handling, dry-run mode, and max-batch limiting
- `StageRegistry` with `@registry.extractor`, `@registry.transformer`, `@registry.loader` decorators for plugin self-registration
- `StageResult` and `PipelineResult` typed result containers with `.summary()` output

#### Models
- `CCSDSPrimaryHeader`: full 6-byte CCSDS Space Packet primary header per CCSDS 133.0-B-2, with `from_bytes()` / `to_bytes()` round-trip and validation
- `TelemetryPacket`: immutable Pydantic v2 model wrapping a parsed CCSDS packet; `from_bytes(raw, sec_hdr_length=N)` with mission-configurable secondary header length
- `TMFramePrimaryHeader` and `TelemetryFrame`: TM Transfer Frame models per CCSDS 132.0-B-3
- `RawParameter`, `EngineeringParameter`, `ParameterRecord`: typed parameter value models
- `TelemetryDataset`: mutable in-memory batch container with tidy and wide DataFrame export, non-destructive merge

#### Plugins: Extractors
- `BinaryPacketExtractor`: CCSDS raw binary stream reader with configurable batch size, APID filter, sync-marker scanning (`0x1ACFFC1D`), and `sec_hdr_length`
- `CsvTelemetryExtractor`: CSV engineering-unit parameter reader with configurable column mapping and chunked reading

#### Plugins: Transformers
- `DecomTransformer`: bit-level decommutation using MIB-style `ParameterDefinition` tables; supports uint8/16/32/64, int8/16/32/64, float32, float64, boolean, string, binary
- `ApidFilterTransformer`: APID whitelist or blacklist packet filter
- `CalibrationTransformer`: polynomial (`c0 + c1·x + c2·x² + ...`) and piecewise-linear table-lookup calibration with per-parameter unit assignment

#### Plugins: Loaders
- `ParquetLoader`: Apache Parquet output via pyarrow; Snappy compression; per-parameter or wide-format; optional APID partitioning; append mode
- `HDF5Loader`: HDF5 output via h5py; gzip compression; append-safe resizable datasets; xarray-compatible group layout; unit attributes
- `CsvLoader`: CSV output; per-parameter or wide-format; append mode

#### Observability
- `configure_logging()`: structlog-based structured logging with console (Rich) and JSON output modes; optional caller info
- `PipelineMetrics`: thread-safe in-process accumulator for batch counts, packet counts, per-stage invocations, throughput, and error counts
- `HookManager`: pub/sub event hook system with built-in pipeline lifecycle events (`pipeline.start`, `pipeline.complete`, `batch.extracted`, `batch.transformed`, `batch.loaded`, `stage.error`)

#### CLI (`mdp`)
- `mdp version`: print version
- `mdp stages`: list all registered plugin stages
- `mdp inspect <file>`: display packet summary table from a raw binary telemetry file
- `mdp run`: execute a pipeline from JSON config files with optional dry-run and max-batch limiting

#### Examples
- `examples/01_binary_ingest.py`: binary packets → decom → polynomial calibration → Parquet
- `examples/02_csv_to_hdf5.py`: CSV telemetry → polynomial calibration → HDF5

#### Infrastructure
- `pyproject.toml` with `[dev]` and `[docs]` extras, ruff, mypy strict, pytest-cov
- GitHub Actions CI matrix (Python 3.10, 3.11, 3.12) with lint, type check, test, and build steps
- pre-commit hooks (ruff, mypy, standard file checks)
- mkdocs-material documentation site configuration

### Standards compliance
- CCSDS 133.0-B-2 (Space Packet Protocol): primary header model and parser
- CCSDS 132.0-B-3 (TM Space Data Link Protocol): transfer frame model and parser
- CCSDS 301.0-B-4 (Time Code Formats): TAI/J2000 epoch convention
- ECSS-E-ST-70-41C (TM/TC Packet Utilization): decommutation model and MIB conventions

---

[Unreleased]: https://github.com/northflowlabs/mission-data-pipeline/compare/8b2a5606e6877d77aedc2c48d136c5180a2e48f0...main
[0.1.0]: https://github.com/northflowlabs/mission-data-pipeline/commit/8b2a5606e6877d77aedc2c48d136c5180a2e48f0
