# Mission Data Pipeline

MDP is a typed Python ETL framework for spacecraft telemetry. It implements the CCSDS Space Packet Protocol (133.0-B-2) and the TM transfer frame primary header (132.0-B-3), and moves telemetry from raw binary or CSV files to Parquet, HDF5 or CSV.

A pipeline has one extractor, any number of transformers and one loader. Every stage has a Pydantic config model, and every stage reads and writes the same container, `TelemetryDataset`.

```
Raw binary (CCSDS) ──► BinaryPacketExtractor
                            │
                        DecomTransformer       ← parameter definitions
                            │
                        CalibrationTransformer ← polynomial or table lookup
                            │
                        ParquetLoader / HDF5Loader / CsvLoader
```

## What is in the box

- Models: `CCSDSPrimaryHeader`, `TelemetryPacket`, `TelemetryFrame`, parameter models and `TelemetryDataset`
- Extractors: CCSDS binary stream (`binary`), CSV engineering values (`csv`)
- Transformers: bit-level decommutation (`decom`), APID include and exclude filter (`apid_filter`), polynomial and table-lookup calibration (`calibration`)
- Loaders: Parquet (`parquet`), HDF5 (`hdf5`), CSV (`csv`)
- A stage registry for your own plugins
- Structured logging with structlog, in-process metrics, and a small event hook manager
- A command line tool, `mdp`

MDP has no cloud or proprietary runtime dependencies. It runs in any Python 3.10+ environment, including headless CI.

## Next steps

- [Installation](getting-started/installation.md)
- [Quick start](getting-started/quickstart.md)
- [Pipeline architecture](guide/architecture.md)
- [API reference](api/core.md)

MDP is MIT licensed and maintained by [Northflow Technologies AS](https://northflow.no). Contact: hello@northflow.no.
