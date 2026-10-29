# Pipeline architecture

## Stages

A `Pipeline` runs one `Extractor`, a list of `Transformer` stages and an optional `Loader`.

1. The extractor yields `TelemetryDataset` batches.
2. Each batch passes through the transformers in order.
3. The loader writes the transformed batch.

Each stage is constructed with an instance of its Pydantic config model, so invalid configuration fails at construction time, before any data is read.

## PipelineConfig

| Field | Default | Effect |
|---|---|---|
| `name` | required | Label used in logs and in the result |
| `stop_on_error` | `True` | Stop at the first transformer or loader error |
| `max_batches` | `None` | Stop after this many batches |
| `dry_run` | `False` | Run extract and transform, skip the loader |
| `extra` | `{}` | Free-form metadata |

## PipelineResult

`Pipeline.run()` returns a `PipelineResult`:

- `status`: `StageStatus.SUCCESS` or `StageStatus.FAILED`
- `ok`: True when the status is success
- `elapsed_s`, `batches_processed`, `total_packets`
- `stage_results`: one `StageResult` per transformer and loader call, with records in and out, elapsed time and any error
- `errors`: the exceptions collected during the run
- `summary()`: a plain text summary

Transformer and loader errors are captured in their `StageResult` and collected in `errors`. An exception raised by the extractor ends the run and is recorded in `errors`.

## Source layout

```
src/mdp/
├── core/
│   ├── base.py          # Extractor, Transformer, Loader, StageResult
│   ├── pipeline.py      # Pipeline, PipelineConfig, PipelineResult
│   └── registry.py      # stage registry and decorators
├── models/
│   ├── packet.py        # CCSDSPrimaryHeader, TelemetryPacket (CCSDS 133.0-B-2)
│   ├── frame.py         # TelemetryFrame, TMFramePrimaryHeader (CCSDS 132.0-B-3)
│   ├── parameter.py     # RawParameter, EngineeringParameter, ParameterRecord
│   └── dataset.py       # TelemetryDataset
├── observability/
│   ├── logging.py       # configure_logging()
│   ├── metrics.py       # PipelineMetrics, StageMetric
│   └── hooks.py         # EventHook, HookManager
├── plugins/
│   ├── extractors/      # binary, csv
│   ├── transformers/    # decom, apid_filter, calibration
│   └── loaders/         # parquet, hdf5, csv
└── cli/
    └── main.py          # the mdp command
```

## Design rules

- Stages are typed. Every stage declares a Pydantic config model.
- `TelemetryDataset` is the only interface between stages.
- The registry is the only integration point for plugins. The pipeline has no knowledge of plugin implementations.
