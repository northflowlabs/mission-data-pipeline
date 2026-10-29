# Observability

## Logging

MDP logs through structlog. Call `configure_logging()` once at start-up.

```python
from mdp.observability.logging import configure_logging, get_logger

configure_logging(level="INFO", fmt="json")
log = get_logger(__name__)
log.info("ingest.start", file="telemetry.bin")
```

| Argument | Values | Effect |
|---|---|---|
| `level` | `DEBUG`, `INFO`, `WARNING`, `ERROR` | Minimum level |
| `fmt` | `console`, `json` | Coloured console lines, or one JSON object per line |
| `include_caller` | `True`, `False` | Add the calling module and line number |

Logs go to stderr. The pipeline logs `pipeline.start`, `pipeline.complete`, transformer errors and unhandled extractor errors.

## Metrics

`PipelineMetrics` is a thread-safe accumulator for batch counts, packet counts and per-stage `StageMetric` values (records in and out, elapsed time, errors, average time and throughput). `snapshot()` returns a plain dict.

```python
from mdp.observability.metrics import PipelineMetrics

metrics = PipelineMetrics(pipeline_name="hk-ingest")
metrics.record_batch(packet_count=256)
metrics.record_stage("DecomTransformer", records_in=256, records_out=256, elapsed_s=0.01)
print(metrics.snapshot())
```

For a single run, the `PipelineResult` from `Pipeline.run()` already carries counts, timings and per-stage results.

## Event hooks

`HookManager` is a small publish and subscribe registry. Handlers that raise are ignored, so a failing handler cannot stop the caller.

```python
from mdp.observability.hooks import HookManager

hooks = HookManager()

@hooks.on("batch.loaded")
def on_loaded(dataset):
    print(f"loaded {len(dataset)} packets")

hooks.fire("batch.loaded", dataset)
```

The predefined event names are `pipeline.start`, `pipeline.complete`, `batch.extracted`, `batch.transformed`, `batch.loaded` and `stage.error`. Any other name also works.

In version 0.1.0, `Pipeline` does not fire these events itself. Fire them from your own code, for example from a custom loader.
