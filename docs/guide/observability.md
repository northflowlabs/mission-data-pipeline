# Observability

## Logging

MDP logs through structlog. Call `configure_logging()` once at start-up.

```python
from mdp.observability.logging import configure_logging, get_logger

configure_logging(level="INFO", fmt="json")
log = get_logger(__name__)
log.info("ingest.start", file="cygnss.tlm")
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

`HookManager` is a small publish and subscribe registry. Pass one to `Pipeline`. Handlers that raise are ignored, so a failing handler cannot stop the pipeline.

```python
from mdp.core.pipeline import Pipeline, PipelineConfig
from mdp.observability.hooks import HookManager

hooks = HookManager()

@hooks.on("batch.loaded")
def on_loaded(dataset, result):
    print(f"{result.stage_name} wrote {dataset.sample_count()} samples")

pipeline = Pipeline(PipelineConfig(name="hk"), extractor, loader=loader, hooks=hooks)
```

| Event | Arguments | Fired |
|---|---|---|
| `pipeline.start` | name | Before the first batch |
| `batch.extracted` | dataset | After each extractor yield |
| `batch.transformed` | dataset | After the transformers have run on a batch |
| `batch.loaded` | dataset, stage result | After the loader has written a batch |
| `stage.error` | stage name, exception | When any stage fails |
| `pipeline.complete` | pipeline result | After the run, also a failed one |

`Pipeline.metrics` holds the batch, packet and per-stage counters of the pipeline.

`PipelineResult.data_issues` counts data lost along the way, and `summary()` prints it: `skipped_frames` from the binary extractor with `frame_sync`, and `short_packets` for parameters that did not fit in a packet.
