"""Pipeline orchestrator: wires Extractor → Transformer(s) → Loader."""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from typing import Any

import structlog

from mdp.core.base import (
    Extractor,
    Loader,
    StageResult,
    StageStatus,
    Transformer,
)
from mdp.models.dataset import TelemetryDataset
from mdp.observability.hooks import HookManager
from mdp.observability.metrics import PipelineMetrics

log = structlog.get_logger(__name__)

# Dataset metadata counters that mean data was lost: skipped_frames from the binary
# extractor, short_packets from decom.
_DATA_ISSUES = ("skipped_frames", "short_packets")


@dataclass
class PipelineConfig:
    """Configuration for a pipeline run."""

    name: str
    stop_on_error: bool = True
    max_batches: int | None = None
    dry_run: bool = False
    extra: dict[str, Any] = field(default_factory=dict)


@dataclass
class PipelineResult:
    """Aggregated result of a complete pipeline execution."""

    pipeline_name: str
    status: StageStatus
    elapsed_s: float
    batches_processed: int
    total_packets: int
    total_samples: int = 0
    data_issues: dict[str, int] = field(default_factory=dict)
    stage_results: list[StageResult] = field(default_factory=list)
    errors: list[Exception] = field(default_factory=list)

    @property
    def ok(self) -> bool:
        return self.status == StageStatus.SUCCESS

    def summary(self) -> str:
        lines = [
            f"Pipeline '{self.pipeline_name}': {self.status}",
            f"  elapsed   : {self.elapsed_s:.3f}s",
            f"  batches   : {self.batches_processed}",
            f"  packets   : {self.total_packets}",
            f"  samples   : {self.total_samples}",
        ]
        lines += [f"  {name} : {count}" for name, count in self.data_issues.items()]
        if self.errors:
            lines.append(f"  errors    : {len(self.errors)}")
        for r in self.stage_results:
            lines.append(
                f"  [{r.stage_name}] {r.status} "
                f"in={r.records_in} out={r.records_out} "
                f"t={r.elapsed_s:.3f}s"
            )
        return "\n".join(lines)


class Pipeline:
    """Orchestrates an ETL pipeline: one Extractor, N Transformers, one Loader.

    Usage::

        pipeline = Pipeline(
            config=PipelineConfig(name="housekeeping-ingest"),
            extractor=BinaryExtractor(cfg),
            transformers=[DecomTransformer(cfg), CalibrationTransformer(cfg)],
            loader=ParquetLoader(cfg),
        )
        result = pipeline.run()

    Each batch emitted by the extractor flows sequentially through all
    transformers before being handed to the loader. Pass a ``HookManager`` as
    ``hooks`` to react to the events listed in ``HookManager.BUILTIN_EVENTS``.
    """

    def __init__(
        self,
        config: PipelineConfig,
        extractor: Extractor,  # type: ignore[type-arg]
        transformers: list[Transformer] | None = None,  # type: ignore[type-arg]
        loader: Loader | None = None,  # type: ignore[type-arg]
        hooks: HookManager | None = None,
    ) -> None:
        self.config = config
        self.extractor = extractor
        self.transformers: list[Transformer] = transformers or []  # type: ignore[type-arg]
        self.loader = loader
        self.hooks = hooks or HookManager()
        self._metrics = PipelineMetrics(pipeline_name=config.name)

    # ------------------------------------------------------------------ #
    #  Public interface                                                     #
    # ------------------------------------------------------------------ #

    @property
    def metrics(self) -> PipelineMetrics:
        """Batch, packet and per-stage counters accumulated by this pipeline."""
        return self._metrics

    def run(self) -> PipelineResult:
        """Execute the pipeline synchronously and return a result summary."""
        t0 = time.perf_counter()
        all_stage_results: list[StageResult] = []
        errors: list[Exception] = []
        batches = 0
        total_packets = 0
        total_samples = 0
        data_issues: dict[str, int] = {}

        log.info(
            "pipeline.start",
            pipeline=self.config.name,
            dry_run=self.config.dry_run,
        )
        self.hooks.fire("pipeline.start", self.config.name)

        if self.loader:
            self.loader.begin_run()

        try:
            for dataset in self.extractor.extract():
                batch_log = log.bind(batch=batches, packets=len(dataset))
                batch_log.debug("pipeline.batch.extracted")
                self.hooks.fire("batch.extracted", dataset)

                dataset, t_results, t_errors = self._run_transformers(dataset)
                all_stage_results.extend(t_results)
                errors.extend(t_errors)
                for t_result in t_results:
                    self._record_stage(t_result)
                for issue in _DATA_ISSUES:
                    count = dataset.metadata.get(issue)
                    if isinstance(count, int) and count:
                        data_issues[issue] = data_issues.get(issue, 0) + count
                self.hooks.fire("batch.transformed", dataset)

                if t_errors and self.config.stop_on_error:
                    log.error("pipeline.stopped_on_error", stage=t_errors[0])
                    break

                if self.loader and not self.config.dry_run:
                    load_result = self.loader._timed_load(dataset)
                    all_stage_results.append(load_result)
                    self._record_stage(load_result)
                    if load_result.error:
                        errors.append(load_result.error)
                        self.hooks.fire("stage.error", load_result.stage_name, load_result.error)
                        if self.config.stop_on_error:
                            break
                        continue  # this batch was not loaded, so it is not counted
                    self.hooks.fire("batch.loaded", dataset, load_result)

                batches += 1
                total_packets += len(dataset)
                total_samples += dataset.sample_count()
                self._metrics.record_batch(len(dataset))

                if self.config.max_batches and batches >= self.config.max_batches:
                    log.info("pipeline.max_batches_reached", max=self.config.max_batches)
                    break

        except Exception as exc:
            errors.append(exc)
            log.exception("pipeline.unhandled_error", error=str(exc))
            self.hooks.fire("stage.error", self.extractor.name, exc)

        elapsed = time.perf_counter() - t0
        status = StageStatus.SUCCESS if not errors else StageStatus.FAILED

        result = PipelineResult(
            pipeline_name=self.config.name,
            status=status,
            elapsed_s=elapsed,
            batches_processed=batches,
            total_packets=total_packets,
            total_samples=total_samples,
            data_issues=data_issues,
            stage_results=all_stage_results,
            errors=errors,
        )

        if data_issues:
            log.warning("pipeline.data_issues", pipeline=self.config.name, **data_issues)

        log.info(
            "pipeline.complete",
            pipeline=self.config.name,
            status=status,
            elapsed_s=f"{elapsed:.3f}",
            batches=batches,
            packets=total_packets,
        )
        self.hooks.fire("pipeline.complete", result)
        return result

    # ------------------------------------------------------------------ #
    #  Internal helpers                                                     #
    # ------------------------------------------------------------------ #

    def _run_transformers(
        self,
        dataset: TelemetryDataset,
    ) -> tuple[TelemetryDataset, list[StageResult], list[Exception]]:
        results: list[StageResult] = []
        errors: list[Exception] = []
        for transformer in self.transformers:
            dataset, result = transformer._timed_transform(dataset)
            results.append(result)
            if result.error:
                errors.append(result.error)
                log.warning(
                    "transformer.error",
                    transformer=transformer.name,
                    error=str(result.error),
                )
                self.hooks.fire("stage.error", result.stage_name, result.error)
                if self.config.stop_on_error:
                    break
        return dataset, results, errors

    def _record_stage(self, result: StageResult) -> None:
        self._metrics.record_stage(
            result.stage_name,
            records_in=result.records_in,
            records_out=result.records_out,
            elapsed_s=result.elapsed_s,
            error=result.error is not None,
        )

    def add_transformer(self, transformer: Transformer) -> Pipeline:  # type: ignore[type-arg]
        """Fluent method to append a transformer stage."""
        self.transformers.append(transformer)
        return self

    def __repr__(self) -> str:
        t_names = [t.name for t in self.transformers]
        loader_name = self.loader.name if self.loader else "none"
        return (
            f"Pipeline(name={self.config.name!r}, "
            f"extractor={self.extractor.name!r}, "
            f"transformers={t_names}, "
            f"loader={loader_name!r})"
        )
