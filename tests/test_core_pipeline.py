"""Tests for the Pipeline orchestrator and stage base classes."""

from __future__ import annotations

from pathlib import Path
from typing import Iterator

import pytest
from pydantic import BaseModel

from mdp.core.base import (
    Extractor,
    Loader,
    StageResult,
    StageStatus,
    Transformer,
)
from mdp.core.pipeline import Pipeline, PipelineConfig, PipelineResult
from mdp.models.dataset import TelemetryDataset
from mdp.models.parameter import EngineeringParameter


# --------------------------------------------------------------------------- #
#  Minimal stub implementations                                                #
# --------------------------------------------------------------------------- #


class _StubConfig(BaseModel):
    n_batches: int = 3
    packets_per_batch: int = 5


class _StubExtractor(Extractor[_StubConfig]):
    config_class = _StubConfig

    def extract(self) -> Iterator[TelemetryDataset]:
        for i in range(self.config.n_batches):
            ds = TelemetryDataset(metadata={"batch": i})
            for j in range(self.config.packets_per_batch):
                p = EngineeringParameter(
                    name="counter",
                    apid=0x100,
                    seq_count=i * 10 + j,
                    sample_time_tai=float(i * 100 + j),
                    raw_value=j,
                    eng_value=float(j),
                )
                ds.add_parameter(p)
            yield ds


class _EmptyConfig(BaseModel):
    pass


class _DoubleTransformer(Transformer[_EmptyConfig]):
    """Doubles all 'counter' eng_value samples."""

    config_class = _EmptyConfig

    def transform(self, dataset: TelemetryDataset) -> TelemetryDataset:
        record = dataset.get_parameter("counter")
        if record is None:
            return dataset
        new_samples = [
            EngineeringParameter(
                **{**s.model_dump(), "eng_value": float(s.eng_value) * 2}  # type: ignore[arg-type]
            )
            for s in record.samples
        ]
        object.__setattr__(record, "samples", new_samples)
        return dataset


class _FailingTransformer(Transformer[_EmptyConfig]):
    config_class = _EmptyConfig

    def transform(self, dataset: TelemetryDataset) -> TelemetryDataset:
        raise RuntimeError("deliberate failure")


class _RecordingLoader(Loader[_EmptyConfig]):
    config_class = _EmptyConfig

    def __init__(self, config: _EmptyConfig) -> None:
        super().__init__(config)
        self.loaded: list[TelemetryDataset] = []

    def load(self, dataset: TelemetryDataset) -> None:
        self.loaded.append(dataset)


# --------------------------------------------------------------------------- #
#  Tests                                                                       #
# --------------------------------------------------------------------------- #


class TestPipeline:
    def _make_pipeline(
        self,
        n_batches: int = 3,
        transformers=None,
        loader=None,
        stop_on_error=True,
        dry_run=False,
        max_batches=None,
    ) -> tuple[Pipeline, _RecordingLoader]:
        if loader is None:
            loader = _RecordingLoader(_EmptyConfig())
        pipeline = Pipeline(
            config=PipelineConfig(
                name="test",
                stop_on_error=stop_on_error,
                dry_run=dry_run,
                max_batches=max_batches,
            ),
            extractor=_StubExtractor(_StubConfig(n_batches=n_batches)),
            transformers=transformers or [],
            loader=loader,
        )
        return pipeline, loader

    def test_basic_run(self) -> None:
        pipeline, loader = self._make_pipeline(n_batches=3)
        result = pipeline.run()
        assert result.ok
        assert result.batches_processed == 3
        assert len(loader.loaded) == 3

    def test_dry_run_skips_loader(self) -> None:
        pipeline, loader = self._make_pipeline(dry_run=True)
        result = pipeline.run()
        assert result.ok
        assert len(loader.loaded) == 0

    def test_max_batches(self) -> None:
        pipeline, loader = self._make_pipeline(n_batches=10, max_batches=2)
        result = pipeline.run()
        assert result.batches_processed == 2
        assert len(loader.loaded) == 2

    def test_transformer_applied(self) -> None:
        loader = _RecordingLoader(_EmptyConfig())
        pipeline = Pipeline(
            config=PipelineConfig(name="t"),
            extractor=_StubExtractor(_StubConfig(n_batches=1, packets_per_batch=3)),
            transformers=[_DoubleTransformer(_EmptyConfig())],
            loader=loader,
        )
        result = pipeline.run()
        assert result.ok
        ds = loader.loaded[0]
        record = ds.get_parameter("counter")
        assert record is not None
        for sample in record.samples:
            assert float(sample.eng_value) % 2 == 0  # all values doubled (were 0,1,2)

    def test_failing_transformer_stops_pipeline(self) -> None:
        pipeline, loader = self._make_pipeline(
            transformers=[_FailingTransformer(_EmptyConfig())],
            stop_on_error=True,
        )
        result = pipeline.run()
        assert not result.ok
        assert len(result.errors) > 0

    def test_failing_transformer_continues_on_error(self) -> None:
        pipeline, loader = self._make_pipeline(
            n_batches=3,
            transformers=[_FailingTransformer(_EmptyConfig())],
            stop_on_error=False,
        )
        result = pipeline.run()
        assert not result.ok
        assert result.batches_processed == 3

    def test_result_summary_string(self) -> None:
        pipeline, _ = self._make_pipeline()
        result = pipeline.run()
        summary = result.summary()
        assert "test" in summary
        assert "batches" in summary

    def test_pipeline_repr(self) -> None:
        pipeline, _ = self._make_pipeline()
        r = repr(pipeline)
        assert "Pipeline" in r
        assert "test" in r


class TestStageResult:
    def test_ok_true_on_success(self) -> None:
        r = StageResult("stage", StageStatus.SUCCESS, 0.01)
        assert r.ok is True

    def test_ok_false_on_failure(self) -> None:
        r = StageResult("stage", StageStatus.FAILED, 0.01, error=RuntimeError("x"))
        assert r.ok is False
