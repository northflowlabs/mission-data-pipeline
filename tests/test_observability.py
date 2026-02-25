"""Tests for observability: metrics, hooks, and logging config."""

from __future__ import annotations

import pytest

from mdp.observability.hooks import EventHook, HookManager
from mdp.observability.logging import configure_logging
from mdp.observability.metrics import PipelineMetrics, StageMetric


class TestPipelineMetrics:
    def test_initial_state(self) -> None:
        m = PipelineMetrics("test-pipeline")
        assert m.batches == 0
        assert m.total_packets == 0
        assert m.elapsed_s >= 0.0

    def test_record_batch(self) -> None:
        m = PipelineMetrics("p")
        m.record_batch(100)
        m.record_batch(50)
        assert m.batches == 2
        assert m.total_packets == 150

    def test_record_stage(self) -> None:
        m = PipelineMetrics("p")
        m.record_stage("decom", records_in=100, records_out=100, elapsed_s=0.5)
        m.record_stage("decom", records_in=50, records_out=50, elapsed_s=0.25)
        s = m.stage("decom")
        assert s is not None
        assert s.invocations == 2
        assert s.records_in == 150
        assert s.total_elapsed_s == pytest.approx(0.75)
        assert s.errors == 0

    def test_record_stage_error(self) -> None:
        m = PipelineMetrics("p")
        m.record_stage("loader", records_in=10, records_out=0, elapsed_s=0.1, error=True)
        s = m.stage("loader")
        assert s is not None
        assert s.errors == 1

    def test_avg_elapsed(self) -> None:
        m = PipelineMetrics("p")
        m.record_stage("t", records_in=0, records_out=0, elapsed_s=0.6)
        m.record_stage("t", records_in=0, records_out=0, elapsed_s=0.4)
        assert m.stage("t").avg_elapsed_s == pytest.approx(0.5)  # type: ignore[union-attr]

    def test_throughput_rps(self) -> None:
        m = PipelineMetrics("p")
        m.record_stage("x", records_in=100, records_out=100, elapsed_s=1.0)
        assert m.stage("x").throughput_rps == pytest.approx(100.0)  # type: ignore[union-attr]

    def test_zero_division_guards(self) -> None:
        m = StageMetric(name="empty")
        assert m.avg_elapsed_s == 0.0
        assert m.throughput_rps == 0.0

    def test_snapshot_serialisable(self) -> None:
        m = PipelineMetrics("snap")
        m.record_batch(10)
        m.record_stage("s", 10, 10, 0.01)
        snap = m.snapshot()
        assert snap["pipeline"] == "snap"
        assert snap["batches"] == 1
        assert "stages" in snap
        assert "s" in snap["stages"]  # type: ignore[operator]

    def test_all_stages(self) -> None:
        m = PipelineMetrics("p")
        m.record_stage("a", 1, 1, 0.1)
        m.record_stage("b", 1, 1, 0.2)
        names = {s.name for s in m.all_stages()}
        assert names == {"a", "b"}


class TestEventHook:
    def test_fire_calls_handlers(self) -> None:
        calls = []
        hook = EventHook("test.event")
        hook.register(lambda x: calls.append(x))
        hook.fire(42)
        assert calls == [42]

    def test_multiple_handlers(self) -> None:
        results = []
        hook = EventHook("e")
        hook.register(lambda v: results.append(v * 2))
        hook.register(lambda v: results.append(v * 3))
        hook.fire(10)
        assert sorted(results) == [20, 30]

    def test_unregister(self) -> None:
        calls = []
        def handler(v: int) -> None:
            calls.append(v)

        hook = EventHook("e")
        hook.register(handler)
        hook.unregister(handler)
        hook.fire(99)
        assert calls == []

    def test_swallows_handler_errors(self) -> None:
        def bad_handler(*_: object) -> None:
            raise RuntimeError("boom")

        hook = EventHook("e")
        hook.register(bad_handler)
        hook.fire()  # should not raise

    def test_len(self) -> None:
        hook = EventHook("e")
        assert len(hook) == 0
        hook.register(lambda: None)
        assert len(hook) == 1


class TestHookManager:
    def test_decorator_registration(self) -> None:
        mgr = HookManager()
        received = []

        @mgr.on("pipeline.start")
        def handler(name: str) -> None:
            received.append(name)

        mgr.fire("pipeline.start", "my-pipeline")
        assert received == ["my-pipeline"]

    def test_fire_unknown_event_is_silent(self) -> None:
        mgr = HookManager()
        mgr.fire("nonexistent.event", "arg")  # should not raise

    def test_registered_events(self) -> None:
        mgr = HookManager()
        mgr.register("batch.complete", lambda: None)
        assert "batch.complete" in mgr.registered_events()

    def test_builtin_events_exist(self) -> None:
        mgr = HookManager()
        for event in HookManager.BUILTIN_EVENTS:
            assert mgr.get_hook(event) is not None

    def test_multiple_fires(self) -> None:
        mgr = HookManager()
        count = []
        mgr.register("pipeline.complete", lambda: count.append(1))
        mgr.fire("pipeline.complete")
        mgr.fire("pipeline.complete")
        assert len(count) == 2


class TestConfigureLogging:
    def test_console_format(self) -> None:
        configure_logging(level="DEBUG", fmt="console")

    def test_json_format(self) -> None:
        configure_logging(level="WARNING", fmt="json")

    def test_with_caller_info(self) -> None:
        configure_logging(level="INFO", fmt="console", include_caller=True)
