"""Pipeline metrics — lightweight counters and timing without external dependencies.

These are intentionally simple in-process accumulators.
For production deployments, bridge these to Prometheus / OpenTelemetry via
the ``HookManager`` in ``hooks.py``.
"""

from __future__ import annotations

import time
from dataclasses import dataclass, field
from threading import Lock


@dataclass
class StageMetric:
    """Per-stage accumulated metrics."""

    name: str
    invocations: int = 0
    records_in: int = 0
    records_out: int = 0
    errors: int = 0
    total_elapsed_s: float = 0.0

    @property
    def avg_elapsed_s(self) -> float:
        if self.invocations == 0:
            return 0.0
        return self.total_elapsed_s / self.invocations

    @property
    def throughput_rps(self) -> float:
        """Records processed per second across all invocations."""
        if self.total_elapsed_s == 0.0:
            return 0.0
        return self.records_out / self.total_elapsed_s


class PipelineMetrics:
    """Thread-safe accumulator for a single pipeline run's metrics."""

    def __init__(self, pipeline_name: str) -> None:
        self.pipeline_name = pipeline_name
        self._lock = Lock()
        self._stages: dict[str, StageMetric] = {}
        self._batches: int = 0
        self._total_packets: int = 0
        self._start_time: float = time.perf_counter()

    # ------------------------------------------------------------------ #
    #  Recording                                                           #
    # ------------------------------------------------------------------ #

    def record_batch(self, packet_count: int) -> None:
        with self._lock:
            self._batches += 1
            self._total_packets += packet_count

    def record_stage(
        self,
        stage_name: str,
        records_in: int,
        records_out: int,
        elapsed_s: float,
        error: bool = False,
    ) -> None:
        with self._lock:
            if stage_name not in self._stages:
                self._stages[stage_name] = StageMetric(name=stage_name)
            m = self._stages[stage_name]
            m.invocations += 1
            m.records_in += records_in
            m.records_out += records_out
            m.total_elapsed_s += elapsed_s
            if error:
                m.errors += 1

    # ------------------------------------------------------------------ #
    #  Querying                                                            #
    # ------------------------------------------------------------------ #

    @property
    def batches(self) -> int:
        return self._batches

    @property
    def total_packets(self) -> int:
        return self._total_packets

    @property
    def elapsed_s(self) -> float:
        return time.perf_counter() - self._start_time

    def stage(self, name: str) -> StageMetric | None:
        return self._stages.get(name)

    def all_stages(self) -> list[StageMetric]:
        return list(self._stages.values())

    def snapshot(self) -> dict[str, object]:
        """Return a serialisable metrics snapshot."""
        with self._lock:
            return {
                "pipeline": self.pipeline_name,
                "elapsed_s": round(self.elapsed_s, 4),
                "batches": self._batches,
                "total_packets": self._total_packets,
                "stages": {
                    name: {
                        "invocations": m.invocations,
                        "records_in": m.records_in,
                        "records_out": m.records_out,
                        "errors": m.errors,
                        "avg_elapsed_s": round(m.avg_elapsed_s, 6),
                        "throughput_rps": round(m.throughput_rps, 2),
                    }
                    for name, m in self._stages.items()
                },
            }
