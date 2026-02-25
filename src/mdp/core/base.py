"""Abstract base classes for all pipeline stages.

Every stage in a Mission Data Pipeline is one of:
  - Extractor  — reads raw data and yields TelemetryDatasets
  - Transformer — mutates / enriches a TelemetryDataset in-place
  - Loader      — persists a TelemetryDataset to a sink

All stages are:
  - Typed via Pydantic config models
  - Observable via structured logging
  - Composable into a Pipeline
"""

from __future__ import annotations

import abc
import time
from collections.abc import AsyncIterator, Iterator
from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Generic, TypeVar

from pydantic import BaseModel

from mdp.models.dataset import TelemetryDataset

ConfigT = TypeVar("ConfigT", bound=BaseModel)


class StageStatus(str, Enum):
    PENDING = "pending"
    RUNNING = "running"
    SUCCESS = "success"
    FAILED = "failed"
    SKIPPED = "skipped"


@dataclass
class StageResult:
    """Outcome of a single stage execution."""

    stage_name: str
    status: StageStatus
    elapsed_s: float
    records_in: int = 0
    records_out: int = 0
    error: Exception | None = None
    extra: dict[str, Any] = field(default_factory=dict)

    @property
    def ok(self) -> bool:
        return self.status == StageStatus.SUCCESS


class PipelineStage(abc.ABC, Generic[ConfigT]):
    """Base class for all pipeline stages.

    Subclasses must declare a ``config_class`` class attribute and implement
    the ``name`` property.
    """

    config_class: type[BaseModel]

    def __init__(self, config: ConfigT) -> None:
        self.config = config
        self._name = self.__class__.__name__

    @property
    def name(self) -> str:
        return self._name

    def validate_config(self) -> None:
        """Optional hook — raise ValueError if config is semantically invalid."""

    def setup(self) -> None:
        """Called once before the stage is first executed (open connections, etc.)."""

    def teardown(self) -> None:
        """Called once after the stage completes, even on error."""


# --------------------------------------------------------------------------- #
#  Extractor                                                                    #
# --------------------------------------------------------------------------- #


class Extractor(PipelineStage[ConfigT]):
    """Reads raw data from a source and emits TelemetryDataset batches.

    Implements both synchronous (``extract``) and asynchronous
    (``extract_async``) interfaces so callers can choose based on I/O model.
    """

    @abc.abstractmethod
    def extract(self) -> Iterator[TelemetryDataset]:
        """Yield successive TelemetryDataset batches from the source."""

    async def extract_async(self) -> AsyncIterator[TelemetryDataset]:
        """Async wrapper — by default delegates to the sync implementation."""
        for ds in self.extract():
            yield ds

    def _timed_extract(self) -> Iterator[tuple[TelemetryDataset, StageResult]]:
        t0 = time.perf_counter()
        try:
            self.setup()
            for ds in self.extract():
                elapsed = time.perf_counter() - t0
                yield (
                    ds,
                    StageResult(
                        stage_name=self.name,
                        status=StageStatus.SUCCESS,
                        elapsed_s=elapsed,
                        records_out=len(ds),
                    ),
                )
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            yield (
                TelemetryDataset(),
                StageResult(
                    stage_name=self.name,
                    status=StageStatus.FAILED,
                    elapsed_s=elapsed,
                    error=exc,
                ),
            )
            raise
        finally:
            self.teardown()


# --------------------------------------------------------------------------- #
#  Transformer                                                                  #
# --------------------------------------------------------------------------- #


class Transformer(PipelineStage[ConfigT]):
    """Transforms / enriches a TelemetryDataset.

    Transformers receive a dataset and return a (possibly new) dataset.
    They may filter packets, add parameters, apply calibrations, etc.
    """

    @abc.abstractmethod
    def transform(self, dataset: TelemetryDataset) -> TelemetryDataset:
        """Apply this transformation and return the resulting dataset."""

    def _timed_transform(self, dataset: TelemetryDataset) -> tuple[TelemetryDataset, StageResult]:
        t0 = time.perf_counter()
        records_in = len(dataset)
        try:
            self.setup()
            result = self.transform(dataset)
            elapsed = time.perf_counter() - t0
            return result, StageResult(
                stage_name=self.name,
                status=StageStatus.SUCCESS,
                elapsed_s=elapsed,
                records_in=records_in,
                records_out=len(result),
            )
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            return dataset, StageResult(
                stage_name=self.name,
                status=StageStatus.FAILED,
                elapsed_s=elapsed,
                records_in=records_in,
                error=exc,
            )
        finally:
            self.teardown()


# --------------------------------------------------------------------------- #
#  Loader                                                                       #
# --------------------------------------------------------------------------- #


class Loader(PipelineStage[ConfigT]):
    """Persists a TelemetryDataset to a sink (file, database, message bus, etc.)."""

    @abc.abstractmethod
    def load(self, dataset: TelemetryDataset) -> None:
        """Write the dataset to the configured sink."""

    def _timed_load(self, dataset: TelemetryDataset) -> StageResult:
        t0 = time.perf_counter()
        records_in = len(dataset)
        try:
            self.setup()
            self.load(dataset)
            elapsed = time.perf_counter() - t0
            return StageResult(
                stage_name=self.name,
                status=StageStatus.SUCCESS,
                elapsed_s=elapsed,
                records_in=records_in,
            )
        except Exception as exc:
            elapsed = time.perf_counter() - t0
            return StageResult(
                stage_name=self.name,
                status=StageStatus.FAILED,
                elapsed_s=elapsed,
                records_in=records_in,
                error=exc,
            )
        finally:
            self.teardown()
