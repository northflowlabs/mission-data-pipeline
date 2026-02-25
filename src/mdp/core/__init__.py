"""Core ETL abstractions and pipeline engine."""

from mdp.core.base import (
    Extractor,
    Transformer,
    Loader,
    PipelineStage,
    StageResult,
    StageStatus,
)
from mdp.core.pipeline import Pipeline, PipelineConfig, PipelineResult
from mdp.core.registry import StageRegistry, registry

__all__ = [
    "Extractor",
    "Transformer",
    "Loader",
    "PipelineStage",
    "StageResult",
    "StageStatus",
    "Pipeline",
    "PipelineConfig",
    "PipelineResult",
    "StageRegistry",
    "registry",
]
