"""Observability — structured logging, metrics, and event hooks."""

from mdp.observability.hooks import EventHook, HookManager
from mdp.observability.logging import configure_logging
from mdp.observability.metrics import PipelineMetrics

__all__ = [
    "configure_logging",
    "PipelineMetrics",
    "EventHook",
    "HookManager",
]
