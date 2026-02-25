"""Structured logging configuration using structlog.

Provides a single ``configure_logging()`` call that sets up:
  - structlog with timestamped, leveled, coloured console output (dev)
  - or JSON output for production / log aggregators
"""

from __future__ import annotations

import logging
import sys
from typing import Literal

import structlog


def configure_logging(
    level: str = "INFO",
    fmt: Literal["console", "json"] = "console",
    include_caller: bool = False,
) -> None:
    """Configure the root logger and structlog processors.

    Parameters
    ----------
    level:
        Standard log level name, e.g. ``"DEBUG"``, ``"INFO"``, ``"WARNING"``.
    fmt:
        ``"console"`` for human-readable Rich-style output (development),
        ``"json"`` for machine-readable newline-delimited JSON (production).
    include_caller:
        If True, attach ``caller_module`` and ``caller_lineno`` to every event.
    """
    shared_processors: list[structlog.types.Processor] = [
        structlog.contextvars.merge_contextvars,
        structlog.stdlib.add_log_level,
        structlog.stdlib.add_logger_name,
        structlog.processors.TimeStamper(fmt="iso", utc=True),
    ]

    if include_caller:
        shared_processors.append(
            structlog.processors.CallsiteParameterAdder(
                parameters=[
                    structlog.processors.CallsiteParameter.MODULE,
                    structlog.processors.CallsiteParameter.LINENO,
                ]
            )
        )

    if fmt == "json":
        structlog.processors.JSONRenderer()
    else:
        structlog.dev.ConsoleRenderer(colors=True)

    structlog.configure(
        processors=[
            *shared_processors,
            structlog.stdlib.PositionalArgumentsFormatter(),
            structlog.processors.StackInfoRenderer(),
            structlog.processors.format_exc_info,
            structlog.stdlib.ProcessorFormatter.wrap_for_formatter,
        ],
        wrapper_class=structlog.make_filtering_bound_logger(logging.getLevelName(level.upper())),
        context_class=dict,
        logger_factory=structlog.PrintLoggerFactory(file=sys.stderr),
        cache_logger_on_first_use=True,
    )

    logging.basicConfig(
        format="%(message)s",
        stream=sys.stderr,
        level=logging.getLevelName(level.upper()),
    )


def get_logger(name: str | None = None) -> structlog.stdlib.BoundLogger:
    """Return a bound structlog logger for the given module name."""
    return structlog.get_logger(name)
