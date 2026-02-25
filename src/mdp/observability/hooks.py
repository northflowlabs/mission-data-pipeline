"""Event hooks — a lightweight publish/subscribe mechanism for pipeline events.

Hooks allow external code (monitoring, alerting, custom logging) to react
to pipeline lifecycle events without modifying the core pipeline logic.

Example::

    hooks = HookManager()

    @hooks.on("batch.complete")
    def on_batch(dataset, result):
        prometheus_counter.inc(len(dataset))

    pipeline = Pipeline(config, extractor, loader=loader, hooks=hooks)
"""

from __future__ import annotations

import contextlib
from collections import defaultdict
from collections.abc import Callable

EventHandler = Callable[..., None]


class EventHook:
    """A named event that can have multiple handlers attached."""

    def __init__(self, name: str) -> None:
        self.name = name
        self._handlers: list[EventHandler] = []

    def register(self, handler: EventHandler) -> None:
        self._handlers.append(handler)

    def unregister(self, handler: EventHandler) -> None:
        with contextlib.suppress(ValueError):
            self._handlers.remove(handler)

    def fire(self, *args: object, **kwargs: object) -> None:
        """Invoke all registered handlers, swallowing individual errors."""
        for handler in list(self._handlers):
            with contextlib.suppress(Exception):
                handler(*args, **kwargs)

    def __len__(self) -> int:
        return len(self._handlers)


class HookManager:
    """Registry of named EventHooks.

    Built-in pipeline events
    ------------------------
    ``pipeline.start``          — fired before the extractor begins
    ``pipeline.complete``       — fired after all batches are processed
    ``batch.extracted``         — fired after each extractor yield
    ``batch.transformed``       — fired after all transformers process a batch
    ``batch.loaded``            — fired after the loader persists a batch
    ``stage.error``             — fired whenever any stage raises an exception
    """

    BUILTIN_EVENTS = (
        "pipeline.start",
        "pipeline.complete",
        "batch.extracted",
        "batch.transformed",
        "batch.loaded",
        "stage.error",
    )

    def __init__(self) -> None:
        self._hooks: dict[str, EventHook] = defaultdict(lambda: EventHook("<dynamic>"))
        for event_name in self.BUILTIN_EVENTS:
            self._hooks[event_name] = EventHook(event_name)

    def on(self, event: str) -> Callable[[EventHandler], EventHandler]:
        """Decorator to register a handler for a named event."""

        def _decorator(handler: EventHandler) -> EventHandler:
            self._hooks[event].register(handler)
            return handler

        return _decorator

    def register(self, event: str, handler: EventHandler) -> None:
        self._hooks[event].register(handler)

    def fire(self, event: str, *args: object, **kwargs: object) -> None:
        if event in self._hooks:
            self._hooks[event].fire(*args, **kwargs)

    def get_hook(self, event: str) -> EventHook | None:
        return self._hooks.get(event)

    def registered_events(self) -> list[str]:
        return [name for name, hook in self._hooks.items() if len(hook) > 0]
