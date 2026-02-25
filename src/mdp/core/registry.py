"""Stage registry — allows plugins to self-register and be discovered by name."""

from __future__ import annotations

from typing import TYPE_CHECKING, Any

if TYPE_CHECKING:
    from mdp.core.base import Extractor, Loader, Transformer


class StageRegistry:
    """A simple name → class registry for pipeline stages.

    Plugins register themselves with::

        @registry.extractor("my_extractor")
        class MyExtractor(Extractor[MyConfig]):
            ...

    And are retrieved later::

        cls = registry.get_extractor("my_extractor")
    """

    def __init__(self) -> None:
        self._extractors: dict[str, type] = {}
        self._transformers: dict[str, type] = {}
        self._loaders: dict[str, type] = {}

    # ------------------------------------------------------------------ #
    #  Registration decorators                                             #
    # ------------------------------------------------------------------ #

    def extractor(self, name: str) -> Any:
        def _decorator(cls: type) -> type:
            self._extractors[name] = cls
            cls._registry_name = name  # type: ignore[attr-defined]
            return cls
        return _decorator

    def transformer(self, name: str) -> Any:
        def _decorator(cls: type) -> type:
            self._transformers[name] = cls
            cls._registry_name = name  # type: ignore[attr-defined]
            return cls
        return _decorator

    def loader(self, name: str) -> Any:
        def _decorator(cls: type) -> type:
            self._loaders[name] = cls
            cls._registry_name = name  # type: ignore[attr-defined]
            return cls
        return _decorator

    # ------------------------------------------------------------------ #
    #  Lookup                                                              #
    # ------------------------------------------------------------------ #

    def get_extractor(self, name: str) -> type:
        try:
            return self._extractors[name]
        except KeyError:
            available = list(self._extractors)
            raise KeyError(f"Unknown extractor '{name}'. Available: {available}") from None

    def get_transformer(self, name: str) -> type:
        try:
            return self._transformers[name]
        except KeyError:
            available = list(self._transformers)
            raise KeyError(f"Unknown transformer '{name}'. Available: {available}") from None

    def get_loader(self, name: str) -> type:
        try:
            return self._loaders[name]
        except KeyError:
            available = list(self._loaders)
            raise KeyError(f"Unknown loader '{name}'. Available: {available}") from None

    # ------------------------------------------------------------------ #
    #  Introspection                                                       #
    # ------------------------------------------------------------------ #

    def list_extractors(self) -> list[str]:
        return sorted(self._extractors)

    def list_transformers(self) -> list[str]:
        return sorted(self._transformers)

    def list_loaders(self) -> list[str]:
        return sorted(self._loaders)

    def all_stages(self) -> dict[str, list[str]]:
        return {
            "extractors": self.list_extractors(),
            "transformers": self.list_transformers(),
            "loaders": self.list_loaders(),
        }


registry = StageRegistry()
