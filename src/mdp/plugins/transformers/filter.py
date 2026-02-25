"""APID filter transformer — drop packets with unwanted APIDs."""

from __future__ import annotations

from pydantic import BaseModel, Field

from mdp.core.base import Transformer
from mdp.core.registry import registry
from mdp.models.dataset import TelemetryDataset


class ApidFilterConfig(BaseModel):
    include: list[int] | None = Field(
        default=None,
        description="Whitelist — only these APIDs are kept. Mutually exclusive with ``exclude``.",
    )
    exclude: list[int] | None = Field(
        default=None,
        description="Blacklist — these APIDs are dropped.",
    )

    def model_post_init(self, __context: object) -> None:
        if self.include is not None and self.exclude is not None:
            raise ValueError("Specify either 'include' or 'exclude', not both.")


@registry.transformer("apid_filter")
class ApidFilterTransformer(Transformer[ApidFilterConfig]):
    """Keep or remove packets based on their APID."""

    config_class = ApidFilterConfig

    def transform(self, dataset: TelemetryDataset) -> TelemetryDataset:
        original = dataset.packets
        if self.config.include is not None:
            allowed = set(self.config.include)
            filtered = [p for p in original if p.apid in allowed]
        elif self.config.exclude is not None:
            blocked = set(self.config.exclude)
            filtered = [p for p in original if p.apid not in blocked]
        else:
            return dataset

        dataset.packets.clear()
        dataset.packets.extend(filtered)
        return dataset
