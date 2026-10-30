"""TelemetryDataset: the primary in-memory container passed between pipeline stages."""

from __future__ import annotations

import math
from collections.abc import Iterator
from dataclasses import dataclass, field

import pandas as pd

from mdp.models.packet import TelemetryPacket
from mdp.models.parameter import EngineeringParameter, ParameterRecord


def _check_units(name: str, first: str | None, second: str | None) -> None:
    if first and second and first != second:
        raise ValueError(f"Parameter '{name}' mixes units: {first} and {second}")


@dataclass
class TelemetryDataset:
    """Container for a batch of telemetry data flowing through the pipeline.

    Designed to be lightweight and mutable during the Transform stage,
    then frozen/serialised during the Load stage.
    """

    packets: list[TelemetryPacket] = field(default_factory=list)
    parameters: dict[str, ParameterRecord] = field(default_factory=dict)
    metadata: dict[str, object] = field(default_factory=dict)

    # ------------------------------------------------------------------ #
    #  Packet helpers                                                       #
    # ------------------------------------------------------------------ #

    def add_packet(self, packet: TelemetryPacket) -> None:
        self.packets.append(packet)

    def packets_by_apid(self, apid: int) -> list[TelemetryPacket]:
        return [p for p in self.packets if p.apid == apid]

    def iter_packets(self) -> Iterator[TelemetryPacket]:
        yield from self.packets

    # ------------------------------------------------------------------ #
    #  Parameter helpers                                                    #
    # ------------------------------------------------------------------ #

    def add_parameter(self, param: EngineeringParameter) -> None:
        if param.name not in self.parameters:
            self.parameters[param.name] = ParameterRecord(name=param.name, unit=param.unit)
        record = self.parameters[param.name]
        _check_units(param.name, record.unit, param.unit)
        object.__setattr__(
            record,
            "samples",
            record.samples + [param],
        )

    def get_parameter(self, name: str) -> ParameterRecord | None:
        return self.parameters.get(name)

    def parameter_names(self) -> list[str]:
        return list(self.parameters.keys())

    # ------------------------------------------------------------------ #
    #  DataFrame export                                                     #
    # ------------------------------------------------------------------ #

    def sample_count(self) -> int:
        """Total parameter samples held across all parameters."""
        return sum(len(self.parameters[name].samples) for name in self.parameters)

    def to_dataframe(self, parameter_name: str) -> pd.DataFrame:
        """Return a tidy DataFrame for a single parameter time-series."""
        record = self.parameters.get(parameter_name)
        if record is None:
            raise KeyError(f"Parameter '{parameter_name}' not found in dataset")
        rows = [
            {
                "time_tai": s.sample_time_tai,
                "apid": s.apid,
                "seq_count": s.seq_count,
                "raw_value": s.raw_value,
                "eng_value": s.eng_value,
                "unit": s.unit,
                "validity": s.validity,
                "out_of_limit": s.out_of_limit,
                "alarm_level": s.alarm_level,
            }
            for s in record.samples
        ]
        return pd.DataFrame(rows).sort_values(["time_tai", "seq_count"]).reset_index(drop=True)

    def to_wide_dataframe(self) -> pd.DataFrame:
        """Return a wide DataFrame with one column per parameter, indexed by TAI time.

        Raises ValueError if a parameter has no sample time, or two samples at one
        time, because such samples cannot share a row.
        """
        frames = {}
        for name, record in self.parameters.items():
            times = [s.sample_time_tai for s in record.samples]
            if any(math.isnan(t) for t in times):
                raise ValueError(
                    f"Parameter '{name}' has samples without a time; set source_time_tai "
                    "or use the per-parameter output"
                )
            if len(set(times)) != len(times):
                raise ValueError(
                    f"Parameter '{name}' has several samples at one time; "
                    "use the per-parameter output"
                )
            frames[name] = pd.Series([s.eng_value for s in record.samples], index=times, name=name)
        if not frames:
            return pd.DataFrame()
        wide = pd.DataFrame(frames).sort_index()
        wide.attrs["units"] = {n: r.unit for n, r in self.parameters.items() if r.unit}
        return wide

    # ------------------------------------------------------------------ #
    #  Dunder helpers                                                       #
    # ------------------------------------------------------------------ #

    def __len__(self) -> int:
        return len(self.packets)

    def __repr__(self) -> str:
        return (
            f"TelemetryDataset("
            f"packets={len(self.packets)}, "
            f"parameters={len(self.parameters)}, "
            f"metadata_keys={list(self.metadata.keys())})"
        )

    def merge(self, other: TelemetryDataset) -> TelemetryDataset:
        """Non-destructively merge two datasets into a new one."""
        merged = TelemetryDataset(
            packets=self.packets + other.packets,
            metadata={**self.metadata, **other.metadata},
        )
        for source in (self, other):
            for name, record in source.parameters.items():
                existing = merged.parameters.get(name)
                if existing:
                    _check_units(name, existing.unit, record.unit)
                samples = existing.samples + record.samples if existing else record.samples
                merged.parameters[name] = ParameterRecord(
                    name=name,
                    unit=(existing.unit or record.unit) if existing else record.unit,
                    samples=list(samples),
                )
        return merged
