"""Telemetry parameter models — raw and engineering-unit values."""

from __future__ import annotations

from enum import StrEnum
from typing import Annotated, Union

from pydantic import BaseModel, Field


class ParameterType(StrEnum):
    UINT = "uint"
    INT = "int"
    FLOAT = "float"
    DOUBLE = "double"
    BOOLEAN = "boolean"
    ENUMERATED = "enumerated"
    BINARY = "binary"
    STRING = "string"


RawValue = Union[int, float, bytes, str, bool]
EngValue = Union[int, float, str, bool]


class RawParameter(BaseModel):
    """A single raw (pre-calibration) parameter extracted from a packet."""

    model_config = {"frozen": True}

    name: str
    apid: int
    seq_count: int
    sample_time_tai: float = Field(description="TAI seconds since J2000")
    raw_value: RawValue
    param_type: ParameterType
    bit_offset: int | None = None
    bit_length: int | None = None


class EngineeringParameter(BaseModel):
    """A calibrated, engineering-unit parameter value."""

    model_config = {"frozen": True}

    name: str
    apid: int
    seq_count: int
    sample_time_tai: float
    raw_value: RawValue
    eng_value: EngValue
    unit: str | None = None
    validity: bool = True
    calibration_id: str | None = None
    out_of_limit: bool = False
    alarm_level: Annotated[int, Field(ge=0, le=3)] = 0


class ParameterRecord(BaseModel):
    """A time-ordered record of engineering parameters for a single parameter name."""

    model_config = {"frozen": True}

    name: str
    unit: str | None = None
    samples: list[EngineeringParameter] = Field(default_factory=list)

    @property
    def count(self) -> int:
        return len(self.samples)

    @property
    def time_range(self) -> tuple[float, float] | None:
        if not self.samples:
            return None
        times = [s.sample_time_tai for s in self.samples]
        return min(times), max(times)
