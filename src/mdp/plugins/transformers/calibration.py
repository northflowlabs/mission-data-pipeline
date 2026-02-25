"""Calibration transformer — applies polynomial or table-lookup calibrations.

Two calibration types are supported:

``polynomial``
    eng_value = c0 + c1*raw + c2*raw^2 + ... + cN*raw^N

``table``
    eng_value = piecewise linear interpolation of (raw, eng) point pairs
"""

from __future__ import annotations

import bisect
from enum import StrEnum
from typing import Annotated

from pydantic import BaseModel, Field

from mdp.core.base import Transformer
from mdp.core.registry import registry
from mdp.models.dataset import TelemetryDataset
from mdp.models.parameter import EngineeringParameter


class CalibrationMethod(StrEnum):
    POLYNOMIAL = "polynomial"
    TABLE = "table"
    IDENTITY = "identity"


class CalibrationEntry(BaseModel):
    """A single calibration specification for one parameter."""

    model_config = {"frozen": True}

    parameter_name: str
    method: CalibrationMethod = CalibrationMethod.IDENTITY
    unit: str | None = None

    coefficients: list[float] | None = Field(
        default=None,
        description="Polynomial coefficients [c0, c1, c2, ...] (low to high order)",
    )
    table_raw: list[float] | None = Field(
        default=None,
        description="Raw values for table interpolation (must be monotonically increasing)",
    )
    table_eng: list[float] | None = Field(
        default=None,
        description="Engineering values corresponding to table_raw",
    )

    def apply(self, raw: float) -> float:
        if self.method == CalibrationMethod.POLYNOMIAL:
            if not self.coefficients:
                return raw
            result = 0.0
            for power, coeff in enumerate(self.coefficients):
                result += coeff * (raw ** power)
            return result

        if self.method == CalibrationMethod.TABLE:
            if not self.table_raw or not self.table_eng:
                return raw
            return _interpolate(raw, self.table_raw, self.table_eng)

        return raw


class CalibrationConfig(BaseModel):
    calibrations: list[CalibrationEntry]
    mark_uncalibrated_invalid: bool = False


@registry.transformer("calibration")
class CalibrationTransformer(Transformer[CalibrationConfig]):
    """Apply calibrations to engineering parameters already in the dataset."""

    config_class = CalibrationConfig

    def setup(self) -> None:
        self._cal_map: dict[str, CalibrationEntry] = {
            c.parameter_name: c for c in self.config.calibrations
        }

    def transform(self, dataset: TelemetryDataset) -> TelemetryDataset:
        for name, record in dataset.parameters.items():
            cal = self._cal_map.get(name)
            if cal is None:
                continue

            calibrated: list[EngineeringParameter] = []
            for sample in record.samples:
                try:
                    raw = float(sample.raw_value)  # type: ignore[arg-type]
                    eng = cal.apply(raw)
                    calibrated.append(
                        EngineeringParameter(
                            name=sample.name,
                            apid=sample.apid,
                            seq_count=sample.seq_count,
                            sample_time_tai=sample.sample_time_tai,
                            raw_value=sample.raw_value,
                            eng_value=eng,
                            unit=cal.unit or sample.unit,
                            validity=sample.validity,
                            calibration_id=cal.method,
                            out_of_limit=sample.out_of_limit,
                            alarm_level=sample.alarm_level,
                        )
                    )
                except (TypeError, ValueError):
                    calibrated.append(sample)

            object.__setattr__(record, "samples", calibrated)
            if cal.unit:
                object.__setattr__(record, "unit", cal.unit)

        return dataset


def _interpolate(x: float, xs: list[float], ys: list[float]) -> float:
    """Linear interpolation / extrapolation over (xs, ys) point pairs."""
    if x <= xs[0]:
        return ys[0]
    if x >= xs[-1]:
        return ys[-1]
    idx = bisect.bisect_right(xs, x) - 1
    x0, x1 = xs[idx], xs[idx + 1]
    y0, y1 = ys[idx], ys[idx + 1]
    t = (x - x0) / (x1 - x0)
    return y0 + t * (y1 - y0)
