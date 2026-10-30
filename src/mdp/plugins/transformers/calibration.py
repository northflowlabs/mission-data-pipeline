"""Calibration transformer: applies polynomial or table-lookup calibrations.

Two calibration types are supported:

``polynomial``
    eng_value = c0 + c1*raw + c2*raw^2 + ... + cN*raw^N

``table``
    eng_value = piecewise linear interpolation of (raw, eng) point pairs. Outside the
    table the value is held at the nearest end and the sample is flagged out_of_limit.
"""

from __future__ import annotations

import bisect
from enum import Enum
from itertools import pairwise

from pydantic import BaseModel, Field, model_validator

from mdp.core.base import Transformer
from mdp.core.registry import registry
from mdp.models.dataset import TelemetryDataset
from mdp.models.parameter import EngineeringParameter


class CalibrationMethod(str, Enum):
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

    @model_validator(mode="after")
    def _check_definition(self) -> CalibrationEntry:
        name = self.parameter_name
        if self.method == CalibrationMethod.POLYNOMIAL and not self.coefficients:
            raise ValueError(f"{name}: a polynomial calibration needs coefficients")
        if self.method == CalibrationMethod.TABLE:
            if not self.table_raw or not self.table_eng:
                raise ValueError(f"{name}: a table calibration needs table_raw and table_eng")
            if len(self.table_raw) != len(self.table_eng):
                raise ValueError(f"{name}: table_raw and table_eng differ in length")
            if any(a >= b for a, b in pairwise(self.table_raw)):
                raise ValueError(f"{name}: table_raw must be strictly increasing")
        if self.method == CalibrationMethod.IDENTITY and self.unit:
            raise ValueError(f"{name}: an identity calibration cannot change the unit")
        return self

    def in_range(self, raw: float) -> bool:
        """False when *raw* lies outside the calibration table."""
        if self.method != CalibrationMethod.TABLE or not self.table_raw:
            return True
        return self.table_raw[0] <= raw <= self.table_raw[-1]

    def apply(self, raw: float) -> float:
        if self.method == CalibrationMethod.POLYNOMIAL:
            result = 0.0
            for power, coeff in enumerate(self.coefficients or []):
                result += coeff * (raw**power)
            return result

        if self.method == CalibrationMethod.TABLE:
            return _interpolate(raw, self.table_raw or [], self.table_eng or [])

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
                if self.config.mark_uncalibrated_invalid:
                    invalid = [s.model_copy(update={"validity": False}) for s in record.samples]
                    object.__setattr__(record, "samples", invalid)
                continue

            calibrated: list[EngineeringParameter] = []
            for sample in record.samples:
                if cal.method == CalibrationMethod.IDENTITY:
                    value = sample.raw_value if sample.eng_value is None else sample.eng_value
                    calibrated.append(
                        sample.model_copy(
                            update={"eng_value": value, "calibration_id": cal.method.value}
                        )
                    )
                    continue
                try:
                    raw = float(sample.raw_value)  # type: ignore[arg-type]
                except (TypeError, ValueError):
                    if cal.unit and sample.unit and cal.unit != sample.unit:
                        raise ValueError(
                            f"Parameter '{name}': {sample.raw_value!r} cannot be converted "
                            f"from {sample.unit} to {cal.unit}, so one record would mix units"
                        ) from None
                    if self.config.mark_uncalibrated_invalid:
                        sample = sample.model_copy(update={"validity": False})
                    calibrated.append(sample)
                    continue
                calibrated.append(
                    sample.model_copy(
                        update={
                            "eng_value": cal.apply(raw),
                            "unit": cal.unit or sample.unit,
                            "calibration_id": cal.method.value,
                            "out_of_limit": sample.out_of_limit or not cal.in_range(raw),
                        }
                    )
                )

            object.__setattr__(record, "samples", calibrated)
            if cal.unit:
                object.__setattr__(record, "unit", cal.unit)

        return dataset


def _interpolate(x: float, xs: list[float], ys: list[float]) -> float:
    """Linear interpolation over (xs, ys) point pairs, held at the ends outside them."""
    if x <= xs[0]:
        return ys[0]
    if x >= xs[-1]:
        return ys[-1]
    idx = bisect.bisect_right(xs, x) - 1
    x0, x1 = xs[idx], xs[idx + 1]
    y0, y1 = ys[idx], ys[idx + 1]
    t = (x - x0) / (x1 - x0)
    return y0 + t * (y1 - y0)
