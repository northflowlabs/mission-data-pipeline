"""HDF5 loader: persists telemetry data to HDF5/NetCDF-compatible files.

Data layout
-----------
All parameters from one run are written to a single HDF5 file. The group
hierarchy is::

    /telemetry/
        <param_name>/
            time_tai       (float64 dataset)
            eng_value      (int64, uint64, float64 or bool dataset), or
            eng_value_str  (string dataset), according to the values' type
            apid           (int32 dataset)
            seq_count      (int32 dataset)
            validity       (uint8 dataset)
            attrs:         unit

Values are stored by their type. Text stays text, even when it looks like a number,
and integers are not passed through float64, so 64-bit counters keep every digit.

A parameter holds one kind of value, and the outcome does not depend on how a run is
split into batches. Integers and floats share a float64 column, and a column is rewritten
to it when floats arrive after integers. Numbers, text and booleans in one parameter
fail the run, as do integers that no single type can hold exactly.

This layout is directly compatible with tools like HDFView, h5py, and xarray.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Annotated, Any

import h5py
import numpy as np
from numpy.typing import NDArray
from pydantic import BaseModel, Field

from mdp.core.base import Loader
from mdp.core.registry import registry
from mdp.models.dataset import TelemetryDataset
from mdp.models.parameter import EngineeringParameter

_ROOT_GROUP = "telemetry"
_MAX_EXACT_FLOAT = 2**53


class HDF5LoaderConfig(BaseModel):
    path: Path = Field(description="Output HDF5 file path (.h5 or .hdf5)")
    mode: str = Field(
        default="w",
        description=(
            "How a run opens the file: 'w' replaces earlier runs, 'a' adds to them "
            "(creating the file), 'r+' adds to an existing file. Batches of one run "
            "always accumulate."
        ),
    )
    compression: str = "gzip"
    compression_opts: Annotated[int, Field(ge=0, le=9)] = 4
    flush_on_batch: bool = True


@registry.loader("hdf5")
class HDF5Loader(Loader[HDF5LoaderConfig]):
    """Write TelemetryDataset parameters to an HDF5 file."""

    config_class = HDF5LoaderConfig

    def __init__(self, config: HDF5LoaderConfig) -> None:
        super().__init__(config)
        self._opened = False

    def begin_run(self) -> None:
        self._opened = False

    def setup(self) -> None:
        self.config.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self, dataset: TelemetryDataset) -> None:
        # mode applies to the file as a whole: "w" must not truncate between batches.
        mode = "a" if self._opened else self.config.mode
        with h5py.File(self.config.path, mode) as hf:
            self._opened = True
            root = hf.require_group(_ROOT_GROUP)

            for name in dataset.parameter_names():
                record = dataset.get_parameter(name)
                if record is None or not record.samples:
                    continue
                self._write_parameter(root, name, record.samples, record.unit)

            if self.config.flush_on_batch:
                hf.flush()

    def _write_parameter(
        self,
        root: h5py.Group,
        name: str,
        samples: list[EngineeringParameter],
        unit: str | None,
    ) -> None:
        times = np.array([s.sample_time_tai for s in samples], dtype=np.float64)
        apids = np.array([s.apid for s in samples], dtype=np.int32)
        seqs = np.array([s.seq_count for s in samples], dtype=np.int32)
        validity = np.array([int(s.validity) for s in samples], dtype=np.uint8)

        eng_values = [s.eng_value for s in samples]

        grp = root.require_group(name)

        stored_unit = grp.attrs.get("unit")
        if "time_tai" in grp and stored_unit != unit:
            raise ValueError(f"Parameter '{name}' mixes units: {stored_unit} and {unit}")

        # Every column of a parameter must stay the same length, so the value column
        # is settled before anything is written.
        typed = _typed_values(name, eng_values)
        if ("eng_value" if typed is None else "eng_value_str") in grp:
            raise _mixed_kinds(name)
        if typed is not None and "eng_value" in grp:
            column = grp["eng_value"]
            common = _common_dtype(name, typed, column.dtype)
            if common != column.dtype:
                stored = column[()]
                _common_dtype(name, stored, typed.dtype)
                del grp["eng_value"]
                self._append_or_create(grp, "eng_value", stored.astype(common))

        self._append_or_create(grp, "time_tai", times, dtype=np.float64)
        self._append_or_create(grp, "apid", apids, dtype=np.int32)
        self._append_or_create(grp, "seq_count", seqs, dtype=np.int32)
        self._append_or_create(grp, "validity", validity, dtype=np.uint8)

        if typed is not None:
            self._append_or_create(grp, "eng_value", typed)
        else:
            encoded = np.array([str(v) for v in eng_values], dtype=h5py.string_dtype())
            self._append_or_create(grp, "eng_value_str", encoded)

        if unit:
            grp.attrs["unit"] = unit

    def _append_or_create(
        self,
        grp: h5py.Group,
        name: str,
        data: NDArray[np.generic],
        dtype: type | None = None,
    ) -> None:
        if name not in grp:
            maxshape = (None,) + data.shape[1:]
            grp.create_dataset(
                name,
                data=data,
                maxshape=maxshape,
                compression=self.config.compression,
                compression_opts=self.config.compression_opts,
                dtype=dtype,
                chunks=True,
            )
        else:
            ds = grp[name]
            old_size = ds.shape[0]
            new_size = old_size + len(data)
            ds.resize(new_size, axis=0)
            ds[old_size:] = data


def _mixed_kinds(name: str) -> ValueError:
    return ValueError(
        f"Parameter '{name}' mixes numbers, text or booleans, and a parameter holds one kind "
        "of value at any batch size. Clean the data or declare the type, for CSV with "
        "parameter_types: a float column reads nan as a number."
    )


def _typed_values(name: str, values: Sequence[object]) -> NDArray[np.generic] | None:
    """An array of the values' own type, or None for text. Mixed kinds are refused."""
    if all(isinstance(v, bool) for v in values):
        return np.array(values, dtype=np.bool_)
    if not any(isinstance(v, (bool, int, float)) for v in values):
        return None
    if any(isinstance(v, bool) or not isinstance(v, (int, float)) for v in values):
        raise _mixed_kinds(name)
    if all(isinstance(v, int) for v in values):
        integers = [int(v) for v in values]  # type: ignore[call-overload]
        if max(integers) < 2**63:
            return np.array(integers, dtype=np.int64)
        if min(integers) < 0:
            raise _no_common_integer(name)
        return np.array(integers, dtype=np.uint64)
    if any(isinstance(v, int) and abs(v) > _MAX_EXACT_FLOAT for v in values):
        raise _no_exact_float(name)
    return np.array(values, dtype=np.float64)


def _no_exact_float(name: str) -> ValueError:
    return ValueError(f"Parameter '{name}': integers beyond 2^53 cannot share a column with floats")


def _no_common_integer(name: str) -> ValueError:
    return ValueError(
        f"Parameter '{name}': negative integers cannot share a column with integers from 2^63"
    )


def _common_dtype(
    name: str, values: NDArray[Any], other: np.dtype[np.generic]
) -> np.dtype[np.generic]:
    """The type that holds *values* and a column of *other* exactly, or a refusal."""
    kinds = {values.dtype.kind, other.kind}
    if len(kinds) == 1:
        return other
    if "b" in kinds:
        raise _mixed_kinds(name)
    is_integer = values.dtype.kind in "iu"
    low, high = (int(np.min(values)), int(np.max(values))) if is_integer else (0, 0)
    if "f" in kinds:
        if max(-low, high) > _MAX_EXACT_FLOAT:
            raise _no_exact_float(name)
        return np.dtype(np.float64)
    if low < 0:
        raise _no_common_integer(name)
    return np.dtype(np.uint64)
