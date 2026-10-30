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

This layout is directly compatible with tools like HDFView, h5py, and xarray.
"""

from __future__ import annotations

from collections.abc import Sequence
from pathlib import Path
from typing import Annotated

import h5py
import numpy as np
from numpy.typing import NDArray
from pydantic import BaseModel, Field

from mdp.core.base import Loader
from mdp.core.registry import registry
from mdp.models.dataset import TelemetryDataset
from mdp.models.parameter import EngineeringParameter

_ROOT_GROUP = "telemetry"


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
        if unit and stored_unit and stored_unit != unit:
            raise ValueError(f"Parameter '{name}' mixes units: {stored_unit} and {unit}")

        # Every column of a parameter must stay the same length, so the value column
        # is settled before anything is written.
        typed = _typed_values(eng_values)
        if typed is None and "eng_value" in grp:
            raise ValueError(f"Parameter '{name}' has numeric samples and now non-numeric ones")
        if typed is not None and "eng_value" in grp and not _fits(typed, grp["eng_value"].dtype):
            raise ValueError(
                f"Parameter '{name}': {typed.dtype} values do not fit its "
                f"{grp['eng_value'].dtype} column"
            )

        self._append_or_create(grp, "time_tai", times, dtype=np.float64)
        self._append_or_create(grp, "apid", apids, dtype=np.int32)
        self._append_or_create(grp, "seq_count", seqs, dtype=np.int32)
        self._append_or_create(grp, "validity", validity, dtype=np.uint8)

        if typed is not None and "eng_value_str" not in grp:
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


def _typed_values(values: Sequence[object]) -> NDArray[np.generic] | None:
    """An array of the values' own type, or None for anything that is not numeric."""
    if all(isinstance(v, bool) for v in values):
        return np.array(values, dtype=np.bool_)
    if any(isinstance(v, bool) or not isinstance(v, (int, float)) for v in values):
        return None
    if all(isinstance(v, int) for v in values):
        integers = [int(v) for v in values]  # type: ignore[call-overload]
        return np.array(integers, dtype=np.uint64 if max(integers) >= 2**63 else np.int64)
    if any(isinstance(v, int) and abs(v) > 2**53 for v in values):
        raise ValueError("Integers beyond 2^53 cannot share a column with floats")
    return np.array(values, dtype=np.float64)


def _fits(values: NDArray[np.generic], column: np.dtype[np.generic]) -> bool:
    """Whether every value keeps its exact meaning in the existing column."""
    if values.dtype == column:
        return True
    if column == np.float64 and values.dtype.kind in "iu":
        return bool(np.all(np.abs(values.astype(np.float64)) <= 2**53))
    if column == np.int64 and values.dtype == np.uint64:
        return False
    if column == np.uint64 and values.dtype == np.int64:
        return bool(np.all(values.astype(np.int64) >= 0))
    return False
