"""HDF5 loader — persists telemetry data to HDF5/NetCDF-compatible files.

Data layout
-----------
All parameters from one dataset batch are written to a single HDF5 file.
The group hierarchy is::

    /telemetry/
        <param_name>/
            time_tai    (float64 dataset)
            eng_value   (float64 | string dataset)
            raw_value   (as-stored)
            apid        (int32 dataset)
            seq_count   (int32 dataset)
            attrs:      unit, validity_count, apid_list

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
        default="a",
        description="File open mode: 'w' (overwrite), 'a' (append/create), 'r+' (append only)",
    )
    compression: str = "gzip"
    compression_opts: Annotated[int, Field(ge=0, le=9)] = 4
    flush_on_batch: bool = True


@registry.loader("hdf5")
class HDF5Loader(Loader[HDF5LoaderConfig]):
    """Write TelemetryDataset parameters to an HDF5 file."""

    config_class = HDF5LoaderConfig

    def setup(self) -> None:
        self.config.path.parent.mkdir(parents=True, exist_ok=True)

    def load(self, dataset: TelemetryDataset) -> None:
        with h5py.File(self.config.path, self.config.mode) as hf:
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
        [s.raw_value for s in samples]

        grp = root.require_group(name)

        self._append_or_create(grp, "time_tai", times, dtype=np.float64)
        self._append_or_create(grp, "apid", apids, dtype=np.int32)
        self._append_or_create(grp, "seq_count", seqs, dtype=np.int32)
        self._append_or_create(grp, "validity", validity, dtype=np.uint8)

        eng_arr = self._coerce_numeric(eng_values)
        if eng_arr is not None:
            self._append_or_create(grp, "eng_value", eng_arr, dtype=np.float64)
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
            )
        else:
            ds = grp[name]
            old_size = ds.shape[0]
            new_size = old_size + len(data)
            ds.resize(new_size, axis=0)
            ds[old_size:] = data

    @staticmethod
    def _coerce_numeric(values: Sequence[object]) -> NDArray[np.float64] | None:
        try:
            arr: NDArray[np.float64] = np.array(values, dtype=np.float64)
            return arr
        except (TypeError, ValueError):
            return None
