"""Tests for Parquet, HDF5, and CSV loaders."""

from __future__ import annotations

from pathlib import Path

import pandas as pd
import pytest

from mdp.models.dataset import TelemetryDataset
from mdp.models.parameter import EngineeringParameter
from mdp.plugins.loaders.csv import CsvLoader, CsvLoaderConfig
from mdp.plugins.loaders.parquet import ParquetLoader, ParquetLoaderConfig
from mdp.plugins.loaders.hdf5 import HDF5Loader, HDF5LoaderConfig


# --------------------------------------------------------------------------- #
#  Shared fixture                                                              #
# --------------------------------------------------------------------------- #


def _make_dataset(n: int = 5, param: str = "temperature") -> TelemetryDataset:
    ds = TelemetryDataset()
    for i in range(n):
        ds.add_parameter(
            EngineeringParameter(
                name=param,
                apid=0x100,
                seq_count=i,
                sample_time_tai=float(i * 10),
                raw_value=i * 100,
                eng_value=float(20 + i),
                unit="degC",
            )
        )
    return ds


# --------------------------------------------------------------------------- #
#  Parquet Loader                                                              #
# --------------------------------------------------------------------------- #


class TestParquetLoader:
    def test_writes_per_parameter(self, tmp_path: Path) -> None:
        ds = _make_dataset()
        cfg = ParquetLoaderConfig(output_dir=tmp_path)
        loader = ParquetLoader(cfg)
        loader.setup()
        loader.load(ds)
        out = tmp_path / "temperature.parquet"
        assert out.exists()
        df = pd.read_parquet(out)
        assert len(df) == 5
        assert "eng_value" in df.columns
        assert "time_tai" in df.columns

    def test_wide_format(self, tmp_path: Path) -> None:
        ds = _make_dataset(param="temp")
        for i in range(3):
            ds.add_parameter(
                EngineeringParameter(
                    name="voltage",
                    apid=0x100,
                    seq_count=i,
                    sample_time_tai=float(i * 10),
                    raw_value=float(i),
                    eng_value=float(i * 3.3),
                )
            )
        cfg = ParquetLoaderConfig(output_dir=tmp_path, wide_format=True)
        loader = ParquetLoader(cfg)
        loader.setup()
        loader.load(ds)
        out = tmp_path / "telemetry.parquet"
        assert out.exists()
        df = pd.read_parquet(out)
        assert "temp" in df.columns

    def test_append_mode(self, tmp_path: Path) -> None:
        ds1 = _make_dataset(n=3)
        ds2 = _make_dataset(n=2)
        cfg = ParquetLoaderConfig(output_dir=tmp_path, overwrite=False)
        loader = ParquetLoader(cfg)
        loader.setup()
        loader.load(ds1)
        loader.load(ds2)
        df = pd.read_parquet(tmp_path / "temperature.parquet")
        assert len(df) == 5

    def test_partition_by_apid(self, tmp_path: Path) -> None:
        ds = _make_dataset()
        cfg = ParquetLoaderConfig(output_dir=tmp_path, partition_by_apid=True)
        loader = ParquetLoader(cfg)
        loader.setup()
        loader.load(ds)
        subdirs = [d for d in tmp_path.iterdir() if d.is_dir()]
        assert len(subdirs) == 1
        assert "apid=0100" in subdirs[0].name.upper() or subdirs[0].name.startswith("apid=")


# --------------------------------------------------------------------------- #
#  HDF5 Loader                                                                 #
# --------------------------------------------------------------------------- #


class TestHDF5Loader:
    def test_writes_hdf5(self, tmp_path: Path) -> None:
        import h5py

        ds = _make_dataset()
        cfg = HDF5LoaderConfig(path=tmp_path / "telem.h5")
        loader = HDF5Loader(cfg)
        loader.setup()
        loader.load(ds)

        assert cfg.path.exists()
        with h5py.File(cfg.path, "r") as hf:
            assert "telemetry" in hf
            assert "temperature" in hf["telemetry"]
            times = hf["telemetry"]["temperature"]["time_tai"][:]
            assert len(times) == 5

    def test_append_mode(self, tmp_path: Path) -> None:
        import h5py

        ds = _make_dataset(n=3)
        cfg = HDF5LoaderConfig(path=tmp_path / "telem.h5", mode="w")
        loader = HDF5Loader(cfg)
        loader.setup()
        loader.load(ds)

        cfg2 = HDF5LoaderConfig(path=tmp_path / "telem.h5", mode="a")
        loader2 = HDF5Loader(cfg2)
        loader2.setup()
        loader2.load(_make_dataset(n=2))

        with h5py.File(tmp_path / "telem.h5", "r") as hf:
            times = hf["telemetry"]["temperature"]["time_tai"][:]
            assert len(times) == 5

    def test_unit_attribute(self, tmp_path: Path) -> None:
        import h5py

        ds = _make_dataset()
        cfg = HDF5LoaderConfig(path=tmp_path / "telem.h5")
        loader = HDF5Loader(cfg)
        loader.setup()
        loader.load(ds)

        with h5py.File(cfg.path, "r") as hf:
            assert hf["telemetry"]["temperature"].attrs.get("unit") == "degC"


# --------------------------------------------------------------------------- #
#  CSV Loader                                                                  #
# --------------------------------------------------------------------------- #


class TestCsvLoader:
    def test_writes_per_parameter(self, tmp_path: Path) -> None:
        ds = _make_dataset()
        cfg = CsvLoaderConfig(output_dir=tmp_path)
        loader = CsvLoader(cfg)
        loader.setup()
        loader.load(ds)
        out = tmp_path / "temperature.csv"
        assert out.exists()
        df = pd.read_csv(out)
        assert len(df) == 5
        assert "eng_value" in df.columns

    def test_wide_format(self, tmp_path: Path) -> None:
        ds = _make_dataset(param="p1")
        cfg = CsvLoaderConfig(output_dir=tmp_path, wide_format=True)
        loader = CsvLoader(cfg)
        loader.setup()
        loader.load(ds)
        out = tmp_path / "telemetry.csv"
        assert out.exists()
        df = pd.read_csv(out)
        assert "p1" in df.columns

    def test_append_mode(self, tmp_path: Path) -> None:
        ds1 = _make_dataset(n=3)
        ds2 = _make_dataset(n=2)
        cfg = CsvLoaderConfig(output_dir=tmp_path, overwrite=False)
        loader = CsvLoader(cfg)
        loader.setup()
        loader.load(ds1)
        loader.load(ds2)
        df = pd.read_csv(tmp_path / "temperature.csv")
        assert len(df) == 5
