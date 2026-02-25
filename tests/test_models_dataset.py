"""Tests for TelemetryDataset."""

from __future__ import annotations

import pytest

from mdp.models.dataset import TelemetryDataset
from mdp.models.parameter import EngineeringParameter


def _make_param(name: str, tai: float, raw: float, eng: float) -> EngineeringParameter:
    return EngineeringParameter(
        name=name,
        apid=0x100,
        seq_count=0,
        sample_time_tai=tai,
        raw_value=raw,
        eng_value=eng,
        unit="K",
    )


class TestTelemetryDataset:
    def test_empty_dataset(self) -> None:
        ds = TelemetryDataset()
        assert len(ds) == 0
        assert ds.parameter_names() == []

    def test_add_and_retrieve_parameter(self) -> None:
        ds = TelemetryDataset()
        p = _make_param("temp", 100.0, 300.0, 26.85)
        ds.add_parameter(p)
        record = ds.get_parameter("temp")
        assert record is not None
        assert record.count == 1
        assert record.unit == "K"

    def test_multiple_samples_accumulate(self) -> None:
        ds = TelemetryDataset()
        for i in range(5):
            ds.add_parameter(_make_param("voltage", float(i), float(i * 10), float(i * 10)))
        record = ds.get_parameter("voltage")
        assert record is not None
        assert record.count == 5

    def test_time_range(self) -> None:
        ds = TelemetryDataset()
        for tai in [10.0, 30.0, 20.0]:
            ds.add_parameter(_make_param("p", tai, 0.0, 0.0))
        record = ds.get_parameter("p")
        assert record is not None
        t_min, t_max = record.time_range  # type: ignore[misc]
        assert t_min == 10.0
        assert t_max == 30.0

    def test_to_dataframe(self, sample_dataset: TelemetryDataset) -> None:
        df = sample_dataset.to_dataframe("temperature")
        assert not df.empty
        assert "time_tai" in df.columns
        assert "eng_value" in df.columns
        assert list(df["time_tai"]) == sorted(df["time_tai"])

    def test_to_dataframe_missing_param(self) -> None:
        ds = TelemetryDataset()
        with pytest.raises(KeyError, match="'nonexistent'"):
            ds.to_dataframe("nonexistent")

    def test_merge(self) -> None:
        ds1 = TelemetryDataset()
        ds1.add_parameter(_make_param("x", 1.0, 1.0, 1.0))

        ds2 = TelemetryDataset()
        ds2.add_parameter(_make_param("x", 2.0, 2.0, 2.0))
        ds2.add_parameter(_make_param("y", 2.0, 5.0, 5.0))

        merged = ds1.merge(ds2)
        assert merged.get_parameter("x") is not None
        assert merged.get_parameter("x").count == 2  # type: ignore[union-attr]
        assert merged.get_parameter("y") is not None
        assert merged.get_parameter("y").count == 1  # type: ignore[union-attr]

    def test_packets_by_apid(self, sample_dataset: TelemetryDataset) -> None:
        packets = sample_dataset.packets_by_apid(0x200)
        assert len(packets) == 1
        assert packets[0].apid == 0x200

    def test_repr(self) -> None:
        ds = TelemetryDataset(metadata={"mission": "test"})
        r = repr(ds)
        assert "TelemetryDataset" in r
        assert "mission" in r
