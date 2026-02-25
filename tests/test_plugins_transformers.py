"""Tests for Decom, ApidFilter, and Calibration transformers."""

from __future__ import annotations

import struct
from pathlib import Path

import pytest

from mdp.models.dataset import TelemetryDataset
from mdp.models.parameter import EngineeringParameter, ParameterType
from mdp.plugins.transformers.decom import (
    DecomConfig,
    DecomTransformer,
    ParameterDefinition,
)
from mdp.plugins.transformers.filter import ApidFilterConfig, ApidFilterTransformer
from mdp.plugins.transformers.calibration import (
    CalibrationConfig,
    CalibrationEntry,
    CalibrationMethod,
    CalibrationTransformer,
    _interpolate,
)
from tests.conftest import make_raw_packet


# --------------------------------------------------------------------------- #
#  Helpers                                                                     #
# --------------------------------------------------------------------------- #


def _dataset_with_packets(*apids: int) -> TelemetryDataset:
    ds = TelemetryDataset()
    for apid in apids:
        raw = make_raw_packet(apid=apid, user_data=struct.pack(">HHf", 1000, 200, 3.14))
        from mdp.models.packet import TelemetryPacket
        p = TelemetryPacket.from_bytes(raw, source_time_tai=0.0)
        ds.add_packet(p)
    return ds


def _dataset_with_param(name: str, values: list[float]) -> TelemetryDataset:
    ds = TelemetryDataset()
    for i, v in enumerate(values):
        ds.add_parameter(EngineeringParameter(
            name=name, apid=0x100, seq_count=i,
            sample_time_tai=float(i), raw_value=v, eng_value=v,
            unit="raw",
        ))
    return ds


# --------------------------------------------------------------------------- #
#  DecomTransformer                                                            #
# --------------------------------------------------------------------------- #


class TestDecomTransformer:
    def test_extracts_uint16(self) -> None:
        pdef = ParameterDefinition(
            name="voltage_raw",
            apid=0x100,
            byte_offset=0,
            bit_length=16,
            param_type=ParameterType.UINT,
            unit="DN",
        )
        cfg = DecomConfig(parameters=[pdef])
        transformer = DecomTransformer(cfg)
        transformer.setup()

        raw = make_raw_packet(apid=0x100, user_data=struct.pack(">H", 1024) + b"\x00\x00")
        from mdp.models.packet import TelemetryPacket
        p = TelemetryPacket.from_bytes(raw, source_time_tai=1.0)
        ds = TelemetryDataset()
        ds.add_packet(p)

        result = transformer.transform(ds)
        record = result.get_parameter("voltage_raw")
        assert record is not None
        assert record.count == 1
        assert record.samples[0].raw_value == 1024

    def test_skips_unknown_apid(self) -> None:
        pdef = ParameterDefinition(
            name="x", apid=0x999, byte_offset=0, bit_length=8, param_type=ParameterType.UINT
        )
        cfg = DecomConfig(parameters=[pdef], skip_unknown_apids=True)
        transformer = DecomTransformer(cfg)
        transformer.setup()
        ds = _dataset_with_packets(0x100)
        result = transformer.transform(ds)
        assert result.get_parameter("x") is None

    def test_raises_on_unknown_apid_if_not_skipping(self) -> None:
        pdef = ParameterDefinition(
            name="x", apid=0x999, byte_offset=0, bit_length=8, param_type=ParameterType.UINT
        )
        cfg = DecomConfig(parameters=[pdef], skip_unknown_apids=False)
        transformer = DecomTransformer(cfg)
        transformer.setup()
        ds = _dataset_with_packets(0x100)
        with pytest.raises(KeyError):
            transformer.transform(ds)

    def test_float32_extraction(self) -> None:
        pdef = ParameterDefinition(
            name="temp",
            apid=0x200,
            byte_offset=0,
            bit_length=32,
            param_type=ParameterType.FLOAT,
            unit="K",
        )
        expected = 298.15
        raw = make_raw_packet(apid=0x200, user_data=struct.pack(">f", expected))
        from mdp.models.packet import TelemetryPacket
        p = TelemetryPacket.from_bytes(raw, source_time_tai=0.0)
        ds = TelemetryDataset()
        ds.add_packet(p)

        transformer = DecomTransformer(DecomConfig(parameters=[pdef]))
        transformer.setup()
        result = transformer.transform(ds)
        sample = result.get_parameter("temp").samples[0]  # type: ignore[union-attr]
        assert abs(float(sample.raw_value) - expected) < 0.01


# --------------------------------------------------------------------------- #
#  ApidFilterTransformer                                                       #
# --------------------------------------------------------------------------- #


class TestApidFilterTransformer:
    def test_include_filter(self) -> None:
        ds = _dataset_with_packets(0x100, 0x200, 0x300)
        transformer = ApidFilterTransformer(ApidFilterConfig(include=[0x100, 0x300]))
        result = transformer.transform(ds)
        apids = {p.apid for p in result.packets}
        assert apids == {0x100, 0x300}

    def test_exclude_filter(self) -> None:
        ds = _dataset_with_packets(0x100, 0x200, 0x300)
        transformer = ApidFilterTransformer(ApidFilterConfig(exclude=[0x200]))
        result = transformer.transform(ds)
        apids = {p.apid for p in result.packets}
        assert 0x200 not in apids
        assert len(result.packets) == 2

    def test_no_filter_passthrough(self) -> None:
        ds = _dataset_with_packets(0x100, 0x200)
        transformer = ApidFilterTransformer(ApidFilterConfig())
        result = transformer.transform(ds)
        assert len(result.packets) == 2

    def test_include_and_exclude_raises(self) -> None:
        with pytest.raises(ValueError):
            ApidFilterConfig(include=[0x100], exclude=[0x200])


# --------------------------------------------------------------------------- #
#  CalibrationTransformer                                                      #
# --------------------------------------------------------------------------- #


class TestCalibrationTransformer:
    def test_polynomial_calibration(self) -> None:
        ds = _dataset_with_param("temp_dn", [0.0, 100.0, 200.0])
        cal = CalibrationEntry(
            parameter_name="temp_dn",
            method=CalibrationMethod.POLYNOMIAL,
            coefficients=[-273.15, 0.5],
            unit="degC",
        )
        transformer = CalibrationTransformer(CalibrationConfig(calibrations=[cal]))
        transformer.setup()
        result = transformer.transform(ds)
        samples = result.get_parameter("temp_dn").samples  # type: ignore[union-attr]
        assert abs(float(samples[0].eng_value) - (-273.15)) < 1e-6
        assert abs(float(samples[1].eng_value) - (-223.15)) < 1e-6
        assert samples[0].unit == "degC"

    def test_table_calibration(self) -> None:
        cal = CalibrationEntry(
            parameter_name="raw",
            method=CalibrationMethod.TABLE,
            table_raw=[0.0, 100.0, 200.0],
            table_eng=[0.0, 10.0, 20.0],
            unit="V",
        )
        ds = _dataset_with_param("raw", [50.0, 150.0])
        transformer = CalibrationTransformer(CalibrationConfig(calibrations=[cal]))
        transformer.setup()
        result = transformer.transform(ds)
        samples = result.get_parameter("raw").samples  # type: ignore[union-attr]
        assert abs(float(samples[0].eng_value) - 5.0) < 1e-6
        assert abs(float(samples[1].eng_value) - 15.0) < 1e-6

    def test_identity_calibration(self) -> None:
        ds = _dataset_with_param("x", [42.0])
        cal = CalibrationEntry(
            parameter_name="x",
            method=CalibrationMethod.IDENTITY,
        )
        transformer = CalibrationTransformer(CalibrationConfig(calibrations=[cal]))
        transformer.setup()
        result = transformer.transform(ds)
        assert float(result.get_parameter("x").samples[0].eng_value) == 42.0  # type: ignore[union-attr]

    def test_uncalibrated_param_untouched(self) -> None:
        ds = _dataset_with_param("uncalibrated", [99.0])
        cfg = CalibrationConfig(calibrations=[])
        transformer = CalibrationTransformer(cfg)
        transformer.setup()
        result = transformer.transform(ds)
        assert float(result.get_parameter("uncalibrated").samples[0].eng_value) == 99.0  # type: ignore[union-attr]


class TestInterpolate:
    def test_midpoint(self) -> None:
        assert _interpolate(50.0, [0.0, 100.0], [0.0, 10.0]) == pytest.approx(5.0)

    def test_below_range(self) -> None:
        assert _interpolate(-10.0, [0.0, 100.0], [0.0, 10.0]) == 0.0

    def test_above_range(self) -> None:
        assert _interpolate(110.0, [0.0, 100.0], [0.0, 10.0]) == 10.0
