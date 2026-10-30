"""Values keep their type and precision, units stay true, losses reach the result."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import h5py
import pandas as pd
import pyarrow.parquet as pq
import pytest

from mdp.core.pipeline import Pipeline, PipelineConfig
from mdp.models.dataset import TelemetryDataset
from mdp.models.parameter import EngineeringParameter, ParameterRecord, ParameterType
from mdp.models.time import utc_to_tai_j2000
from mdp.plugins.extractors.binary import BinaryExtractorConfig, BinaryPacketExtractor
from mdp.plugins.loaders.csv import CsvLoader, CsvLoaderConfig
from mdp.plugins.loaders.hdf5 import HDF5Loader, HDF5LoaderConfig
from mdp.plugins.loaders.parquet import ParquetLoader, ParquetLoaderConfig
from mdp.plugins.transformers.calibration import (
    CalibrationConfig,
    CalibrationEntry,
    CalibrationMethod,
    CalibrationTransformer,
)
from mdp.plugins.transformers.decom import DecomConfig, DecomTransformer, ParameterDefinition
from tests.conftest import make_raw_packet
from tests.test_integrity import _Batches, _param


def _sample(value: object, name: str = "x", time: float = 0.0, unit: str | None = None):
    return EngineeringParameter(
        name=name,
        apid=1,
        seq_count=0,
        sample_time_tai=time,
        raw_value=value,  # type: ignore[arg-type]
        eng_value=value,  # type: ignore[arg-type]
        unit=unit,
    )


def _load_hdf5(tmp_path: Path, *batches: list[EngineeringParameter]) -> tuple[bool, Path]:
    path = tmp_path / "t.h5"
    loader = HDF5Loader(HDF5LoaderConfig(path=path))
    result = Pipeline(PipelineConfig(name="x"), _Batches(list(batches)), [], loader).run()
    return result.ok, path


class TestHdf5Types:
    def test_text_that_looks_like_a_number_stays_text(self, tmp_path: Path) -> None:
        ok, path = _load_hdf5(tmp_path, [_sample("0123"), _sample("1e5")])
        assert ok
        with h5py.File(path) as hf:
            values = [v.decode() for v in hf["telemetry/x/eng_value_str"][:]]
        assert values == ["0123", "1e5"]

    def test_hex_of_a_binary_field_stays_text(self, tmp_path: Path) -> None:
        ok, path = _load_hdf5(tmp_path, [_sample("1234")])
        assert ok
        with h5py.File(path) as hf:
            assert "eng_value" not in hf["telemetry/x"]

    def test_uint64_keeps_every_digit(self, tmp_path: Path) -> None:
        big = 2**64 - 1
        ok, path = _load_hdf5(tmp_path, [_sample(big)])
        assert ok
        with h5py.File(path) as hf:
            assert int(hf["telemetry/x/eng_value"][0]) == big

    def test_int64_above_float_precision_keeps_every_digit(self, tmp_path: Path) -> None:
        ok, path = _load_hdf5(tmp_path, [_sample(2**53 + 1)])
        assert ok
        with h5py.File(path) as hf:
            assert int(hf["telemetry/x/eng_value"][0]) == 2**53 + 1

    def test_booleans_stay_booleans(self, tmp_path: Path) -> None:
        ok, path = _load_hdf5(tmp_path, [_sample(True), _sample(False)])
        assert ok
        with h5py.File(path) as hf:
            assert hf["telemetry/x/eng_value"][:].tolist() == [True, False]

    def test_a_float_does_not_silently_truncate_in_an_integer_column(self, tmp_path: Path) -> None:
        ok, path = _load_hdf5(tmp_path, [_sample(1)], [_sample(1.5)])
        assert not ok
        with h5py.File(path) as hf:
            assert len(hf["telemetry/x/time_tai"]) == len(hf["telemetry/x/eng_value"]) == 1

    def test_a_small_integer_fits_a_float_column(self, tmp_path: Path) -> None:
        ok, path = _load_hdf5(tmp_path, [_sample(1.5)], [_sample(2)])
        assert ok
        with h5py.File(path) as hf:
            assert hf["telemetry/x/eng_value"][:].tolist() == [1.5, 2.0]


class TestDecomValidation:
    @pytest.mark.parametrize(
        "param_type,bit_length",
        [
            (ParameterType.FLOAT, 16),
            (ParameterType.FLOAT, 64),
            (ParameterType.DOUBLE, 32),
            (ParameterType.DOUBLE, 80),
        ],
    )
    def test_floats_of_the_wrong_length_are_refused(self, param_type, bit_length) -> None:
        with pytest.raises(ValueError, match="bits"):
            ParameterDefinition(
                name="f", apid=1, byte_offset=0, bit_length=bit_length, param_type=param_type
            )

    def test_enumerated_fields_can_be_bit_fields(self) -> None:
        field = ParameterDefinition(
            name="mode",
            apid=1,
            byte_offset=0,
            bit_offset=4,
            bit_length=2,
            param_type=ParameterType.ENUMERATED,
        )
        assert field.bit_offset == 4


def _binary_file(tmp_path: Path, *packets: bytes) -> Path:
    path = tmp_path / "t.bin"
    path.write_bytes(b"".join(packets))
    return path


class TestLossesReachTheResult:
    def test_short_packets_are_counted_in_the_result_and_summary(self, tmp_path: Path) -> None:
        path = _binary_file(
            tmp_path,
            make_raw_packet(apid=1, user_data=b"\x00" * 8, seq_count=0),
            make_raw_packet(apid=1, user_data=b"\x00" * 2, seq_count=1),
        )
        field = ParameterDefinition(
            name="f", apid=1, byte_offset=4, bit_length=16, param_type=ParameterType.UINT
        )
        result = Pipeline(
            PipelineConfig(name="x"),
            BinaryPacketExtractor(BinaryExtractorConfig(path=path)),
            [DecomTransformer(DecomConfig(parameters=[field]))],
        ).run()
        assert result.ok
        assert result.total_samples == 1
        assert result.data_issues == {"short_packets": 1}
        assert "short_packets : 1" in result.summary()

    def test_frame_sync_losses_are_in_the_result(self, tmp_path: Path) -> None:
        sync = b"\x1a\xcf\xfc\x1d"
        path = _binary_file(tmp_path, sync + b"\xff" * 6, sync + make_raw_packet())
        result = Pipeline(
            PipelineConfig(name="x"),
            BinaryPacketExtractor(BinaryExtractorConfig(path=path, frame_sync=True)),
        ).run()
        assert result.data_issues == {"skipped_frames": 1}
        assert "skipped_frames : 1" in result.summary()

    def test_a_clean_run_has_no_issues(self, tmp_path: Path) -> None:
        path = _binary_file(tmp_path, make_raw_packet())
        result = Pipeline(
            PipelineConfig(name="x"), BinaryPacketExtractor(BinaryExtractorConfig(path=path))
        ).run()
        assert result.data_issues == {}


class TestUnits:
    def test_a_dataset_refuses_mixed_units(self) -> None:
        dataset = TelemetryDataset()
        dataset.add_parameter(_sample(1.0, unit="degC"))
        with pytest.raises(ValueError, match="mixes units"):
            dataset.add_parameter(_sample(2.0, unit="K"))

    def test_merge_refuses_mixed_units(self) -> None:
        a, b = TelemetryDataset(), TelemetryDataset()
        a.add_parameter(_sample(1.0, unit="degC"))
        b.add_parameter(_sample(2.0, unit="K"))
        with pytest.raises(ValueError, match="mixes units"):
            a.merge(b)

    def test_merge_keeps_a_known_unit(self) -> None:
        a, b = TelemetryDataset(), TelemetryDataset()
        a.add_parameter(_sample(1.0))
        b.add_parameter(_sample(2.0, unit="K"))
        assert a.merge(b).parameters["x"].unit == "K"

    def test_wide_parquet_carries_units_across_batches(self, tmp_path: Path) -> None:
        batches = [
            [_sample(1.0, time=1.0, unit="K")],
            [_sample(2.0, time=2.0, unit="K")],
        ]
        loader = ParquetLoader(ParquetLoaderConfig(output_dir=tmp_path, wide_format=True))
        assert Pipeline(PipelineConfig(name="x"), _Batches(batches), [], loader).run().ok
        table = pq.read_table(tmp_path / "telemetry.parquet")
        assert json.loads(table.schema.metadata[b"mdp_units"]) == {"x": "K"}
        assert table.num_rows == 2

    def test_wide_csv_carries_units(self, tmp_path: Path) -> None:
        batches = [[_sample(1.0, time=1.0, unit="K")], [_sample(2.0, time=2.0, unit="K")]]
        loader = CsvLoader(CsvLoaderConfig(output_dir=tmp_path, wide_format=True))
        assert Pipeline(PipelineConfig(name="x"), _Batches(batches), [], loader).run().ok
        df = pd.read_csv(tmp_path / "telemetry.csv")
        assert "x [K]" in df.columns
        assert len(df) == 2


def _calibrate(dataset: TelemetryDataset, entries, mark: bool) -> TelemetryDataset:
    transformer = CalibrationTransformer(
        CalibrationConfig(calibrations=entries, mark_uncalibrated_invalid=mark)
    )
    transformer.setup()
    return transformer.transform(dataset)


class TestCalibration:
    ENTRY = CalibrationEntry(
        parameter_name="x",
        method=CalibrationMethod.POLYNOMIAL,
        coefficients=[273.15, 1.0],
        unit="K",
    )

    def test_parameters_without_a_calibration_are_marked_invalid(self) -> None:
        dataset = TelemetryDataset()
        dataset.add_parameter(_sample(1.0, name="other"))
        _calibrate(dataset, [self.ENTRY], mark=True)
        assert dataset.parameters["other"].samples[0].validity is False

    def test_they_stay_valid_by_default(self) -> None:
        dataset = TelemetryDataset()
        dataset.add_parameter(_sample(1.0, name="other"))
        _calibrate(dataset, [self.ENTRY], mark=False)
        assert dataset.parameters["other"].samples[0].validity is True

    def test_a_sample_that_cannot_be_calibrated_is_marked(self) -> None:
        dataset = TelemetryDataset()
        dataset.add_parameter(_sample("NOMINAL"))
        _calibrate(dataset, [self.ENTRY], mark=True)
        assert dataset.parameters["x"].samples[0].validity is False

    def test_a_record_keeps_its_unit_if_any_sample_was_not_converted(self) -> None:
        record = ParameterRecord(
            name="x",
            unit="degC",
            samples=[_sample(1.0, unit="degC"), _sample("bad", unit="degC")],
        )
        dataset = TelemetryDataset(parameters={"x": record})
        _calibrate(dataset, [self.ENTRY], mark=False)
        assert dataset.parameters["x"].unit == "degC"
        assert [s.unit for s in dataset.parameters["x"].samples] == ["K", "degC"]

    def test_a_record_takes_the_new_unit_when_all_were_converted(self) -> None:
        dataset = TelemetryDataset()
        dataset.add_parameter(_sample(1.0, unit="degC"))
        _calibrate(dataset, [self.ENTRY], mark=False)
        assert dataset.parameters["x"].unit == "K"
        assert dataset.parameters["x"].samples[0].eng_value == pytest.approx(274.15)


class TestParquet:
    def test_an_integer_column_widens_for_floats_in_a_later_batch(self, tmp_path: Path) -> None:
        batches = [[_param(value=1)], [_param(value=2.5, time=1.0)]]
        loader = ParquetLoader(ParquetLoaderConfig(output_dir=tmp_path))
        assert Pipeline(PipelineConfig(name="x"), _Batches(batches), [], loader).run().ok
        values = pd.read_parquet(tmp_path / "x.parquet")["eng_value"].tolist()
        assert values == [1.0, 2.5]


class TestEpoch:
    def test_j2000_is_zero_at_noon_tt(self) -> None:
        # 2000-01-01T12:00:00 TT is 11:59:27.816 TAI, and TAI-UTC was 32 s
        utc = datetime(2000, 1, 1, 11, 58, 55, 816000)
        assert utc_to_tai_j2000(utc, 32) == pytest.approx(0.0, abs=1e-6)

    def test_a_known_later_time(self) -> None:
        # 2022 is 8084.5 days after the epoch's calendar noon
        utc = datetime(2022, 1, 1, 0, 0, 0)
        days = (utc - datetime(2000, 1, 1, 12)).total_seconds()
        assert utc_to_tai_j2000(utc, 37) == pytest.approx(days + 69.184)


def test_config_files_are_read_as_utf8(tmp_path: Path) -> None:
    from mdp.cli.main import _load_json

    path = tmp_path / "c.json"
    path.write_bytes('{"source_id": "Tromsø"}'.encode())
    assert _load_json(path) == {"source_id": "Tromsø"}
