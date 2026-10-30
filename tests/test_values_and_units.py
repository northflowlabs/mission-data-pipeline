"""Values keep their type and precision, units stay true, losses reach the result."""

from __future__ import annotations

import json
from datetime import datetime
from pathlib import Path

import h5py
import numpy as np
import pandas as pd
import pyarrow.parquet as pq
import pytest

from mdp.core.base import StageStatus
from mdp.core.pipeline import Pipeline, PipelineConfig, PipelineResult
from mdp.models.dataset import TelemetryDataset
from mdp.models.parameter import EngineeringParameter, ParameterType
from mdp.models.time import utc_to_tai_j2000
from mdp.plugins.extractors.binary import BinaryExtractorConfig, BinaryPacketExtractor
from mdp.plugins.extractors.csv import CsvExtractorConfig, CsvTelemetryExtractor
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

    def test_floats_after_integers_widen_the_column(self, tmp_path: Path) -> None:
        ok, path = _load_hdf5(tmp_path, [_sample(1)], [_sample(1.5)], [_sample(2)])
        assert ok
        with h5py.File(path) as hf:
            assert hf["telemetry/x/eng_value"].dtype == "float64"
            assert hf["telemetry/x/eng_value"][:].tolist() == [1.0, 1.5, 2.0]
            assert len(hf["telemetry/x/time_tai"]) == 3

    def test_a_large_integer_does_not_widen_to_float(self, tmp_path: Path) -> None:
        ok, path = _load_hdf5(tmp_path, [_sample(2**53 + 1)], [_sample(1.5)])
        assert not ok
        with h5py.File(path) as hf:
            assert int(hf["telemetry/x/eng_value"][0]) == 2**53 + 1
            assert len(hf["telemetry/x/time_tai"]) == 1

    def test_an_integer_just_past_exact_float_stays_out_of_a_float_column(
        self, tmp_path: Path
    ) -> None:
        ok, _ = _load_hdf5(tmp_path, [_sample(1.5)], [_sample(2**53 + 1)])
        assert not ok

    def test_large_integers_widen_to_uint64(self, tmp_path: Path) -> None:
        ok, path = _load_hdf5(tmp_path, [_sample(1)], [_sample(2**63)])
        assert ok
        with h5py.File(path) as hf:
            assert hf["telemetry/x/eng_value"][:].tolist() == [1, 2**63]

    @pytest.mark.parametrize("batch_size", [1, 2, 3, 1000])
    def test_a_negative_and_a_very_large_integer_fail_at_any_batch_size(
        self, tmp_path: Path, batch_size: int
    ) -> None:
        samples = [_sample(1), _sample(-1), _sample(2**63)]
        error = _run_batches(tmp_path, samples, batch_size)
        assert "negative integers" in str(error)

    def test_a_small_integer_fits_a_float_column(self, tmp_path: Path) -> None:
        ok, path = _load_hdf5(tmp_path, [_sample(1.5)], [_sample(2)])
        assert ok
        with h5py.File(path) as hf:
            assert hf["telemetry/x/eng_value"][:].tolist() == [1.5, 2.0]


def _run_batches(
    tmp_path: Path, samples: list[EngineeringParameter], batch_size: int
) -> Exception | None:
    batches = [samples[i : i + batch_size] for i in range(0, len(samples), batch_size)]
    loader = HDF5Loader(HDF5LoaderConfig(path=tmp_path / f"{batch_size}.h5"))
    result = Pipeline(PipelineConfig(name="x"), _Batches(batches), [], loader).run()
    return result.errors[0] if result.errors else None


class TestHdf5AtAnyBatchSize:
    CSV = "time,apid,seq_count,temp\n" + "".join(
        f"{i}.0,1,{i},{cell}\n" for i, cell in enumerate(["20.5", "21", "nan", "22.5", "23"])
    )

    def _run_csv(self, tmp_path: Path, batch_size: int, **config: object) -> PipelineResult:
        source = tmp_path / "t.csv"
        source.write_text(self.CSV, encoding="utf-8")
        extractor = CsvTelemetryExtractor(
            CsvExtractorConfig(path=source, batch_size=batch_size, **config)
        )
        loader = HDF5Loader(HDF5LoaderConfig(path=tmp_path / f"{batch_size}.h5"))
        return Pipeline(PipelineConfig(name="x"), extractor, [], loader).run()

    @pytest.mark.parametrize("batch_size", [1, 2, 3, 4, 1000])
    def test_a_nan_cell_in_a_numeric_column_fails_the_same_way(
        self, tmp_path: Path, batch_size: int
    ) -> None:
        result = self._run_csv(tmp_path, batch_size)
        assert result.status == StageStatus.FAILED
        assert "'temp' mixes numbers, text or booleans" in str(result.errors[0])

    @pytest.mark.parametrize("batch_size", [1, 2, 3, 1000])
    def test_a_declared_float_column_keeps_its_nan(self, tmp_path: Path, batch_size: int) -> None:
        result = self._run_csv(tmp_path, batch_size, parameter_types={"temp": ParameterType.FLOAT})
        assert result.ok
        with h5py.File(tmp_path / f"{batch_size}.h5") as hf:
            values = hf["telemetry/temp/eng_value"][:]
        assert values.dtype == "float64"
        assert values[np.isfinite(values)].tolist() == [20.5, 21.0, 22.5, 23.0]

    @pytest.mark.parametrize("batch_size", [1, 2, 3, 1000])
    def test_integers_and_floats_give_one_column(self, tmp_path: Path, batch_size: int) -> None:
        samples = [_sample(1), _sample(2), _sample(2.5), _sample(3)]
        assert _run_batches(tmp_path, samples, batch_size) is None
        with h5py.File(tmp_path / f"{batch_size}.h5") as hf:
            assert hf["telemetry/x/eng_value"].dtype == "float64"
            assert hf["telemetry/x/eng_value"][:].tolist() == [1.0, 2.0, 2.5, 3.0]

    @pytest.mark.parametrize("batch_size", [1, 2, 3])
    @pytest.mark.parametrize(
        "values", [[1.0, "x", 2.0], ["x", 1.0, "y"], [True, 1, False], [1, True, 2], [True, "x", 1]]
    )
    def test_mixed_kinds_fail_alike(
        self, tmp_path: Path, batch_size: int, values: list[object]
    ) -> None:
        error = _run_batches(tmp_path, [_sample(v) for v in values], batch_size)
        assert "mixes numbers, text or booleans" in str(error)


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
        assert result.total_samples == 1
        assert result.data_issues == {"short_packets": 1}
        assert "short_packets : 1" in result.summary()

    def test_frame_sync_losses_are_in_the_result(self, tmp_path: Path) -> None:
        sync = b"\x1a\xcf\xfc\x1d"
        path = _binary_file(tmp_path, sync + b"\xfe\xfd" * 3, sync + make_raw_packet())
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
