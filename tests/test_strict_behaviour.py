"""Frames, exact values, refused configs, run status and stage lifecycle."""

from __future__ import annotations

import math
import struct
from datetime import datetime, timedelta
from pathlib import Path

import h5py
import pandas as pd
import pyarrow.parquet as pq
import pytest
from click.testing import CliRunner
from pydantic import BaseModel

from mdp.cli.main import EXIT_DATA_LOST, cli
from mdp.core.base import Extractor, Loader, StageStatus, Transformer
from mdp.core.pipeline import Pipeline, PipelineConfig
from mdp.models.dataset import TelemetryDataset
from mdp.models.parameter import EngineeringParameter, ParameterRecord, ParameterType
from mdp.models.time import utc_to_tai_j2000
from mdp.observability.hooks import HookManager
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
from tests.conftest import make_raw_packet
from tests.test_integrity import _Batches
from tests.test_values_and_units import _sample

SYNC = b"\x1a\xcf\xfc\x1d"
TELEMETRY = (
    Path(__file__).parent.parent
    / "examples"
    / "data"
    / "CYGNSS_F7_L0_2022_086_10_15_V01_F__first101pkts.tlm"
)


def _write(tmp_path: Path, data: bytes) -> Path:
    path = tmp_path / "t.bin"
    path.write_bytes(data)
    return path


def _packets(path: Path, **config: object) -> list:
    extractor = BinaryPacketExtractor(BinaryExtractorConfig(path=path, **config))
    return [p for ds in extractor.extract() for p in ds.packets]


class TestFrames:
    def test_a_marker_right_after_a_stray_1a_is_found(self, tmp_path: Path) -> None:
        path = _write(tmp_path, b"\x1a" + SYNC + make_raw_packet(seq_count=7))
        assert [p.seq_count for p in _packets(path, frame_sync=True)] == [7]

    def test_every_packet_of_a_frame_is_read(self, tmp_path: Path) -> None:
        frame = make_raw_packet(seq_count=1) + make_raw_packet(seq_count=2)
        path = _write(tmp_path, SYNC + frame + SYNC + make_raw_packet(seq_count=3))
        assert [p.seq_count for p in _packets(path, frame_sync=True)] == [1, 2, 3]

    def test_a_marker_inside_packet_data_is_not_a_frame(self, tmp_path: Path) -> None:
        packet = make_raw_packet(seq_count=1, user_data=SYNC + b"\x00" * 4)
        path = _write(tmp_path, SYNC + packet)
        assert len(_packets(path, frame_sync=True)) == 1

    def test_ground_receipt_time_is_not_invented(self, tmp_path: Path) -> None:
        path = _write(tmp_path, make_raw_packet())
        assert _packets(path)[0].ground_receipt_time is None

    def test_a_configured_ground_receipt_time_is_kept(self, tmp_path: Path) -> None:
        path = _write(tmp_path, make_raw_packet())
        assert _packets(path, ground_receipt_time=5.0)[0].ground_receipt_time == 5.0


class TestCsvValues:
    def _read(self, tmp_path: Path, text: str, **config: object) -> TelemetryDataset:
        path = tmp_path / "t.csv"
        path.write_text(text, encoding="utf-8")
        extractor = CsvTelemetryExtractor(CsvExtractorConfig(path=path, **config))
        return next(iter(extractor.extract()))

    def test_a_long_integer_keeps_every_digit(self, tmp_path: Path) -> None:
        ds = self._read(tmp_path, "time,apid,seq_count,x\n1.0,1,0,9007199254740993\n")
        assert ds.parameters["x"].samples[0].eng_value == 9007199254740993

    def test_a_code_with_leading_zeros_stays_text(self, tmp_path: Path) -> None:
        ds = self._read(tmp_path, "time,apid,seq_count,x\n1.0,1,0,007\n")
        assert ds.parameters["x"].samples[0].eng_value == "007"

    def test_declared_types_are_followed(self, tmp_path: Path) -> None:
        ds = self._read(
            tmp_path,
            "time,apid,seq_count,a,b,c\n1.0,1,0,007,12,true\n",
            parameter_types={
                "a": ParameterType.INT,
                "b": ParameterType.STRING,
                "c": ParameterType.BOOLEAN,
            },
        )
        values = {n: r.samples[0].eng_value for n, r in ds.parameters.items()}
        assert values == {"a": 7, "b": "12", "c": True}

    def test_a_short_row_is_counted(self, tmp_path: Path) -> None:
        ds = self._read(tmp_path, "time,apid,seq_count,x\n1.0,1,0,5\n,,,\n2.0,1\n")
        assert ds.metadata["skipped_rows"] == 1
        assert ds.metadata["wrong_width_rows"] == 1
        assert ds.parameters["x"].count == 1

    def test_an_empty_cell_is_counted(self, tmp_path: Path) -> None:
        ds = self._read(tmp_path, "time,apid,seq_count,x,y\n1.0,1,0,,5\n")
        assert ds.metadata["empty_cells"] == 1

    def test_csv_losses_reach_the_result(self, tmp_path: Path) -> None:
        path = tmp_path / "t.csv"
        path.write_text("time,apid,seq_count,x\n1.0,1,0,5\n,,,\n", encoding="utf-8")
        extractor = CsvTelemetryExtractor(CsvExtractorConfig(path=path))
        result = Pipeline(PipelineConfig(name="x"), extractor).run()
        assert result.data_issues == {"skipped_rows": 1}
        assert result.status == StageStatus.DEGRADED


class TestRefusedConfigs:
    def test_an_empty_apid_filter(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError):
            BinaryExtractorConfig(path=tmp_path, apid_filter=[])

    def test_empty_parameter_columns(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError):
            CsvExtractorConfig(path=tmp_path, parameter_columns=[])

    def test_max_batches_zero(self) -> None:
        with pytest.raises(ValueError, match="max_batches"):
            PipelineConfig(name="x", max_batches=0)

    def test_cli_max_batches_zero(self) -> None:
        result = CliRunner().invoke(cli, ["run", "--extractor", "binary", "--max-batches", "0"])
        assert result.exit_code == 2

    @pytest.mark.parametrize(
        "fields",
        [
            {"method": CalibrationMethod.POLYNOMIAL, "unit": "K"},
            {"method": CalibrationMethod.TABLE, "unit": "K"},
            {"method": CalibrationMethod.IDENTITY, "unit": "K"},
            {"method": CalibrationMethod.TABLE, "table_raw": [0.0, 1.0], "table_eng": [1.0]},
            {"method": CalibrationMethod.TABLE, "table_raw": [2.0, 1.0], "table_eng": [0.0, 1.0]},
            {"method": CalibrationMethod.TABLE, "table_raw": [1.0, 1.0], "table_eng": [0.0, 1.0]},
        ],
    )
    def test_calibrations_that_would_lie(self, fields) -> None:
        with pytest.raises(ValueError):
            CalibrationEntry(parameter_name="x", **fields)


class TestTableLimits:
    def _run(self, raw: float) -> EngineeringParameter:
        entry = CalibrationEntry(
            parameter_name="x",
            method=CalibrationMethod.TABLE,
            table_raw=[0.0, 3.0],
            table_eng=[0.0, 30.0],
        )
        dataset = TelemetryDataset()
        dataset.add_parameter(_sample(raw))
        transformer = CalibrationTransformer(CalibrationConfig(calibrations=[entry]))
        transformer.setup()
        transformer.transform(dataset)
        return dataset.parameters["x"].samples[0]

    def test_inside_the_table(self) -> None:
        sample = self._run(1.5)
        assert (sample.eng_value, sample.out_of_limit) == (15.0, False)

    def test_beyond_the_table_is_held_and_flagged(self) -> None:
        sample = self._run(99.0)
        assert (sample.eng_value, sample.out_of_limit) == (30.0, True)


class TestUnitsInOutputs:
    def test_hdf5_refuses_a_new_unit(self, tmp_path: Path) -> None:
        path = tmp_path / "t.h5"
        batches = [[_sample(1.0, unit="V")], [_sample(1000.0, unit="mV")]]
        loader = HDF5Loader(HDF5LoaderConfig(path=path))
        result = Pipeline(PipelineConfig(name="x"), _Batches(batches), [], loader).run()
        assert not result.ok
        with h5py.File(path) as hf:
            assert hf["telemetry/x"].attrs["unit"] == "V"
            assert len(hf["telemetry/x/eng_value"]) == 1

    def test_wide_parquet_refuses_a_new_unit(self, tmp_path: Path) -> None:
        batches = [[_sample(1.0, time=1.0, unit="V")], [_sample(1000.0, time=2.0, unit="mV")]]
        loader = ParquetLoader(ParquetLoaderConfig(output_dir=tmp_path, wide_format=True))
        assert not Pipeline(PipelineConfig(name="x"), _Batches(batches), [], loader).run().ok

    def test_calibration_refuses_to_leave_one_record_in_two_units(self) -> None:
        record = ParameterRecord(
            name="x", unit="s", samples=[_sample(3600.0, unit="s"), _sample("bad", unit="s")]
        )
        entry = CalibrationEntry(
            parameter_name="x",
            method=CalibrationMethod.POLYNOMIAL,
            coefficients=[0.0, 1 / 3600],
            unit="h",
        )
        transformer = CalibrationTransformer(CalibrationConfig(calibrations=[entry]))
        transformer.setup()
        with pytest.raises(ValueError, match="mix units"):
            transformer.transform(TelemetryDataset(parameters={"x": record}))


class TestExactWideOutput:
    BIG = 2**53 + 1

    def test_wide_frame_keeps_a_large_integer(self) -> None:
        dataset = TelemetryDataset()
        dataset.add_parameter(_sample(self.BIG, time=1.0))
        assert int(dataset.to_wide_dataframe()["x"].iloc[0]) == self.BIG

    def test_wide_frame_keeps_integers_beside_a_gap(self) -> None:
        dataset = TelemetryDataset()
        dataset.add_parameter(_sample(self.BIG, time=1.0))
        dataset.add_parameter(_sample(2.5, name="y", time=2.0))
        assert int(dataset.to_wide_dataframe()["x"].dropna().iloc[0]) == self.BIG

    def test_wide_parquet_keeps_a_large_integer(self, tmp_path: Path) -> None:
        loader = ParquetLoader(ParquetLoaderConfig(output_dir=tmp_path, wide_format=True))
        batches = [[_sample(self.BIG, time=1.0)]]
        assert Pipeline(PipelineConfig(name="x"), _Batches(batches), [], loader).run().ok
        assert pq.read_table(tmp_path / "telemetry.parquet")["x"].to_pylist() == [self.BIG]

    def test_a_large_integer_is_refused_beside_floats(self, tmp_path: Path) -> None:
        loader = HDF5Loader(HDF5LoaderConfig(path=tmp_path / "t.h5"))
        batches = [[_sample(self.BIG), _sample(1.5, time=1.0)]]
        assert not Pipeline(PipelineConfig(name="x"), _Batches(batches), [], loader).run().ok

    def test_wide_csv_keeps_text_and_digits_across_batches(self, tmp_path: Path) -> None:
        batches = [
            [_sample("007", time=1.0), _sample(self.BIG, name="n", time=1.0)],
            [_sample("008", time=2.0), _sample(self.BIG, name="n", time=2.0)],
        ]
        loader = CsvLoader(CsvLoaderConfig(output_dir=tmp_path, wide_format=True))
        assert Pipeline(PipelineConfig(name="x"), _Batches(batches), [], loader).run().ok
        df = pd.read_csv(tmp_path / "telemetry.csv", dtype=str)
        assert df["x"].tolist() == ["007", "008"]
        assert df["n"].tolist() == [str(self.BIG)] * 2

    @pytest.mark.parametrize(
        "loader_class,config",
        [(ParquetLoader, ParquetLoaderConfig), (CsvLoader, CsvLoaderConfig)],
    )
    def test_a_time_repeated_across_batches_is_refused(
        self, tmp_path: Path, loader_class, config
    ) -> None:
        batches = [[_sample(1.0, time=1.0)], [_sample(2.0, time=1.0)]]
        loader = loader_class(config(output_dir=tmp_path, wide_format=True))
        result = Pipeline(PipelineConfig(name="x"), _Batches(batches), [], loader).run()
        assert not result.ok
        assert "repeats time" in str(result.errors[0])


class TestRunStatus:
    def _lossy(self, tmp_path: Path, strict: bool = False):
        path = _write(tmp_path, SYNC + b"\xff" * 6 + SYNC + make_raw_packet())
        extractor = BinaryPacketExtractor(BinaryExtractorConfig(path=path, frame_sync=True))
        return Pipeline(PipelineConfig(name="x", strict=strict), extractor).run()

    def test_losses_are_a_distinct_status(self, tmp_path: Path) -> None:
        result = self._lossy(tmp_path)
        assert result.status == StageStatus.DEGRADED
        assert not result.ok

    def test_strict_makes_them_a_failure(self, tmp_path: Path) -> None:
        assert self._lossy(tmp_path, strict=True).status == StageStatus.FAILED

    def _run_cli(self, tmp_path: Path, *extra: str):
        path = _write(tmp_path, SYNC + b"\xff" * 6 + SYNC + make_raw_packet())
        config = tmp_path / "e.json"
        config.write_text(f'{{"path": "{path.as_posix()}", "frame_sync": true}}', encoding="utf-8")
        args = ["run", "--extractor", "binary", "--extractor-config", str(config), *extra]
        return CliRunner().invoke(cli, args)

    def test_the_cli_exit_code_shows_losses(self, tmp_path: Path) -> None:
        assert self._run_cli(tmp_path).exit_code == EXIT_DATA_LOST

    def test_the_cli_strict_flag_fails_the_run(self, tmp_path: Path) -> None:
        assert self._run_cli(tmp_path, "--strict").exit_code == 1


class _Config(BaseModel):
    pass


class _Source(Extractor[_Config]):
    config_class = _Config

    def __init__(self, log: list[str], batches: int = 3) -> None:
        super().__init__(_Config())
        self.log = log
        self.batches = batches

    def validate_config(self) -> None:
        self.log.append("source.validate")

    def setup(self) -> None:
        self.log.append("source.setup")

    def teardown(self) -> None:
        self.log.append("source.teardown")

    def extract(self):  # type: ignore[no-untyped-def]
        for _ in range(self.batches):
            yield TelemetryDataset()


class _Step(Transformer[_Config]):
    config_class = _Config

    def __init__(self, log: list[str]) -> None:
        super().__init__(_Config())
        self.log = log

    def setup(self) -> None:
        self.log.append("step.setup")

    def teardown(self) -> None:
        self.log.append("step.teardown")

    def transform(self, dataset: TelemetryDataset) -> TelemetryDataset:
        self.log.append("step.transform")
        return dataset


class _Sink(Loader[_Config]):
    config_class = _Config

    def __init__(self, log: list[str]) -> None:
        super().__init__(_Config())
        self.log = log

    def setup(self) -> None:
        self.log.append("sink.setup")

    def teardown(self) -> None:
        self.log.append("sink.teardown")

    def load(self, dataset: TelemetryDataset) -> None:
        self.log.append("sink.load")


class TestLifecycle:
    def test_each_stage_is_set_up_and_torn_down_once(self) -> None:
        log: list[str] = []
        result = Pipeline(PipelineConfig(name="x"), _Source(log), [_Step(log)], _Sink(log)).run()
        assert result.ok
        for event in ("source.validate", "source.setup", "step.setup", "sink.setup"):
            assert log.count(event) == 1
        for event in ("source.teardown", "step.teardown", "sink.teardown"):
            assert log.count(event) == 1
        assert log.count("step.transform") == 3
        assert log.index("source.setup") < log.index("step.transform")
        assert log[-1].endswith("teardown")

    def test_a_second_run_sets_up_again(self) -> None:
        log: list[str] = []
        pipeline = Pipeline(PipelineConfig(name="x"), _Source(log), [_Step(log)])
        pipeline.run()
        pipeline.run()
        assert log.count("step.setup") == 2

    def test_teardown_runs_after_a_failed_setup(self) -> None:
        class Bad(_Step):
            def setup(self) -> None:
                raise OSError("no connection")

        log: list[str] = []
        result = Pipeline(PipelineConfig(name="x"), _Source(log), [Bad(log)]).run()
        assert not result.ok
        assert "source.teardown" in log

    def test_validate_config_can_refuse_a_run(self) -> None:
        class Refuses(_Source):
            def validate_config(self) -> None:
                raise ValueError("bad config")

        result = Pipeline(PipelineConfig(name="x"), Refuses([])).run()
        assert not result.ok
        assert "bad config" in str(result.errors[0])


def test_batch_transformed_does_not_fire_after_a_failed_transformer() -> None:
    class Fails(_Step):
        def transform(self, dataset: TelemetryDataset) -> TelemetryDataset:
            raise ValueError("boom")

    hooks = HookManager()
    seen: list[object] = []
    hooks.register("batch.transformed", seen.append)
    config = PipelineConfig(name="x", stop_on_error=False)
    Pipeline(config, _Source([], batches=1), [Fails([])], hooks=hooks).run()
    assert seen == []


class TestTimeRange:
    def test_missing_times_are_ignored_in_either_order(self) -> None:
        for times in ([math.nan, 5.0], [5.0, math.nan]):
            samples = [_sample(1.0, time=t) for t in times]
            assert ParameterRecord(name="x", samples=samples).time_range == (5.0, 5.0)

    def test_no_real_time_gives_no_range(self) -> None:
        record = ParameterRecord(name="x", samples=[_sample(1.0, time=math.nan)])
        assert record.time_range is None


class TestEpochAgainstAstropy:
    def test_the_first_apid_393_packet(self) -> None:
        # 2022 day 84, 21:43:34.031043 UTC; astropy gives 701516683.2150431
        utc = datetime(2022, 1, 1) + timedelta(
            days=83, hours=21, minutes=43, seconds=34, microseconds=31043
        )
        assert utc_to_tai_j2000(utc, 37) == pytest.approx(701516683.2150431, abs=1e-6)


def test_the_two_clocks_agree_to_the_stated_figures() -> None:
    from tests.test_real_telemetry import _header_utc

    gaps = []
    extractor = BinaryPacketExtractor(
        BinaryExtractorConfig(path=TELEMETRY, apid_filter=[1313], sec_hdr_length=10)
    )
    for packet in (p for ds in extractor.extract() for p in ds.packets):
        week = int.from_bytes(packet.user_data[4:6], "little")
        seconds = struct.unpack("<d", packet.user_data[6:14])[0]
        gps_utc = datetime(1980, 1, 6) + timedelta(weeks=week, seconds=seconds - 18)
        gaps.append((_header_utc(packet.secondary_header) - gps_utc).total_seconds())
    assert min(gaps) >= 0.37 and max(gaps) <= 0.45
