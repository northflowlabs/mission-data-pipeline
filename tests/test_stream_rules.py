"""The frame_sync rules, CSV row widths, no unit as a distinct unit, CLI edges."""

from __future__ import annotations

from pathlib import Path

import h5py
import pytest
from click.testing import CliRunner

from mdp.cli.main import cli
from mdp.core.base import StageStatus
from mdp.core.pipeline import Pipeline, PipelineConfig
from mdp.models.dataset import TelemetryDataset
from mdp.plugins.extractors.binary import BinaryExtractorConfig, BinaryPacketExtractor
from mdp.plugins.extractors.csv import CsvExtractorConfig, CsvTelemetryExtractor
from mdp.plugins.loaders.hdf5 import HDF5Loader, HDF5LoaderConfig
from tests.conftest import make_raw_packet
from tests.test_integrity import _Batches
from tests.test_values_and_units import _sample

SYNC = b"\x1a\xcf\xfc\x1d"


def _run(tmp_path: Path, data: bytes, **config: object):
    path = tmp_path / "t.bin"
    path.write_bytes(data)
    extractor = BinaryPacketExtractor(BinaryExtractorConfig(path=path, frame_sync=True, **config))
    return Pipeline(PipelineConfig(name="x"), extractor).run()


def _seqs(tmp_path: Path, data: bytes) -> list[int]:
    path = tmp_path / "t.bin"
    path.write_bytes(data)
    extractor = BinaryPacketExtractor(BinaryExtractorConfig(path=path, frame_sync=True))
    return [p.seq_count for ds in extractor.extract() for p in ds.packets]


class TestFrameSync:
    def test_lead_in_bytes_are_counted(self, tmp_path: Path) -> None:
        result = _run(tmp_path, b"\x55" * 9 + SYNC + make_raw_packet())
        assert result.data_issues == {"lead_in_bytes": 9}
        assert result.status == StageStatus.DEGRADED
        assert result.total_packets == 1

    def test_a_file_with_no_marker_fails_and_says_why(self, tmp_path: Path) -> None:
        result = _run(tmp_path, make_raw_packet())
        assert result.status == StageStatus.FAILED
        assert "No sync marker" in str(result.errors[0])

    def test_an_empty_file_fails_like_a_file_with_no_marker(self, tmp_path: Path) -> None:
        result = _run(tmp_path, b"")
        assert result.status == StageStatus.FAILED
        assert "No sync marker" in str(result.errors[0])

    def test_a_clean_file_has_no_issues(self, tmp_path: Path) -> None:
        result = _run(tmp_path, SYNC + make_raw_packet())
        assert result.data_issues == {}
        assert result.status == StageStatus.SUCCESS

    def test_a_bad_length_does_not_swallow_the_next_frame(self, tmp_path: Path) -> None:
        bad = bytearray(make_raw_packet(seq_count=1, user_data=b"\x00" * 4))
        bad[4:6] = (40).to_bytes(2, "big")  # claims 41 bytes, runs across the next marker
        data = SYNC + bytes(bad) + SYNC + make_raw_packet(seq_count=2, user_data=b"\x01" * 60)
        assert _seqs(tmp_path, data) == [2]
        assert _run(tmp_path, data).data_issues == {"skipped_frames": 1}

    def test_a_marker_in_a_payload_ending_at_a_marker_is_data(self, tmp_path: Path) -> None:
        packet = make_raw_packet(seq_count=1, user_data=b"\x01" + SYNC + b"\x02")
        assert _seqs(tmp_path, SYNC + packet + SYNC + make_raw_packet(seq_count=2)) == [1, 2]

    def test_a_marker_in_a_payload_ending_the_file_is_data(self, tmp_path: Path) -> None:
        assert _seqs(tmp_path, SYNC + make_raw_packet(seq_count=1, user_data=SYNC + b"\x00")) == [1]

    def test_a_payload_marker_before_another_packet_is_counted_as_documented(
        self, tmp_path: Path
    ) -> None:
        first = make_raw_packet(seq_count=1, user_data=SYNC + b"\x00" * 4)
        second = make_raw_packet(seq_count=2)
        result = _run(tmp_path, SYNC + first + second)
        assert result.data_issues["skipped_frames"] >= 1
        assert result.status != StageStatus.SUCCESS

    @pytest.mark.parametrize("into_marker", [1, 2, 3])
    def test_a_length_ending_inside_a_marker_crosses_it(
        self, tmp_path: Path, into_marker: int
    ) -> None:
        bad = bytearray(make_raw_packet(seq_count=1, user_data=b"\x07" * 4))
        bad[4:6] = (3 + into_marker).to_bytes(2, "big")
        data = SYNC + bytes(bad) + SYNC + make_raw_packet(seq_count=2, user_data=b"\x09" * 8)
        assert _seqs(tmp_path, data) == [2]
        assert _run(tmp_path, data).data_issues == {"skipped_frames": 1}

    def test_a_payload_tail_gives_no_phantom_packet(self, tmp_path: Path) -> None:
        phantom = make_raw_packet(apid=9, seq_count=7, user_data=b"\x05" * 2)
        first = make_raw_packet(seq_count=1, user_data=SYNC + phantom + b"\x06" * 3)
        second = make_raw_packet(seq_count=2, user_data=b"\x08" * 4)
        data = SYNC + first + second
        assert _seqs(tmp_path, data) == []
        assert _run(tmp_path, data).data_issues == {"skipped_frames": 2}

    def test_a_real_frame_past_the_stated_length_survives(self, tmp_path: Path) -> None:
        bad = bytearray(make_raw_packet(seq_count=1, user_data=b"\x07" * 4))
        bad[4:6] = (20).to_bytes(2, "big")
        data = SYNC + bytes(bad) + SYNC + make_raw_packet(seq_count=2, user_data=b"\x09" * 40)
        assert _seqs(tmp_path, data) == [2]

    def test_a_huge_bad_length_costs_only_its_own_frame(self, tmp_path: Path) -> None:
        bad = bytearray(make_raw_packet(seq_count=1, user_data=b"\x07" * 4))
        bad[4:6] = (60).to_bytes(2, "big")
        rest = b"".join(
            SYNC + make_raw_packet(seq_count=seq, user_data=b"\x09" * 8) for seq in (2, 3, 4)
        )
        assert _seqs(tmp_path, SYNC + bytes(bad) + rest + b"\x03" * 40) == [2, 3, 4]

    def test_zero_fill_is_not_phantom_packets(self, tmp_path: Path) -> None:
        data = (
            SYNC + make_raw_packet(seq_count=1) + b"\x00" * 20 + SYNC + make_raw_packet(seq_count=2)
        )
        assert _seqs(tmp_path, data) == [1, 2]
        result = _run(tmp_path, data)
        assert result.stream_notes == {"fill_bytes": 20}
        assert "fill_bytes : 20" in result.summary()
        assert result.status == StageStatus.SUCCESS

    def test_ones_fill_is_fill(self, tmp_path: Path) -> None:
        result = _run(tmp_path, SYNC + make_raw_packet() + b"\xff" * 12)
        assert result.stream_notes == {"fill_bytes": 12}
        assert result.data_issues == {}

    def test_idle_packets_are_dropped_and_counted(self, tmp_path: Path) -> None:
        data = SYNC + make_raw_packet(apid=0x7FF) + make_raw_packet(apid=5, seq_count=1)
        result = _run(tmp_path, data)
        assert result.total_packets == 1
        assert result.stream_notes == {"idle_packets": 1}


class TestCsvWidths:
    def _read(self, tmp_path: Path, text: str, **config: object) -> list[TelemetryDataset]:
        path = tmp_path / "t.csv"
        path.write_text(text, encoding="utf-8")
        return list(CsvTelemetryExtractor(CsvExtractorConfig(path=path, **config)).extract())

    HEADER = "time,apid,seq_count,x,y\n"

    def test_an_extra_column_never_shifts_values(self, tmp_path: Path) -> None:
        ds = self._read(tmp_path, self.HEADER + "1.0,1,0,5,6\n2.0,1,1,7,8,9\n")[0]
        assert ds.metadata["wrong_width_rows"] == 1
        assert [s.eng_value for s in ds.parameters["x"].samples] == [5]
        assert [s.eng_value for s in ds.parameters["y"].samples] == [6]

    def test_a_short_row_is_counted(self, tmp_path: Path) -> None:
        ds = self._read(tmp_path, self.HEADER + "1.0,1,0,5,6\n2.0,1\n")[0]
        assert ds.metadata["wrong_width_rows"] == 1

    def test_a_trailing_delimiter_on_every_row_is_fine(self, tmp_path: Path) -> None:
        ds = self._read(tmp_path, self.HEADER + "1.0,1,0,5,6,\n2.0,1,1,7,8,\n")[0]
        assert ds.metadata["wrong_width_rows"] == 0
        assert [s.eng_value for s in ds.parameters["y"].samples] == [6, 8]

    def test_a_trailing_delimiter_on_the_header_too(self, tmp_path: Path) -> None:
        ds = self._read(tmp_path, "time,apid,seq_count,x,y,\n1.0,1,0,5,6,\n")[0]
        assert ds.metadata["wrong_width_rows"] == 0
        assert list(ds.parameters) == ["x", "y"]

    def test_a_lone_trailing_delimiter_has_the_wrong_width(self, tmp_path: Path) -> None:
        text = "time,apid,seq_count,temp,mode\n1.0,100,1,20,4\n2.0,100,2,21,5,\n3.0,100,3,22,6\n"
        ds = self._read(tmp_path, text)[0]
        assert ds.metadata["wrong_width_rows"] == 1
        assert [s.sample_time_tai for s in ds.parameters["temp"].samples] == [1.0, 3.0]

    def test_a_row_without_the_files_trailing_delimiter_has_the_wrong_width(
        self, tmp_path: Path
    ) -> None:
        text = self.HEADER + "1.0,1,0,5,6,\n2.0,1,1,7,8,\n3.0,1,2,9,\n"
        ds = self._read(tmp_path, text)[0]
        assert ds.metadata["wrong_width_rows"] == 1
        assert [s.eng_value for s in ds.parameters["x"].samples] == [5, 7]

    def test_a_trailing_header_decides_a_tie(self, tmp_path: Path) -> None:
        text = "time,apid,seq_count,x,\n1.0,1,0,5,\n2.0,1,1,7\n"
        ds = self._read(tmp_path, text)[0]
        assert ds.metadata["wrong_width_rows"] == 1
        assert [s.eng_value for s in ds.parameters["x"].samples] == [5]

    def test_the_header_alone_does_not_outvote_the_rows(self, tmp_path: Path) -> None:
        ds = self._read(tmp_path, "time,apid,seq_count,x,\n1.0,1,0,5\n2.0,1,1,7\n")[0]
        assert ds.metadata["wrong_width_rows"] == 0

    @pytest.mark.parametrize("batch_size", [1, 2, 1000])
    def test_the_form_is_one_decision_for_the_file(self, tmp_path: Path, batch_size: int) -> None:
        rows = "".join(f"{i}.0,1,{i},5,6\n" for i in range(4)) + "9.0,1,9,5,6,\n"
        batches = self._read(tmp_path, self.HEADER + rows, batch_size=batch_size)
        assert sum(ds.metadata["wrong_width_rows"] for ds in batches) == 1

    def test_the_decision_takes_a_bounded_look(self, tmp_path: Path) -> None:
        rows = "".join(f"{i}.0,1,{i},5,6\n" for i in range(1000)) + "9e3,1,9,5,6,\n"
        batches = self._read(tmp_path, self.HEADER + rows, batch_size=300)
        assert sum(ds.metadata["wrong_width_rows"] for ds in batches) == 1
        assert sum(ds.metadata["rows"] for ds in batches) == 1001

    @pytest.mark.parametrize("cell", ["nan", "NaN", "inf", "-inf", "Infinity", "1e999"])
    def test_a_time_that_is_not_finite_is_a_bad_row(self, tmp_path: Path, cell: str) -> None:
        ds = self._read(tmp_path, f"{self.HEADER}1.0,1,0,5,6\n{cell},1,1,7,8\n")[0]
        assert ds.metadata["skipped_rows"] == 1
        assert [s.sample_time_tai for s in ds.parameters["x"].samples] == [1.0]

    def test_a_repeated_column_fails(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match=r"repeats column names: \['x'\]"):
            self._read(tmp_path, "time,apid,seq_count,x,x\n1.0,1,0,5,6\n")

    def test_an_empty_file_fails(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="CSV file is empty"):
            self._read(tmp_path, "")

    def test_a_header_with_no_rows_is_not_an_error(self, tmp_path: Path) -> None:
        assert self._read(tmp_path, self.HEADER) == []

    def test_a_stray_quote_in_the_last_column_fails(self, tmp_path: Path) -> None:
        text = self.HEADER + '1.0,1,0,5,6\n2.0,1,1,5,"6\n3.0,1,2,5,6\n4.0,1,3,5,6\n'
        with pytest.raises(ValueError, match="starting at line 3.*unclosed quote"):
            self._read(tmp_path, text, batch_size=1)

    def test_a_stray_quote_in_a_middle_column_fails(self, tmp_path: Path) -> None:
        text = self.HEADER + '1.0,1,0,5,6\n2.0,1,"1,5,6\n3.0,1,2,5,6\n4.0,1,3,5,6\n'
        with pytest.raises(ValueError, match="starting at line 3"):
            self._read(tmp_path, text)

    def test_a_file_cut_off_inside_a_quoted_cell_fails(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="Malformed CSV"):
            self._read(tmp_path, self.HEADER + '1.0,1,0,5,6\n2.0,1,1,5,"cut off')

    def test_a_stray_quote_after_the_sample_fails_too(self, tmp_path: Path) -> None:
        rows = "".join(f"{i}.0,1,{i},5,6\n" for i in range(1500))
        with pytest.raises(ValueError, match="starting at line 1502"):
            self._read(tmp_path, self.HEADER + rows + '2e3,1,1,"5,6\n2e3,1,2,5,6\n')

    def test_bad_quoting_fails_the_run(self, tmp_path: Path) -> None:
        path = tmp_path / "t.csv"
        path.write_text(self.HEADER + '1.0,1,0,5,"6\n', encoding="utf-8")
        extractor = CsvTelemetryExtractor(CsvExtractorConfig(path=path))
        result = Pipeline(PipelineConfig(name="x"), extractor).run()
        assert result.status == StageStatus.FAILED
        assert "unclosed quote" in str(result.errors[0])

    def test_good_quoting_still_reads(self, tmp_path: Path) -> None:
        text = 'time,apid,seq_count,x\n1.0,1,0,"a,b"\n2.0,1,1,"two\nlines"\n3.0,1,2,"c"'
        ds = self._read(tmp_path, text)[0]
        assert [str(s.eng_value).split() for s in ds.parameters["x"].samples] == [
            ["a,b"],
            ["two", "lines"],
            ["c"],
        ]

    def test_wrong_widths_reach_the_result(self, tmp_path: Path) -> None:
        path = tmp_path / "t.csv"
        path.write_text(self.HEADER + "1.0,1,0,5,6,7\n", encoding="utf-8")
        extractor = CsvTelemetryExtractor(CsvExtractorConfig(path=path))
        result = Pipeline(PipelineConfig(name="x"), extractor).run()
        assert result.data_issues == {"wrong_width_rows": 1}

    @pytest.mark.parametrize("cell", ["E1", "e5", "-", ".", "-.", "1e", "E"])
    def test_cells_that_are_not_numbers_stay_text(self, tmp_path: Path, cell: str) -> None:
        ds = self._read(tmp_path, f"time,apid,seq_count,x\n1.0,1,0,{cell}\n")[0]
        assert ds.parameters["x"].samples[0].eng_value == cell

    @pytest.mark.parametrize(
        "cell,value", [("1.5", 1.5), ("-2e3", -2000.0), (".5", 0.5), ("0", 0), ("1.", 1.0)]
    )
    def test_numbers_are_numbers(self, tmp_path: Path, cell: str, value: float) -> None:
        ds = self._read(tmp_path, f"time,apid,seq_count,x\n1.0,1,0,{cell}\n")[0]
        assert ds.parameters["x"].samples[0].eng_value == value

    def test_source_id_reaches_the_dataset(self, tmp_path: Path) -> None:
        ds = self._read(tmp_path, "time,apid,seq_count,x\n1.0,1,0,1\n", source_id="OPS-SAT")[0]
        assert ds.metadata["source_id"] == "OPS-SAT"

    def test_a_named_column_that_is_absent_is_an_error(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="no parameter columns"):
            self._read(tmp_path, "time,apid,seq_count,x\n1.0,1,0,1\n", parameter_columns=["z"])


class TestNoUnitIsDistinct:
    def test_a_unit_after_no_unit_is_refused(self) -> None:
        dataset = TelemetryDataset()
        dataset.add_parameter(_sample(1.0))
        with pytest.raises(ValueError, match="mixes units"):
            dataset.add_parameter(_sample(2.0, unit="K"))

    def test_merge_refuses_it(self) -> None:
        a, b = TelemetryDataset(), TelemetryDataset()
        a.add_parameter(_sample(1.0))
        b.add_parameter(_sample(2.0, unit="K"))
        with pytest.raises(ValueError, match="mixes units"):
            a.merge(b)

    def test_hdf5_refuses_it_across_batches(self, tmp_path: Path) -> None:
        path = tmp_path / "t.h5"
        batches = [[_sample(1.0)], [_sample(2.0, unit="K")]]
        loader = HDF5Loader(HDF5LoaderConfig(path=path))
        assert not Pipeline(PipelineConfig(name="x"), _Batches(batches), [], loader).run().ok
        with h5py.File(path) as hf:
            assert len(hf["telemetry/x/eng_value"]) == 1


def test_inspect_refuses_zero_packets() -> None:
    telemetry = (
        Path(__file__).parent.parent
        / "examples"
        / "data"
        / "CYGNSS_F7_L0_2022_086_10_15_V01_F__first101pkts.tlm"
    )
    result = CliRunner().invoke(cli, ["inspect", str(telemetry), "--max-packets", "0"])
    assert result.exit_code == 2
    assert "Traceback" not in result.output
