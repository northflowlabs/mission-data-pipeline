"""A bad stream is an error, no sample is lost silently, and decom works at bit level."""

from __future__ import annotations

import math
from pathlib import Path

import h5py
import pandas as pd
import pytest

from mdp.core.pipeline import Pipeline, PipelineConfig
from mdp.models.dataset import TelemetryDataset
from mdp.models.packet import CCSDSPrimaryHeader, TelemetryPacket
from mdp.models.parameter import EngineeringParameter, ParameterType
from mdp.observability.hooks import HookManager
from mdp.plugins.extractors.binary import BinaryExtractorConfig, BinaryPacketExtractor
from mdp.plugins.loaders.csv import CsvLoader, CsvLoaderConfig
from mdp.plugins.loaders.hdf5 import HDF5Loader, HDF5LoaderConfig
from mdp.plugins.loaders.parquet import ParquetLoader, ParquetLoaderConfig
from mdp.plugins.transformers.decom import DecomConfig, DecomTransformer, ParameterDefinition
from tests.conftest import make_raw_packet

TELEMETRY = (
    Path(__file__).parent.parent
    / "examples"
    / "data"
    / "CYGNSS_F7_L0_2022_086_10_15_V01_F__first101pkts.tlm"
)


def _param(name: str = "x", time: float = 0.0, value: float | str = 1.0) -> EngineeringParameter:
    return EngineeringParameter(
        name=name, apid=1, seq_count=0, sample_time_tai=time, raw_value=value, eng_value=value
    )


def _extract(path: Path, **config: object) -> list[TelemetryDataset]:
    return list(BinaryPacketExtractor(BinaryExtractorConfig(path=path, **config)).extract())


class TestBadStreams:
    def test_a_truncated_packet_raises_with_its_offset(self, tmp_path: Path) -> None:
        whole = make_raw_packet(seq_count=0)
        path = tmp_path / "t.bin"
        path.write_bytes(whole + make_raw_packet(seq_count=1)[:-2])
        with pytest.raises(ValueError, match=f"Truncated packet at byte {len(whole)}"):
            _extract(path)

    def test_a_partial_header_raises(self, tmp_path: Path) -> None:
        path = tmp_path / "t.bin"
        path.write_bytes(make_raw_packet() + b"\x00\x01")
        with pytest.raises(ValueError):
            _extract(path)

    def test_lost_sync_raises(self, tmp_path: Path) -> None:
        whole = make_raw_packet()
        path = tmp_path / "t.bin"
        path.write_bytes(whole + b"\xff" * 16)
        with pytest.raises(ValueError, match=f"Invalid CCSDS header at byte {len(whole)}"):
            _extract(path)

    def test_a_bad_stream_fails_the_pipeline(self, tmp_path: Path) -> None:
        path = tmp_path / "t.bin"
        path.write_bytes(make_raw_packet()[:-1])
        extractor = BinaryPacketExtractor(BinaryExtractorConfig(path=path))
        assert not Pipeline(PipelineConfig(name="x"), extractor).run().ok

    def test_frame_sync_recovers_and_counts_what_it_skipped(self, tmp_path: Path) -> None:
        sync = b"\x1a\xcf\xfc\x1d"
        path = tmp_path / "t.bin"
        path.write_bytes(sync + b"\xfe\xfd" * 3 + sync + make_raw_packet())
        batches = _extract(path, frame_sync=True)
        assert len(batches[0]) == 1
        assert batches[0].metadata["skipped_frames"] == 1

    def test_a_clean_file_still_reads_to_the_end(self) -> None:
        assert sum(len(ds) for ds in _extract(TELEMETRY, sec_hdr_length=10)) == 101

    def test_version_bits_are_validated(self) -> None:
        with pytest.raises(ValueError):
            CCSDSPrimaryHeader.from_bytes(b"\x20\x01\xc0\x00\x00\x03")


class TestBitLevelDecom:
    def _decode(self, data: bytes, definition: ParameterDefinition) -> object:
        packet = TelemetryPacket.from_bytes(make_raw_packet(apid=1, user_data=data))
        dataset = TelemetryDataset(packets=[packet])
        transformer = DecomTransformer(DecomConfig(parameters=[definition]))
        transformer.setup()
        transformer.transform(dataset)
        return dataset.parameters[definition.name].samples[0].raw_value

    def _field(self, **kwargs: object) -> ParameterDefinition:
        fields: dict = {"name": "f", "apid": 1, "byte_offset": 0, "param_type": ParameterType.UINT}
        return ParameterDefinition(**{**fields, **kwargs})

    def test_real_utc_fields_of_a_cygnss_secondary_header(self) -> None:
        # Without sec_hdr_length the secondary header stays in user_data. These bit
        # positions are CYGNSS's: 2022, day 84, 21:43, matching the file name's date.
        fields = {
            "year": (2, 6, 12),
            "day": (4, 2, 9),
            "hour": (5, 3, 5),
            "minute": (6, 0, 6),
        }
        definitions = [
            self._field(name=n, apid=393, byte_offset=b, bit_offset=o, bit_length=length)
            for n, (b, o, length) in fields.items()
        ]
        transformer = DecomTransformer(DecomConfig(parameters=definitions))
        transformer.setup()
        dataset = _extract(TELEMETRY, apid_filter=[393], batch_size=1)[0]
        transformer.transform(dataset)
        decoded = {n: dataset.parameters[n].samples[0].raw_value for n in fields}
        assert decoded == {"year": 2022, "day": 84, "hour": 21, "minute": 43}

    def test_a_field_can_cross_bytes(self) -> None:
        field = self._field(bit_offset=4, bit_length=8)
        assert self._decode(b"\xab\xcd\x00\x00", field) == 0xBC

    def test_a_single_bit(self) -> None:
        field = self._field(param_type=ParameterType.BOOLEAN, bit_offset=7, bit_length=1)
        assert self._decode(b"\x01\x00\x00\x00", field) is True
        assert self._decode(b"\xfe\x00\x00\x00", field) is False

    @pytest.mark.parametrize(
        "bit_length,data,expected", [(4, b"\xf0", -1), (4, b"\x70", 7), (12, b"\x80\x00", -2048)]
    )
    def test_signed_fields_of_any_length(self, bit_length, data, expected) -> None:
        field = self._field(param_type=ParameterType.INT, bit_length=bit_length)
        assert self._decode(data + b"\x00\x00\x00", field) == expected

    def test_a_24_bit_signed_field(self) -> None:
        field = self._field(param_type=ParameterType.INT, bit_length=24)
        assert self._decode(b"\xff\xff\xff\x00", field) == -1

    def test_unaligned_floats_are_refused(self) -> None:
        with pytest.raises(ValueError, match="byte-aligned"):
            self._field(param_type=ParameterType.FLOAT, bit_offset=1, bit_length=32)

    def test_unaligned_little_endian_is_refused(self) -> None:
        with pytest.raises(ValueError, match="byte-aligned"):
            self._field(bit_offset=1, bit_length=8, little_endian=True)


class TestDataset:
    def _with(self, *params: EngineeringParameter) -> TelemetryDataset:
        dataset = TelemetryDataset()
        for param in params:
            dataset.add_parameter(param)
        return dataset

    def test_merge_leaves_both_inputs_unchanged(self) -> None:
        a = self._with(_param(time=1.0))
        b = self._with(_param(time=2.0))
        merged = a.merge(b)
        assert merged.parameters["x"].count == 2
        assert a.parameters["x"].count == 1
        assert b.parameters["x"].count == 1

    def test_wide_output_refuses_to_lose_samples_at_one_time(self) -> None:
        dataset = self._with(_param(time=1.0, value=1.0), _param(time=1.0, value=2.0))
        with pytest.raises(ValueError, match="several samples at one time"):
            dataset.to_wide_dataframe()

    def test_wide_output_needs_a_time(self) -> None:
        with pytest.raises(ValueError, match="without a time"):
            self._with(_param(time=math.nan)).to_wide_dataframe()

    def test_wide_output_keeps_every_sample(self) -> None:
        dataset = self._with(_param(time=1.0), _param(time=2.0), _param("y", time=2.0))
        wide = dataset.to_wide_dataframe()
        assert wide["x"].count() == 2
        assert wide["y"].count() == 1

    def test_seq_count_is_not_presented_as_time(self, tmp_path: Path) -> None:
        extractor = BinaryPacketExtractor(
            BinaryExtractorConfig(path=TELEMETRY, apid_filter=[1313], sec_hdr_length=10)
        )
        week = ParameterDefinition(
            name="gps_week",
            apid=1313,
            byte_offset=4,
            bit_length=16,
            param_type=ParameterType.UINT,
            little_endian=True,
        )
        Pipeline(
            PipelineConfig(name="x"),
            extractor,
            [DecomTransformer(DecomConfig(parameters=[week]))],
            ParquetLoader(ParquetLoaderConfig(output_dir=tmp_path)),
        ).run()
        df = pd.read_parquet(tmp_path / "gps_week.parquet")
        assert df["time_tai"].isna().all()
        assert df["seq_count"].is_monotonic_increasing


class _Batches(BinaryPacketExtractor):
    """Yields prepared parameter batches."""

    def __init__(self, batches: list[list[EngineeringParameter]]) -> None:
        super().__init__(BinaryExtractorConfig(path=TELEMETRY))
        self.batches = batches

    def extract(self):  # type: ignore[no-untyped-def]
        for params in self.batches:
            dataset = TelemetryDataset()
            for param in params:
                dataset.add_parameter(param)
            yield dataset


class TestLoaders:
    def test_hdf5_rerun_replaces_the_file(self, tmp_path: Path) -> None:
        path = tmp_path / "t.h5"
        for _ in range(2):
            Pipeline(
                PipelineConfig(name="x"),
                _Batches([[_param()]]),
                [],
                HDF5Loader(HDF5LoaderConfig(path=path)),
            ).run()
        with h5py.File(path) as hf:
            assert len(hf["telemetry/x/eng_value"]) == 1

    def test_hdf5_mode_a_keeps_earlier_runs(self, tmp_path: Path) -> None:
        path = tmp_path / "t.h5"
        for _ in range(2):
            Pipeline(
                PipelineConfig(name="x"),
                _Batches([[_param()]]),
                [],
                HDF5Loader(HDF5LoaderConfig(path=path, mode="a")),
            ).run()
        with h5py.File(path) as hf:
            assert len(hf["telemetry/x/eng_value"]) == 2

    def test_hdf5_columns_stay_aligned_when_values_turn_numeric(self, tmp_path: Path) -> None:
        path = tmp_path / "t.h5"
        batches = [[_param(value="NOMINAL")], [_param(value=1.0)]]
        result = Pipeline(
            PipelineConfig(name="x"), _Batches(batches), [], HDF5Loader(HDF5LoaderConfig(path=path))
        ).run()
        assert result.ok
        with h5py.File(path) as hf:
            group = hf["telemetry/x"]
            assert len(group["eng_value_str"]) == len(group["time_tai"]) == 2

    def test_hdf5_refuses_text_after_numbers_without_writing(self, tmp_path: Path) -> None:
        path = tmp_path / "t.h5"
        batches = [[_param(value=1.0)], [_param(value="NOMINAL")]]
        result = Pipeline(
            PipelineConfig(name="x"), _Batches(batches), [], HDF5Loader(HDF5LoaderConfig(path=path))
        ).run()
        assert not result.ok
        with h5py.File(path) as hf:
            assert len(hf["telemetry/x/time_tai"]) == len(hf["telemetry/x/eng_value"]) == 1

    def test_csv_keeps_small_values(self, tmp_path: Path) -> None:
        value = 2.1487e-05
        loader = CsvLoader(CsvLoaderConfig(output_dir=tmp_path))
        Pipeline(PipelineConfig(name="x"), _Batches([[_param(value=value)]]), [], loader).run()
        assert pd.read_csv(tmp_path / "x.csv")["eng_value"][0] == value


class TestFailedLoads:
    def test_a_failed_load_fires_no_batch_loaded_and_is_not_counted(self, tmp_path: Path) -> None:
        class FailsFirst(ParquetLoader):
            def load(self, dataset: TelemetryDataset) -> None:
                if not getattr(self, "failed", False):
                    self.failed = True
                    raise OSError("disk full")
                super().load(dataset)

        hooks = HookManager()
        loaded: list[int] = []
        hooks.register("batch.loaded", lambda ds, r: loaded.append(ds.sample_count()))

        pipeline = Pipeline(
            PipelineConfig(name="x", stop_on_error=False),
            _Batches([[_param()], [_param(), _param(time=1.0)]]),
            [],
            FailsFirst(ParquetLoaderConfig(output_dir=tmp_path)),
            hooks,
        )
        result = pipeline.run()

        assert loaded == [2]
        assert result.total_samples == 2
        assert result.batches_processed == 1
        assert not result.ok
