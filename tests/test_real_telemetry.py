"""End-to-end tests on the real telemetry in examples/data, hooks and multi-batch loads."""

from __future__ import annotations

import json
from datetime import datetime, timedelta
from pathlib import Path

import h5py
import pandas as pd
import pytest
from click.testing import CliRunner

from mdp.cli.main import cli
from mdp.core.pipeline import Pipeline, PipelineConfig
from mdp.models.parameter import ParameterType
from mdp.observability.hooks import HookManager
from mdp.plugins.extractors.binary import BinaryExtractorConfig, BinaryPacketExtractor
from mdp.plugins.loaders.csv import CsvLoader, CsvLoaderConfig
from mdp.plugins.loaders.hdf5 import HDF5Loader, HDF5LoaderConfig
from mdp.plugins.loaders.parquet import ParquetLoader, ParquetLoaderConfig
from mdp.plugins.transformers.decom import DecomConfig, DecomTransformer, ParameterDefinition

TELEMETRY = (
    Path(__file__).parent.parent
    / "examples"
    / "data"
    / "CYGNSS_F7_L0_2022_086_10_15_V01_F__first101pkts.tlm"
)
DIAG_APID = 1313

WEEK = ParameterDefinition(
    name="gps_week",
    apid=DIAG_APID,
    byte_offset=4,
    bit_length=16,
    param_type=ParameterType.UINT,
    little_endian=True,
)
SECONDS = ParameterDefinition(
    name="gps_seconds",
    apid=DIAG_APID,
    byte_offset=6,
    bit_length=64,
    param_type=ParameterType.DOUBLE,
    little_endian=True,
)


def _extractor(batch_size: int = 4, apids: list[int] | None = None) -> BinaryPacketExtractor:
    return BinaryPacketExtractor(
        BinaryExtractorConfig(
            path=TELEMETRY, batch_size=batch_size, apid_filter=apids, sec_hdr_length=10
        )
    )


def _pipeline(loader: object, hooks: HookManager | None = None) -> Pipeline:
    return Pipeline(
        config=PipelineConfig(name="real"),
        extractor=_extractor(apids=[DIAG_APID]),
        transformers=[DecomTransformer(DecomConfig(parameters=[WEEK, SECONDS]))],
        loader=loader,  # type: ignore[arg-type]
        hooks=hooks,
    )


def test_the_whole_file_parses_with_every_apid() -> None:
    packets = [p for ds in _extractor(batch_size=1000).extract() for p in ds.packets]
    assert len(packets) == 101
    assert {p.apid for p in packets} == {384, 386, 391, 392, 393, 394, 1313}


def _header_utc(secondary_header: bytes) -> datetime:
    bits = int.from_bytes(secondary_header[2:10], "big")

    def field(start: int, width: int) -> int:
        return (bits >> (64 - start - width)) & ((1 << width) - 1)

    return datetime(field(6, 12), 1, 1) + timedelta(
        days=field(18, 9) - 1,
        hours=field(27, 5),
        minutes=field(32, 6),
        seconds=field(38, 6),
        microseconds=field(44, 20),
    )


def test_decoded_gps_time_agrees_with_every_packet_secondary_header() -> None:
    # Two independent clocks in each packet: GPS time in the data, UTC in the header.
    # GPS was 18 s ahead of UTC in 2022. Any wrong offset or byte order fails this.
    transformer = DecomTransformer(DecomConfig(parameters=[WEEK, SECONDS]))
    transformer.setup()
    compared = 0
    for dataset in _extractor(apids=[DIAG_APID]).extract():
        transformer.transform(dataset)
        weeks = dataset.parameters["gps_week"].samples
        seconds = dataset.parameters["gps_seconds"].samples
        for packet, week, sec in zip(dataset.packets, weeks, seconds, strict=True):
            gps_utc = datetime(1980, 1, 6) + timedelta(
                weeks=int(week.eng_value), seconds=float(sec.eng_value) - 18
            )
            assert abs(gps_utc - _header_utc(packet.secondary_header)) < timedelta(seconds=1)
            compared += 1
    assert compared == 9


def test_parquet_keeps_every_batch_of_a_run(tmp_path: Path) -> None:
    result = _pipeline(ParquetLoader(ParquetLoaderConfig(output_dir=tmp_path))).run()
    assert result.ok
    assert result.batches_processed == 3  # 9 packets in batches of 4
    assert len(pd.read_parquet(tmp_path / "gps_week.parquet")) == 9


def test_parquet_replaces_earlier_runs_by_default(tmp_path: Path) -> None:
    pipeline = _pipeline(ParquetLoader(ParquetLoaderConfig(output_dir=tmp_path)))
    pipeline.run()
    pipeline.run()
    assert len(pd.read_parquet(tmp_path / "gps_week.parquet")) == 9


def test_csv_keeps_every_batch_of_a_run(tmp_path: Path) -> None:
    _pipeline(CsvLoader(CsvLoaderConfig(output_dir=tmp_path))).run()
    assert len(pd.read_csv(tmp_path / "gps_week.csv")) == 9


def test_csv_wide_keeps_every_batch_of_a_run(tmp_path: Path) -> None:
    _pipeline(CsvLoader(CsvLoaderConfig(output_dir=tmp_path, wide_format=True))).run()
    assert len(pd.read_csv(tmp_path / "telemetry.csv")) == 9


def test_hdf5_mode_w_keeps_every_batch_of_a_run(tmp_path: Path) -> None:
    path = tmp_path / "t.h5"
    _pipeline(HDF5Loader(HDF5LoaderConfig(path=path, mode="w"))).run()
    with h5py.File(path, "r") as hf:
        assert len(hf["telemetry/gps_week/eng_value"]) == 9


def test_hooks_fire_in_order_with_their_documented_arguments(tmp_path: Path) -> None:
    hooks = HookManager()
    events: list[str] = []

    hooks.register("pipeline.start", lambda name: events.append(f"start:{name}"))
    hooks.register("batch.extracted", lambda ds: events.append(f"extracted:{len(ds)}"))
    hooks.register(
        "batch.transformed", lambda ds: events.append(f"transformed:{ds.sample_count()}")
    )
    hooks.register("batch.loaded", lambda ds, r: events.append(f"loaded:{r.stage_name}"))
    hooks.register("pipeline.complete", lambda r: events.append(f"complete:{r.total_samples}"))

    _pipeline(ParquetLoader(ParquetLoaderConfig(output_dir=tmp_path)), hooks).run()

    assert events[:4] == ["start:real", "extracted:4", "transformed:8", "loaded:ParquetLoader"]
    assert events[-1] == "complete:18"
    assert sum(e.startswith("loaded:") for e in events) == 3


def test_stage_error_hook_fires_when_the_loader_fails(tmp_path: Path) -> None:
    class Failing(ParquetLoader):
        def load(self, dataset: object) -> None:
            raise OSError("disk full")

    hooks = HookManager()
    seen: list[tuple[str, str]] = []
    hooks.register("stage.error", lambda stage, exc: seen.append((stage, str(exc))))

    result = _pipeline(Failing(ParquetLoaderConfig(output_dir=tmp_path)), hooks).run()

    assert not result.ok
    assert seen == [("Failing", "disk full")]


def test_stage_error_hook_fires_when_the_extractor_fails(tmp_path: Path) -> None:
    hooks = HookManager()
    seen: list[str] = []
    hooks.register("stage.error", lambda stage, exc: seen.append(stage))
    extractor = BinaryPacketExtractor(BinaryExtractorConfig(path=tmp_path / "missing.bin"))

    result = Pipeline(PipelineConfig(name="x"), extractor, hooks=hooks).run()

    assert not result.ok
    assert seen == ["BinaryPacketExtractor"]


def test_pipeline_metrics_record_stages(tmp_path: Path) -> None:
    pipeline = _pipeline(ParquetLoader(ParquetLoaderConfig(output_dir=tmp_path)))
    pipeline.run()
    assert pipeline.metrics.batches == 3
    assert pipeline.metrics.stage("DecomTransformer").invocations == 3  # type: ignore[union-attr]


def _write_cli_configs(tmp_path: Path) -> tuple[Path, Path, Path]:
    extractor = tmp_path / "extractor.json"
    extractor.write_text(
        json.dumps({"path": str(TELEMETRY), "sec_hdr_length": 10, "batch_size": 4})
    )
    decom = tmp_path / "decom.json"
    decom.write_text(json.dumps({"parameters": [WEEK.model_dump(mode="json")]}))
    loader = tmp_path / "loader.json"
    loader.write_text(json.dumps({"output_dir": str(tmp_path / "out")}))
    return extractor, decom, loader


def test_cli_run_writes_output_with_a_transformer_config(tmp_path: Path) -> None:
    extractor, decom, loader = _write_cli_configs(tmp_path)
    result = CliRunner().invoke(
        cli,
        [
            "run",
            "--extractor",
            "binary",
            "--extractor-config",
            str(extractor),
            "--transformer",
            "decom",
            "--transformer-config",
            f"decom={decom}",
            "--loader",
            "parquet",
            "--loader-config",
            str(loader),
        ],
    )
    assert result.exit_code == 0, result.output
    assert len(pd.read_parquet(tmp_path / "out" / "gps_week.parquet")) == 9


def test_cli_run_fails_loudly_when_nothing_reaches_the_loader(tmp_path: Path) -> None:
    extractor, _, loader = _write_cli_configs(tmp_path)
    result = CliRunner().invoke(
        cli,
        [
            "run",
            "--extractor",
            "binary",
            "--extractor-config",
            str(extractor),
            "--transformer",
            "apid_filter",
            "--loader",
            "parquet",
            "--loader-config",
            str(loader),
        ],
    )
    assert result.exit_code == 1


@pytest.mark.parametrize("value", ["decom", "other=x.json", "decom=missing.json"])
def test_cli_rejects_bad_transformer_configs(value: str) -> None:
    result = CliRunner().invoke(
        cli,
        ["run", "--extractor", "binary", "--transformer", "decom", "--transformer-config", value],
    )
    assert result.exit_code == 2
