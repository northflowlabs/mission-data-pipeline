"""Tests for BinaryPacketExtractor."""

from __future__ import annotations

from pathlib import Path

import pytest

from mdp.plugins.extractors.binary import BinaryExtractorConfig, BinaryPacketExtractor
from tests.conftest import make_raw_packet


class TestBinaryPacketExtractor:
    def test_reads_all_packets(self, binary_telemetry_file: Path) -> None:
        cfg = BinaryExtractorConfig(path=binary_telemetry_file, batch_size=100)
        extractor = BinaryPacketExtractor(cfg)
        all_packets = []
        for ds in extractor.extract():
            all_packets.extend(ds.packets)
        assert len(all_packets) == 10

    def test_batching(self, binary_telemetry_file: Path) -> None:
        cfg = BinaryExtractorConfig(path=binary_telemetry_file, batch_size=3)
        extractor = BinaryPacketExtractor(cfg)
        datasets = list(extractor.extract())
        total = sum(len(ds.packets) for ds in datasets)
        assert total == 10
        assert len(datasets) == 4

    def test_apid_filter(self, binary_telemetry_file: Path) -> None:
        cfg = BinaryExtractorConfig(path=binary_telemetry_file, batch_size=100, apid_filter=[0x200])
        extractor = BinaryPacketExtractor(cfg)
        packets = [p for ds in extractor.extract() for p in ds.packets]
        assert packets == []

    def test_apid_filter_match(self, binary_telemetry_file: Path) -> None:
        cfg = BinaryExtractorConfig(path=binary_telemetry_file, batch_size=100, apid_filter=[0x100])
        extractor = BinaryPacketExtractor(cfg)
        packets = [p for ds in extractor.extract() for p in ds.packets]
        assert len(packets) == 10

    def test_seq_count_sequential(self, binary_telemetry_file: Path) -> None:
        cfg = BinaryExtractorConfig(path=binary_telemetry_file, batch_size=100)
        extractor = BinaryPacketExtractor(cfg)
        packets = [p for ds in extractor.extract() for p in ds.packets]
        seq_counts = [p.seq_count for p in packets]
        assert seq_counts == list(range(10))

    def test_file_not_found(self, tmp_path: Path) -> None:
        cfg = BinaryExtractorConfig(path=tmp_path / "nonexistent.bin")
        extractor = BinaryPacketExtractor(cfg)
        with pytest.raises(FileNotFoundError):
            list(extractor.extract())

    def test_metadata_in_dataset(self, binary_telemetry_file: Path) -> None:
        cfg = BinaryExtractorConfig(
            path=binary_telemetry_file, batch_size=100, source_id="GS_KOUROU"
        )
        extractor = BinaryPacketExtractor(cfg)
        datasets = list(extractor.extract())
        assert datasets[0].metadata["extractor"] == "binary"
        for ds in datasets:
            for p in ds.packets:
                assert p.source_id == "GS_KOUROU"

    def test_frame_sync(self, tmp_path: Path) -> None:
        """Verify sync-marker scanning with interleaved garbage bytes."""
        from mdp.plugins.extractors.binary import SYNC_MARKER

        outfile = tmp_path / "framed.bin"
        with open(outfile, "wb") as fh:
            for i in range(3):
                fh.write(b"\xff\xff")
                fh.write(SYNC_MARKER)
                fh.write(make_raw_packet(apid=0x050, seq_count=i, user_data=b"\x00\x00\x00\x00"))

        cfg = BinaryExtractorConfig(path=outfile, batch_size=100, frame_sync=True)
        extractor = BinaryPacketExtractor(cfg)
        packets = [p for ds in extractor.extract() for p in ds.packets]
        assert len(packets) == 3
