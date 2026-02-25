"""Tests for CCSDS packet models."""

from __future__ import annotations

import struct

import pytest

from mdp.models.packet import (
    CCSDSPrimaryHeader,
    PacketSequenceFlags,
    TelemetryPacket,
)
from tests.conftest import make_raw_packet


class TestCCSDSPrimaryHeader:
    def test_roundtrip(self) -> None:
        header = CCSDSPrimaryHeader(
            apid=0x1A2,
            seq_flags=PacketSequenceFlags.UNSEGMENTED,
            seq_count=1234,
            data_length=15,
            sec_hdr_flag=1,
        )
        raw = header.to_bytes()
        assert len(raw) == 6
        recovered = CCSDSPrimaryHeader.from_bytes(raw)
        assert recovered == header

    def test_from_bytes_too_short(self) -> None:
        with pytest.raises(ValueError, match="6 bytes"):
            CCSDSPrimaryHeader.from_bytes(b"\x00\x00\x00")

    def test_total_length(self) -> None:
        header = CCSDSPrimaryHeader(apid=0x100, seq_count=0, data_length=19)
        assert header.packet_data_length == 20
        assert header.total_length == 26

    def test_apid_max(self) -> None:
        header = CCSDSPrimaryHeader(apid=0x7FF, seq_count=0, data_length=0)
        raw = header.to_bytes()
        recovered = CCSDSPrimaryHeader.from_bytes(raw)
        assert recovered.apid == 0x7FF

    def test_seq_count_max(self) -> None:
        header = CCSDSPrimaryHeader(apid=0x001, seq_count=0x3FFF, data_length=0)
        raw = header.to_bytes()
        recovered = CCSDSPrimaryHeader.from_bytes(raw)
        assert recovered.seq_count == 0x3FFF


class TestTelemetryPacket:
    def test_from_bytes_basic(self, raw_packet_bytes: bytes) -> None:
        packet = TelemetryPacket.from_bytes(raw_packet_bytes)
        assert packet.apid == 0x100
        assert packet.seq_count == 42
        assert packet.user_data == b"\xDE\xAD\xBE\xEF"

    def test_from_bytes_with_metadata(self, raw_packet_bytes: bytes) -> None:
        packet = TelemetryPacket.from_bytes(
            raw_packet_bytes,
            source_time_tai=86400.0,
            source_id="GS1",
        )
        assert packet.source_time_tai == 86400.0
        assert packet.source_id == "GS1"

    def test_data_length_validation(self) -> None:
        header = CCSDSPrimaryHeader(
            apid=0x100, seq_count=0, data_length=7, sec_hdr_flag=0
        )
        with pytest.raises(ValueError, match="data field mismatch"):
            TelemetryPacket(
                header=header,
                secondary_header=b"",
                user_data=b"\x00\x00\x00",
            )

    def test_packet_is_frozen(self, telemetry_packet: TelemetryPacket) -> None:
        with pytest.raises(Exception):
            telemetry_packet.user_data = b"\xFF"  # type: ignore[misc]

    def test_bytearray_user_data_coerced(self) -> None:
        header = CCSDSPrimaryHeader(apid=0x010, seq_count=0, data_length=3, sec_hdr_flag=0)
        packet = TelemetryPacket(
            header=header,
            secondary_header=b"",
            user_data=bytearray(b"\x01\x02\x03\x04"),
        )
        assert isinstance(packet.user_data, bytes)
