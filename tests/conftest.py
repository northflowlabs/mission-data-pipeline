"""Shared pytest fixtures for MDP test suite."""

from __future__ import annotations

import struct
import time
from pathlib import Path

import pytest

from mdp.models.packet import CCSDSPrimaryHeader, PacketSequenceFlags, TelemetryPacket
from mdp.models.dataset import TelemetryDataset
from mdp.models.parameter import EngineeringParameter


# --------------------------------------------------------------------------- #
#  Helper — build a minimal valid CCSDS packet as raw bytes                    #
# --------------------------------------------------------------------------- #


def make_raw_packet(
    apid: int = 0x123,
    seq_count: int = 0,
    user_data: bytes = b"\x00" * 4,
    sec_hdr: bool = False,
    sec_hdr_bytes: bytes = b"",
) -> bytes:
    """Build a minimal CCSDS Space Packet byte string.

    Parameters
    ----------
    sec_hdr:
        Set the secondary header present flag in the primary header.
    sec_hdr_bytes:
        Raw bytes to include as the secondary header (length is mission-specific).
        If non-empty, ``sec_hdr`` is forced True.
    """
    if sec_hdr_bytes:
        sec_hdr = True
    secondary_header = sec_hdr_bytes
    data_field = secondary_header + user_data
    data_length = len(data_field) - 1

    word0 = (0b000 << 13) | (0 << 12) | (int(sec_hdr) << 11) | (apid & 0x07FF)
    word1 = (PacketSequenceFlags.UNSEGMENTED << 14) | (seq_count & 0x3FFF)
    header_bytes = struct.pack(">HHH", word0, word1, data_length)
    return header_bytes + data_field


# --------------------------------------------------------------------------- #
#  Fixtures                                                                    #
# --------------------------------------------------------------------------- #


@pytest.fixture
def raw_packet_bytes() -> bytes:
    return make_raw_packet(apid=0x100, seq_count=42, user_data=b"\xDE\xAD\xBE\xEF")


@pytest.fixture
def telemetry_packet() -> TelemetryPacket:
    raw = make_raw_packet(apid=0x200, seq_count=7, user_data=b"\x01\x02\x03\x04")
    return TelemetryPacket.from_bytes(raw, sec_hdr_length=0, source_time_tai=12345.0)


@pytest.fixture
def sample_dataset(telemetry_packet: TelemetryPacket) -> TelemetryDataset:
    ds = TelemetryDataset()
    ds.add_packet(telemetry_packet)

    for i in range(5):
        p = EngineeringParameter(
            name="temperature",
            apid=0x200,
            seq_count=i,
            sample_time_tai=float(i * 10),
            raw_value=i * 100,
            eng_value=float(i * 10 - 273.15),
            unit="degC",
        )
        ds.add_parameter(p)

    return ds


@pytest.fixture
def binary_telemetry_file(tmp_path: Path) -> Path:
    """Write a binary file containing 10 CCSDS packets (no secondary header)."""
    outfile = tmp_path / "telemetry.bin"
    with open(outfile, "wb") as fh:
        for i in range(10):
            fh.write(make_raw_packet(
                apid=0x100, seq_count=i,
                user_data=struct.pack(">I", i * 1000),
                sec_hdr=False,
            ))
    return outfile
