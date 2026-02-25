"""CCSDS-style telemetry packet models.

Reference: CCSDS 133.0-B-2 (Space Packet Protocol)
"""

from __future__ import annotations

import struct
from enum import IntEnum
from typing import Annotated

from pydantic import BaseModel, Field, field_validator, model_validator


class PacketSequenceFlags(IntEnum):
    """CCSDS sequence flags (2 bits)."""

    CONTINUATION = 0b00
    FIRST_SEGMENT = 0b01
    LAST_SEGMENT = 0b10
    UNSEGMENTED = 0b11


class ApidCategory(IntEnum):
    """Application Process Identifier category conventions."""

    HOUSEKEEPING = 0x001
    SCIENCE = 0x100
    EVENT = 0x200
    MEMORY_DUMP = 0x300
    COMMAND_VERIFICATION = 0x400


class CCSDSPrimaryHeader(BaseModel):
    """6-byte CCSDS Space Packet primary header.

    Bit layout (48 bits total):
        [3]  version    — always 0b000
        [1]  type       — 0=telemetry, 1=telecommand
        [1]  sec_hdr    — secondary header present flag
        [11] apid       — Application Process Identifier
        [2]  seq_flags  — sequence flags
        [14] seq_count  — packet sequence count (0–16383)
        [16] data_len   — data field length minus 1 (octets)
    """

    model_config = {"frozen": True}

    version: Annotated[int, Field(ge=0, le=7, default=0)]
    type_flag: Annotated[int, Field(ge=0, le=1, default=0)]
    sec_hdr_flag: Annotated[int, Field(ge=0, le=1, default=1)]
    apid: Annotated[int, Field(ge=0, le=0x7FF)]
    seq_flags: PacketSequenceFlags = PacketSequenceFlags.UNSEGMENTED
    seq_count: Annotated[int, Field(ge=0, le=0x3FFF)]
    data_length: Annotated[int, Field(ge=0, le=0xFFFF)]

    @classmethod
    def from_bytes(cls, raw: bytes) -> CCSDSPrimaryHeader:
        """Deserialise from exactly 6 raw bytes."""
        if len(raw) < 6:
            raise ValueError(f"Primary header requires 6 bytes, got {len(raw)}")
        word0, word1, word2 = struct.unpack(">HHH", raw[:6])
        return cls(
            version=(word0 >> 13) & 0x07,
            type_flag=(word0 >> 12) & 0x01,
            sec_hdr_flag=(word0 >> 11) & 0x01,
            apid=word0 & 0x07FF,
            seq_flags=PacketSequenceFlags((word1 >> 14) & 0x03),
            seq_count=word1 & 0x3FFF,
            data_length=word2,
        )

    def to_bytes(self) -> bytes:
        """Serialise back to 6 bytes."""
        word0 = (self.version << 13) | (self.type_flag << 12) | (self.sec_hdr_flag << 11) | self.apid
        word1 = (self.seq_flags << 14) | self.seq_count
        return struct.pack(">HHH", word0, word1, self.data_length)

    @property
    def packet_data_length(self) -> int:
        """Actual data field length in bytes (data_length + 1)."""
        return self.data_length + 1

    @property
    def total_length(self) -> int:
        """Total packet length in bytes (header + data field)."""
        return 6 + self.packet_data_length


class TelemetryPacket(BaseModel):
    """A fully parsed CCSDS telemetry packet with metadata."""

    model_config = {"frozen": True, "arbitrary_types_allowed": True}

    header: CCSDSPrimaryHeader
    secondary_header: bytes = Field(default=b"", repr=False)
    user_data: bytes = Field(repr=False)
    source_time_tai: float | None = Field(
        default=None,
        description="Source packet time in TAI seconds since J2000",
    )
    ground_receipt_time: float | None = Field(
        default=None,
        description="Ground station receipt time (UNIX epoch)",
    )
    source_id: str | None = Field(
        default=None,
        description="Originating spacecraft or ground station identifier",
    )

    @field_validator("secondary_header", "user_data", mode="before")
    @classmethod
    def _coerce_bytes(cls, v: object) -> bytes:
        if isinstance(v, (bytearray, memoryview)):
            return bytes(v)
        if isinstance(v, bytes):
            return v
        raise ValueError(f"Expected bytes-like, got {type(v)}")

    @model_validator(mode="after")
    def _validate_data_length(self) -> TelemetryPacket:
        expected = self.header.packet_data_length
        actual = len(self.secondary_header) + len(self.user_data)
        if actual != expected:
            raise ValueError(
                f"Packet data field mismatch: header declares {expected} bytes, "
                f"secondary_header+user_data is {actual} bytes"
            )
        return self

    @classmethod
    def from_bytes(
        cls,
        raw: bytes,
        sec_hdr_length: int = 0,
        **metadata: object,
    ) -> TelemetryPacket:
        """Parse a raw byte buffer into a TelemetryPacket.

        Parameters
        ----------
        raw:
            Raw bytes starting at the first byte of the primary header.
        sec_hdr_length:
            Length of the secondary header in bytes.  This is mission-specific
            and must be provided by the caller when ``sec_hdr_flag`` is set.
            Per CCSDS 133.0-B-2, the secondary header format is not standardised
            at the Space Packet layer — common values are 4 (CDS short) or
            10 (CUC with fine time).  Defaults to 0 (no secondary header parsed).
        """
        header = CCSDSPrimaryHeader.from_bytes(raw[:6])
        data_field = raw[6 : 6 + header.packet_data_length]
        if header.sec_hdr_flag and sec_hdr_length > 0:
            sec_hdr = data_field[:sec_hdr_length]
        else:
            sec_hdr = b""
        user_data = data_field[len(sec_hdr):]
        return cls(
            header=header,
            secondary_header=sec_hdr,
            user_data=user_data,
            **metadata,
        )

    @property
    def apid(self) -> int:
        """Shortcut to the Application Process Identifier."""
        return self.header.apid

    @property
    def seq_count(self) -> int:
        """Shortcut to the packet sequence count."""
        return self.header.seq_count
