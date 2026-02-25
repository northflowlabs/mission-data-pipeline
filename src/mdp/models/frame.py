"""Telemetry transfer frame model.

Reference: CCSDS 132.0-B-3 (TM Space Data Link Protocol)
"""

from __future__ import annotations

import struct
from enum import IntEnum
from typing import Annotated

from pydantic import BaseModel, Field, field_validator


class FrameQuality(IntEnum):
    """Decoded frame quality indicator."""

    GOOD = 0
    DEGRADED = 1
    BAD = 2
    MISSING = 3


class TMFramePrimaryHeader(BaseModel):
    """6-byte TM Transfer Frame primary header (CCSDS 132.0-B-3)."""

    model_config = {"frozen": True}

    version: Annotated[int, Field(ge=0, le=3, default=0)]
    spacecraft_id: Annotated[int, Field(ge=0, le=0x3FF)]
    virtual_channel_id: Annotated[int, Field(ge=0, le=0x07)]
    ocf_flag: Annotated[int, Field(ge=0, le=1, default=0)]
    master_channel_frame_count: Annotated[int, Field(ge=0, le=0xFF)]
    virtual_channel_frame_count: Annotated[int, Field(ge=0, le=0xFF)]
    secondary_header_flag: Annotated[int, Field(ge=0, le=1, default=0)]
    sync_flag: Annotated[int, Field(ge=0, le=1, default=0)]
    packet_order_flag: Annotated[int, Field(ge=0, le=1, default=0)]
    segment_length_id: Annotated[int, Field(ge=0, le=3, default=3)]
    first_header_pointer: Annotated[int, Field(ge=0, le=0x7FF)]

    @classmethod
    def from_bytes(cls, raw: bytes) -> TMFramePrimaryHeader:
        if len(raw) < 6:
            raise ValueError(f"Frame primary header requires 6 bytes, got {len(raw)}")
        w0, w1, w2 = struct.unpack(">HBB", raw[:4])
        w3 = struct.unpack(">H", raw[4:6])[0]
        return cls(
            version=(w0 >> 14) & 0x03,
            spacecraft_id=(w0 >> 4) & 0x3FF,
            virtual_channel_id=(w0 >> 1) & 0x07,
            ocf_flag=w0 & 0x01,
            master_channel_frame_count=w1,
            virtual_channel_frame_count=w2,
            secondary_header_flag=(w3 >> 15) & 0x01,
            sync_flag=(w3 >> 14) & 0x01,
            packet_order_flag=(w3 >> 13) & 0x01,
            segment_length_id=(w3 >> 11) & 0x03,
            first_header_pointer=w3 & 0x07FF,
        )


class TelemetryFrame(BaseModel):
    """A decoded TM transfer frame containing one or more packets."""

    model_config = {"frozen": True, "arbitrary_types_allowed": True}

    header: TMFramePrimaryHeader
    data_field: bytes = Field(repr=False)
    quality: FrameQuality = FrameQuality.GOOD
    ground_receipt_time: float | None = None
    ground_station_id: str | None = None
    bit_error_rate: float | None = Field(
        default=None,
        ge=0.0,
        le=1.0,
        description="Bit error rate measured at ground station",
    )

    @field_validator("data_field", mode="before")
    @classmethod
    def _coerce_bytes(cls, v: object) -> bytes:
        if isinstance(v, (bytearray, memoryview)):
            return bytes(v)
        if isinstance(v, bytes):
            return v
        raise ValueError(f"Expected bytes-like, got {type(v)}")

    @property
    def spacecraft_id(self) -> int:
        return self.header.spacecraft_id

    @property
    def virtual_channel_id(self) -> int:
        return self.header.virtual_channel_id

    @property
    def frame_count(self) -> int:
        return self.header.virtual_channel_frame_count

    @property
    def is_good(self) -> bool:
        return self.quality == FrameQuality.GOOD
