"""Binary packet extractor: reads raw CCSDS packet streams from files or byte streams.

Supports:
  - Fixed-size packet files (flat binary dump)
  - VCDU/frame-stripped packet streams
  - Files with sync markers (0x1ACFFC1D)

Format assumptions
------------------
Unless ``frame_sync`` is enabled, the input is assumed to be a contiguous
stream of CCSDS Space Packets (no framing).  Each packet starts with a
6-byte primary header followed by ``header.packet_data_length`` bytes.

A stream that stops making sense is an error, not the end of the data: an invalid
header or a truncated packet raises ``ValueError`` with its byte offset. With
``frame_sync`` the extractor can recover, so it moves on to the next sync marker and
counts the frame in the batch's ``skipped_frames`` metadata.
"""

from __future__ import annotations

import io
import struct
import time
from collections.abc import Iterator
from pathlib import Path
from typing import Annotated

from pydantic import BaseModel, Field

from mdp.core.base import Extractor
from mdp.core.registry import registry
from mdp.models.dataset import TelemetryDataset
from mdp.models.packet import CCSDSPrimaryHeader, TelemetryPacket

SYNC_MARKER = b"\x1a\xcf\xfc\x1d"
CCSDS_HEADER_SIZE = 6


class BinaryExtractorConfig(BaseModel):
    path: Path = Field(description="Path to the binary telemetry file")
    batch_size: Annotated[int, Field(gt=0)] = 256
    apid_filter: list[int] | None = Field(
        default=None,
        description="If set, only yield packets whose APID is in this list",
    )
    sec_hdr_length: Annotated[int, Field(ge=0)] = Field(
        default=0,
        description=(
            "Secondary header length in bytes. It is mission-specific: "
            "CCSDS 133.0-B-2 does not fix its format at the Space Packet layer."
        ),
    )
    frame_sync: bool = Field(
        default=False,
        description="If True, scan for 0x1ACFFC1D sync markers before each frame",
    )
    source_id: str | None = None
    ground_receipt_time: float | None = None


@registry.extractor("binary")
class BinaryPacketExtractor(Extractor[BinaryExtractorConfig]):
    """Stream CCSDS packets from a raw binary file in configurable-size batches."""

    config_class = BinaryExtractorConfig

    def extract(self) -> Iterator[TelemetryDataset]:
        path = self.config.path
        if not path.exists():
            raise FileNotFoundError(f"Telemetry file not found: {path}")

        grt = self.config.ground_receipt_time or time.time()

        with open(path, "rb") as fh:
            buffer = io.BytesIO(fh.read())

        yield from self._parse_buffer(buffer, grt)

    def _parse_buffer(
        self, buffer: io.BytesIO, ground_receipt_time: float
    ) -> Iterator[TelemetryDataset]:
        dataset = self._new_dataset()
        count = 0
        skipped_frames = 0

        while True:
            if self.config.frame_sync:
                if not self._seek_to_sync(buffer):
                    break
                buffer.read(4)

            offset = buffer.tell()
            header_bytes = buffer.read(CCSDS_HEADER_SIZE)
            if not header_bytes:
                break

            try:
                header = CCSDSPrimaryHeader.from_bytes(header_bytes)
            except (struct.error, ValueError) as exc:
                if self.config.frame_sync:
                    skipped_frames += 1
                    continue
                raise ValueError(
                    f"Invalid CCSDS header at byte {offset} of {self.config.path}: "
                    f"{header_bytes.hex()}"
                ) from exc

            data_field = buffer.read(header.packet_data_length)
            if len(data_field) < header.packet_data_length:
                if self.config.frame_sync:
                    skipped_frames += 1
                    continue
                raise ValueError(
                    f"Truncated packet at byte {offset} of {self.config.path}: "
                    f"expected {header.packet_data_length} data bytes, got {len(data_field)}"
                )

            if self.config.apid_filter and header.apid not in self.config.apid_filter:
                continue

            packet = TelemetryPacket.from_bytes(
                header_bytes + data_field,
                sec_hdr_length=self.config.sec_hdr_length,
                ground_receipt_time=ground_receipt_time,
                source_id=self.config.source_id,
            )
            dataset.add_packet(packet)
            count += 1

            if count >= self.config.batch_size:
                dataset.metadata["skipped_frames"] = skipped_frames
                yield dataset
                dataset = self._new_dataset()
                count = 0
                skipped_frames = 0

        if dataset.packets or skipped_frames:
            dataset.metadata["skipped_frames"] = skipped_frames
            yield dataset

    def _new_dataset(self) -> TelemetryDataset:
        return TelemetryDataset(metadata={"source": str(self.config.path), "extractor": "binary"})

    @staticmethod
    def _seek_to_sync(buffer: io.BytesIO) -> bool:
        """Advance the buffer position to the next 0x1ACFFC1D sync marker."""
        while True:
            byte = buffer.read(1)
            if not byte:
                return False
            if byte == b"\x1a":
                candidate = byte + buffer.read(3)
                if candidate == SYNC_MARKER:
                    buffer.seek(-4, io.SEEK_CUR)
                    return True
