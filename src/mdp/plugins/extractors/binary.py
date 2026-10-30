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
counts the frame in the batch's ``skipped_frames`` metadata. A frame may hold several
packets: they are read until the next marker or the end of the file.
"""

from __future__ import annotations

import struct
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
        min_length=1,
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

        with open(path, "rb") as fh:
            data = fh.read()

        yield from self._parse(data)

    def _parse(self, data: bytes) -> Iterator[TelemetryDataset]:
        sync = self.config.frame_sync
        dataset = self._new_dataset()
        count = 0
        skipped_frames = 0

        offset = 0
        if sync:
            first = data.find(SYNC_MARKER)
            offset = first + len(SYNC_MARKER) if first >= 0 else len(data)

        while offset < len(data):
            if sync and data.startswith(SYNC_MARKER, offset):
                offset += len(SYNC_MARKER)
                continue

            try:
                packet_bytes, header = self._read_packet(data, offset)
            except ValueError:
                if not sync:
                    raise
                skipped_frames += 1
                next_sync = data.find(SYNC_MARKER, offset + 1)
                if next_sync < 0:
                    break
                offset = next_sync + len(SYNC_MARKER)
                continue

            offset += len(packet_bytes)

            if self.config.apid_filter and header.apid not in self.config.apid_filter:
                continue

            packet = TelemetryPacket.from_bytes(
                packet_bytes,
                sec_hdr_length=self.config.sec_hdr_length,
                ground_receipt_time=self.config.ground_receipt_time,
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

    def _read_packet(self, data: bytes, offset: int) -> tuple[bytes, CCSDSPrimaryHeader]:
        header_bytes = data[offset : offset + CCSDS_HEADER_SIZE]
        try:
            header = CCSDSPrimaryHeader.from_bytes(header_bytes)
        except (struct.error, ValueError) as exc:
            raise ValueError(
                f"Invalid CCSDS header at byte {offset} of {self.config.path}: {header_bytes.hex()}"
            ) from exc

        end = offset + CCSDS_HEADER_SIZE + header.packet_data_length
        if end > len(data):
            raise ValueError(
                f"Truncated packet at byte {offset} of {self.config.path}: expected "
                f"{header.packet_data_length} data bytes, "
                f"got {len(data) - offset - CCSDS_HEADER_SIZE}"
            )
        return data[offset:end], header

    def _new_dataset(self) -> TelemetryDataset:
        return TelemetryDataset(metadata={"source": str(self.config.path), "extractor": "binary"})
