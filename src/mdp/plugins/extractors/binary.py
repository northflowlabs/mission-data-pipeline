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
header or a truncated packet raises ``ValueError`` with its byte offset.

With ``frame_sync`` a frame runs from one marker to the next, and may hold several
packets. The extractor can recover from damage, so it moves on to the next marker and
counts the frame in ``skipped_frames``. The rules:

- Bytes before the first marker count in ``lead_in_bytes``. A file with no marker fails,
  an empty file too.
- A packet's length comes from its header. The bytes 0x1ACFFC1D can occur in a payload,
  so a marker inside a packet is only accepted as payload when the packet ends exactly
  at another marker or at the end of the file. Otherwise the length is taken to be
  wrong, and the frame is skipped. A payload marker in a packet followed by another
  packet of the same frame is therefore counted as a skipped frame: that is the cost of
  not reading past a real marker.
- After such a skip the bytes behind the crossed marker can be a real frame or the tail
  of a payload. A packet read there that ends within the skipped packet's stated length
  is not trusted, and the rest of that frame is skipped and counted too. This can cost
  a real frame its packets after a damaged length. The bytes alone cannot settle every
  case: a packet read from a payload tail that runs past the stated length, or that
  follows a packet cut off by the end of the file, is still emitted. Treat packets next
  to a skipped frame with care.
- A rest of a frame that is all 0x00 or all 0xFF is fill: it counts in ``fill_bytes``.
- Idle packets, APID 0x7FF, are dropped and counted in ``idle_packets``.
"""

from __future__ import annotations

import struct
from collections import Counter
from collections.abc import Iterator
from pathlib import Path
from typing import Annotated

from pydantic import BaseModel, Field

from mdp.core.base import Extractor
from mdp.core.registry import registry
from mdp.models.dataset import TelemetryDataset
from mdp.models.packet import CCSDSPrimaryHeader, TelemetryPacket

SYNC_MARKER = b"\x1a\xcf\xfc\x1d"
_IDLE_APID = 0x7FF
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
        counters: Counter[str] = Counter()

        offset = 0
        tail_end = 0  # where a skipped packet said it ended, if that is in the next frame
        if sync:
            first = data.find(SYNC_MARKER)
            if first < 0:
                raise ValueError(
                    f"No sync marker 0x{SYNC_MARKER.hex().upper()} in the {len(data)} bytes "
                    f"of {self.config.path}. Is frame_sync right for this file?"
                )
            counters["lead_in_bytes"] = first
            offset = first + len(SYNC_MARKER)

        while offset < len(data):
            if sync:
                if data.startswith(SYNC_MARKER, offset):
                    offset += len(SYNC_MARKER)
                    continue
                next_sync = data.find(SYNC_MARKER, offset + 1)
                frame_end = next_sync if next_sync >= 0 else len(data)
                rest = data[offset:frame_end]
                if not rest.strip(b"\x00") or not rest.strip(b"\xff"):
                    counters["fill_bytes"] += len(rest)
                    offset = frame_end
                    continue

            try:
                packet_bytes, header = self._read_packet(data, offset)
            except ValueError:
                if not sync:
                    raise
                counters["skipped_frames"] += 1
                offset = frame_end
                continue

            end = offset + len(packet_bytes)
            if sync and end <= tail_end:
                counters["skipped_frames"] += 1
                offset = frame_end
                continue
            if sync and end > frame_end and not self._ends_cleanly(data, end):
                counters["skipped_frames"] += 1
                next_marker = data.find(SYNC_MARKER, frame_end + len(SYNC_MARKER))
                tail_end = end if next_marker < 0 or end < next_marker else 0
                offset = frame_end
                continue

            offset = end

            if header.apid == _IDLE_APID and sync:
                counters["idle_packets"] += 1
                continue

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
                dataset.metadata.update(counters)
                yield dataset
                dataset = self._new_dataset()
                count = 0
                counters.clear()

        if dataset.packets or any(counters.values()):
            dataset.metadata.update(counters)
            yield dataset

    @staticmethod
    def _ends_cleanly(data: bytes, end: int) -> bool:
        """A packet that crosses a marker must end at another marker or the end of file."""
        return end == len(data) or data.startswith(SYNC_MARKER, end)

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
