"""Decommutation transformer: extracts named parameters from packet user_data.

Each ParameterDefinition describes the bit-level location of a parameter
within a specific APID's user data field.  This mirrors the concept of an
MIB (Mission Information Base) packet definition in ESA/NASA ground systems.
"""

from __future__ import annotations

import math
import struct
from typing import Annotated

from pydantic import BaseModel, Field, model_validator

from mdp.core.base import Transformer
from mdp.core.registry import registry
from mdp.models.dataset import TelemetryDataset
from mdp.models.packet import TelemetryPacket
from mdp.models.parameter import EngineeringParameter, ParameterType, RawValue

_STRUCT_FMTS: dict[tuple[str, int], str] = {
    (ParameterType.UINT.value, 8): ">B",
    (ParameterType.UINT.value, 16): ">H",
    (ParameterType.UINT.value, 32): ">I",
    (ParameterType.UINT.value, 64): ">Q",
    (ParameterType.INT.value, 8): ">b",
    (ParameterType.INT.value, 16): ">h",
    (ParameterType.INT.value, 32): ">i",
    (ParameterType.INT.value, 64): ">q",
    (ParameterType.FLOAT.value, 32): ">f",
    (ParameterType.DOUBLE.value, 64): ">d",
}


class ParameterDefinition(BaseModel):
    """Describes how to extract a single parameter from a packet's user_data field."""

    model_config = {"frozen": True}

    name: str
    apid: int
    byte_offset: Annotated[int, Field(ge=0)]
    bit_length: Annotated[int, Field(gt=0)]
    param_type: ParameterType
    bit_offset: Annotated[int, Field(ge=0, le=7)] = Field(
        default=0,
        description="First bit within byte_offset, counted from the most significant bit",
    )
    unit: str | None = None
    little_endian: bool = False
    description: str | None = None

    @model_validator(mode="after")
    def _check_layout(self) -> ParameterDefinition:
        byte_aligned = self.bit_offset == 0 and self.bit_length % 8 == 0
        if self.param_type in (ParameterType.UINT, ParameterType.INT, ParameterType.BOOLEAN):
            if self.little_endian and not byte_aligned:
                raise ValueError(f"{self.name}: a little-endian field must be byte-aligned")
            return self
        if not byte_aligned:
            raise ValueError(f"{self.name}: a {self.param_type.value} field must be byte-aligned")
        return self


class DecomConfig(BaseModel):
    parameters: list[ParameterDefinition]
    skip_unknown_apids: bool = True


@registry.transformer("decom")
class DecomTransformer(Transformer[DecomConfig]):
    """Extract named parameters from packet user_data using a parameter definition table."""

    config_class = DecomConfig

    def setup(self) -> None:
        self._apid_map: dict[int, list[ParameterDefinition]] = {}
        for pdef in self.config.parameters:
            self._apid_map.setdefault(pdef.apid, []).append(pdef)

    def transform(self, dataset: TelemetryDataset) -> TelemetryDataset:
        for packet in dataset.iter_packets():
            defs = self._apid_map.get(packet.apid)
            if defs is None:
                if not self.config.skip_unknown_apids:
                    raise KeyError(f"No parameter definitions for APID 0x{packet.apid:04X}")
                continue
            for pdef in defs:
                param = self._extract_parameter(packet, pdef)
                if param is not None:
                    dataset.add_parameter(param)
        return dataset

    def _extract_parameter(
        self, packet: TelemetryPacket, pdef: ParameterDefinition
    ) -> EngineeringParameter | None:
        data = packet.user_data
        byte_start = pdef.byte_offset
        byte_count = (pdef.bit_offset + pdef.bit_length + 7) // 8

        if byte_start + byte_count > len(data):
            return None

        raw_bytes = data[byte_start : byte_start + byte_count]
        raw_value = self._decode_bytes(raw_bytes, pdef)

        tai = _extract_tai(packet)

        return EngineeringParameter(
            name=pdef.name,
            apid=packet.apid,
            seq_count=packet.seq_count,
            sample_time_tai=tai,
            raw_value=raw_value,
            eng_value=raw_value if not isinstance(raw_value, bytes) else raw_value.hex(),
            unit=pdef.unit,
        )

    def _decode_bytes(self, raw_bytes: bytes, pdef: ParameterDefinition) -> RawValue:
        endian = "<" if pdef.little_endian else ">"
        fmt_key = (pdef.param_type.value, pdef.bit_length)

        if pdef.bit_offset == 0 and fmt_key in _STRUCT_FMTS:
            fmt = _STRUCT_FMTS[fmt_key].replace(">", endian)
            (value,) = struct.unpack(fmt, raw_bytes)
            result: RawValue = value
            return result

        if pdef.param_type == ParameterType.STRING:
            return raw_bytes.decode("ascii", errors="replace").rstrip("\x00")

        if pdef.param_type == ParameterType.BINARY:
            return raw_bytes

        return _decode_bits(raw_bytes, pdef)


def _decode_bits(raw_bytes: bytes, pdef: ParameterDefinition) -> int | bool:
    """Read an integer field of any length at any bit position."""
    if pdef.little_endian:
        value = int.from_bytes(raw_bytes, byteorder="little")
    else:
        spare_bits = len(raw_bytes) * 8 - pdef.bit_offset - pdef.bit_length
        value = int.from_bytes(raw_bytes, byteorder="big") >> spare_bits
        value &= (1 << pdef.bit_length) - 1

    if pdef.param_type == ParameterType.BOOLEAN:
        return value != 0
    if pdef.param_type == ParameterType.INT and value >= 1 << (pdef.bit_length - 1):
        value -= 1 << pdef.bit_length
    return value


def _extract_tai(packet: TelemetryPacket) -> float:
    """Return the packet's time, or NaN when no transformer has set one.

    MDP does not know a mission's secondary header format. Set ``source_time_tai`` in
    a transformer ahead of decom; ``seq_count`` stays available for ordering.
    """
    if packet.source_time_tai is not None:
        return packet.source_time_tai
    return math.nan
