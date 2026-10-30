# Data models

All models live in `mdp.models`.

## CCSDSPrimaryHeader

The 6-byte CCSDS Space Packet primary header, per CCSDS 133.0-B-2 section 4.1.

| Bits | Field | Range |
|------|-------|-------|
| 3 | `version` | 0 (fixed) |
| 1 | `type_flag` | 0 = telemetry (reporting), 1 = telecommand (requesting) |
| 1 | `sec_hdr_flag` | secondary header present |
| 11 | `apid` | 0x000 to 0x7FF |
| 2 | `seq_flags` | see `PacketSequenceFlags` |
| 14 | `seq_count` | 0 to 16383, wraps |
| 16 | `data_length` | data field length minus 1 |

`from_bytes()` and `to_bytes()` round-trip the header and validate every field.

## TelemetryPacket

| Field | Type | Description |
|---|---|---|
| `header` | `CCSDSPrimaryHeader` | Parsed primary header |
| `secondary_header` | `bytes` | Raw secondary header, length is mission specific |
| `user_data` | `bytes` | Application data field |
| `source_time_tai` | `float \| None` | TAI seconds since J2000.0 (2000-01-01T12:00:00 TT, which is 11:59:27.816 TAI) |
| `ground_receipt_time` | `float \| None` | Ground receipt time, UNIX epoch |
| `source_id` | `str \| None` | Ground station or spacecraft identifier |

`TelemetryPacket.from_bytes(raw, sec_hdr_length=N)` takes the secondary header length as an argument.

## TelemetryFrame

`TelemetryFrame` and `TMFramePrimaryHeader` model TM transfer frames per CCSDS 132.0-B-3. `FrameQuality` records frame quality flags.

## Parameters

- `ParameterType`: the raw type of a decommutated value, for example `UINT`
- `RawParameter`: a value as extracted from a packet
- `EngineeringParameter`: a value in engineering units, after calibration
- `ParameterRecord`: the time series of one parameter, with `count` and `time_range`

## TelemetryDataset

The batch container passed between stages.

```python
dataset.packets                    # list[TelemetryPacket]
dataset.parameters                 # dict[str, ParameterRecord]
dataset.add_packet(packet)
dataset.packets_by_apid(0x100)     # packets for one APID
dataset.add_parameter(param)
dataset.get_parameter("obc_temp")  # ParameterRecord or None
dataset.parameter_names()
dataset.to_dataframe("obc_temp")   # tidy pandas DataFrame, sorted by TAI
dataset.to_wide_dataframe()        # wide pandas DataFrame, indexed by TAI
dataset.merge(other)               # new dataset, inputs unchanged
len(dataset)                       # number of packets
```

## Standards

| Standard | Title | Scope in MDP |
|---|---|---|
| CCSDS 133.0-B-2 | Space Packet Protocol | `CCSDSPrimaryHeader`, `TelemetryPacket` |
| CCSDS 132.0-B-3 | TM Space Data Link Protocol | `TMFramePrimaryHeader`, `TelemetryFrame` |
| ECSS-E-ST-70-41C | TM/TC Packet Utilization | Decommutation model, MIB conventions |
