"""Sample times: TAI seconds since J2000.0.

J2000.0 is 2000-01-01T12:00:00 TT. TT runs 32.184 s ahead of TAI, so the epoch is
2000-01-01T11:59:27.816 TAI.
"""

from __future__ import annotations

from datetime import datetime

# Seconds from 2000-01-01T12:00:00 on the TAI scale back to J2000.0
_TT_MINUS_TAI_S = 32.184


def utc_to_tai_j2000(utc: datetime, tai_minus_utc_s: float) -> float:
    """Convert a naive UTC time to TAI seconds since J2000.0.

    ``tai_minus_utc_s`` is the leap second offset in force at *utc*: 37 s since 2017.
    """
    noon_on_utc_clock = datetime(2000, 1, 1, 12)
    return (utc - noon_on_utc_clock).total_seconds() + tai_minus_utc_s + _TT_MINUS_TAI_S
