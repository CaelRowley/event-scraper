"""ULID generation (Crockford base32, time-sortable)."""

import os
import time

_B32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"


def _encode(value: int, length: int) -> str:
    out = []
    for _ in range(length):
        out.append(_B32[value & 0x1F])
        value >>= 5
    return "".join(reversed(out))


def ulid() -> str:
    ts = int(time.time() * 1000)
    rand = int.from_bytes(os.urandom(10), "big")
    return _encode(ts, 10) + _encode(rand, 16)


def event_id() -> str:
    return f"evt_{ulid()}"


def venue_id() -> str:
    return f"ven_{ulid()}"
