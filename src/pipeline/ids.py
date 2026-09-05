"""Identifiers. Event and occurrence ids are derived, not minted.

An event id is the one value this pipeline publishes that anything downstream
holds onto — a saved event, a bookmark, a share link. It used to be a ULID
generated at insert time, which made it a property of *when a row was first
written* rather than of the event itself. That put the id's stability entirely in
the hands of `data/pipeline.db`: rebuild from an empty database and every event
comes back under a new id, orphaning every reference to it.

Deriving the id from the source's own identity removes the problem instead of
guarding against it. The same listing yields the same id on any machine, from any
starting state, forever — so the database is a cache again, and losing it costs a
slow run rather than a re-keyed catalogue.

The null byte matters: without it, ("ab", "c") and ("a", "bc") would collide.

Crockford base32, 26 characters — the same shape the ULIDs had, so nothing
downstream has to care that the derivation changed. Venue ids stay random; they
are internal and never published.
"""

import hashlib
import os
import time

_B32 = "0123456789ABCDEFGHJKMNPQRSTVWXYZ"

# 26 base32 characters carry 130 bits; 128 of them come from the digest. At
# Berlin's ~20k events a birthday collision sits far below any rate that matters.
_ID_CHARS = 26
_DIGEST_BYTES = 16


def _encode(value: int, length: int) -> str:
    out = []
    for _ in range(length):
        out.append(_B32[value & 0x1F])
        value >>= 5
    return "".join(reversed(out))


def _derive(prefix: str, *parts: str) -> str:
    payload = "\x00".join(parts).encode()
    digest = hashlib.sha256(payload).digest()[:_DIGEST_BYTES]
    return f"{prefix}_{_encode(int.from_bytes(digest, 'big'), _ID_CHARS)}"


def ulid() -> str:
    """Time-sortable random id. Only venues still need one."""
    ts = int(time.time() * 1000)
    rand = int.from_bytes(os.urandom(10), "big")
    return _encode(ts, 10) + _encode(rand, 16)


def event_id(source: str, source_event_id: str) -> str:
    """Stable for a listing across rebuilds, machines and wipes."""
    return _derive("evt", source, source_event_id)


def occurrence_id(event: str, starts_at_utc: str) -> str:
    """Surrogate key for an occurrence row.

    Never read back — `(event_id, starts_at_utc)` is what the table conflicts on
    and what the export addresses — but deriving it keeps a rebuilt database
    byte-identical to the one it replaced.
    """
    return _derive("occ", event, starts_at_utc)


def venue_id() -> str:
    return f"ven_{ulid()}"
