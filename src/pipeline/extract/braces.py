"""Brace-matching JSON extractor for server-rendered data blobs (__SERVER_DATA__ etc.).

A lazy regex truncates Eventbrite's ~391 KB blob — this walks balanced braces,
respecting string literals and escapes.
"""

from __future__ import annotations

import json


def extract_json_object(text: str, marker: str) -> dict | None:
    """Return the JSON object whose opening brace follows `marker` in `text`."""
    idx = text.find(marker)
    if idx == -1:
        return None
    start = text.find("{", idx + len(marker))
    if start == -1:
        return None
    depth = 0
    in_string = False
    escaped = False
    for pos in range(start, len(text)):
        ch = text[pos]
        if in_string:
            if escaped:
                escaped = False
            elif ch == "\\":
                escaped = True
            elif ch == '"':
                in_string = False
            continue
        if ch == '"':
            in_string = True
        elif ch == "{":
            depth += 1
        elif ch == "}":
            depth -= 1
            if depth == 0:
                try:
                    return json.loads(text[start : pos + 1])
                except json.JSONDecodeError:
                    return None
    return None
