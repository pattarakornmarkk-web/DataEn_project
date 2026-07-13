"""Batch-metadata parsing from landing filenames.

Contract (unit spec §1.5): the parser is TOTAL — any input string yields a BatchMeta,
malformed names get is_malformed=True with None fields, never an exception.
Convention: <source>_<YYYYMMDD>_<HHMMSS>_<seq>.<ext>
"""

from __future__ import annotations

import re
from dataclasses import dataclass
from datetime import datetime, timezone

_FILENAME_RE = re.compile(
    r"^(?P<source>[a-z][a-z0-9_]*)_(?P<date>\d{8})_(?P<time>\d{6})_(?P<seq>\d{1,4})"
    r"\.(?P<ext>[a-z0-9]+)$"
)


@dataclass(frozen=True)
class BatchMeta:
    raw_name: str
    source: str | None
    batch_ts: datetime | None  # UTC
    batch_seq: int | None
    extension: str | None
    is_malformed: bool


def parse_batch_metadata(file_name: str) -> BatchMeta:
    """Total parser: never raises, whatever the input."""
    malformed = BatchMeta(str(file_name), None, None, None, None, True)
    match = _FILENAME_RE.match(str(file_name))
    if not match:
        return malformed
    try:
        batch_ts = datetime.strptime(match["date"] + match["time"], "%Y%m%d%H%M%S").replace(
            tzinfo=timezone.utc
        )
    except ValueError:  # e.g. month 13 — pattern-valid but calendar-impossible
        return malformed
    return BatchMeta(
        raw_name=str(file_name),
        source=match["source"],
        batch_ts=batch_ts,
        batch_seq=int(match["seq"]),
        extension=match["ext"],
        is_malformed=False,
    )
