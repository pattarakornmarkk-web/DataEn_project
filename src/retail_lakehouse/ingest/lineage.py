"""Standard lineage fields for all bronze records.

Pure builder here; the Spark wrapper (reading _metadata.*) lands with the transforms
slice and must emit exactly these field names. Blueprint ref: Bronze Design §4.
"""

from __future__ import annotations

from datetime import datetime

from retail_lakehouse.ingest.filenames import parse_batch_metadata

LINEAGE_FIELDS = (
    "_ingest_ts",
    "_source_file",
    "_file_path",
    "_file_size",
    "_file_mod_ts",
    "_batch_date",
)


def build_lineage(
    file_name: str,
    file_path: str,
    file_size: int,
    modified_ts: datetime,
    ingest_ts: datetime,
) -> dict:
    """File facts + clock -> the standard lineage field set (dict, ordered per LINEAGE_FIELDS)."""
    meta = parse_batch_metadata(file_name)
    return {
        "_ingest_ts": ingest_ts,
        "_source_file": file_name,
        "_file_path": file_path,
        "_file_size": file_size,
        "_file_mod_ts": modified_ts,
        "_batch_date": meta.batch_ts.date() if meta.batch_ts else None,
    }


def with_lineage_columns(df):
    """Spark wrapper — implemented in the transforms slice; must mirror build_lineage."""
    raise NotImplementedError
