"""Batch-metadata parsing from landing filenames.

Contract (unit spec §1.5): the parser is TOTAL — any input string yields a result,
malformed names get a sentinel + flag, never an exception.
Pattern: <source>_<YYYYMMDD_HHMMSS>_<batch_seq>.<ext>
"""


def parse_batch_metadata(file_name: str) -> dict:
    """Return {batch_date, batch_seq, is_malformed} for any filename."""
    raise NotImplementedError
