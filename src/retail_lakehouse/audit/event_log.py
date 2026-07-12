"""SDP event-log harvesting — the ONLY module allowed to know the event log schema."""

from pyspark.sql import DataFrame


def extract_expectation_metrics(event_log: DataFrame) -> DataFrame:
    """Per run x table x expectation: pass/fail counts."""
    raise NotImplementedError


def extract_flow_progress(event_log: DataFrame) -> DataFrame:
    """Per run x table: rows written, status, timing."""
    raise NotImplementedError
