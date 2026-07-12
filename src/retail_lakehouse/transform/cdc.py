"""CDC stream preparation (unit spec §1.3, silver _v_customer_cdc_unified).

- snapshot_to_cdc: full-snapshot rows -> synthetic op=U events sequenced by updated_at
- unify: both branches must produce IDENTICAL schemas
- op-aware validation: delete images exempt from non-key null rules;
  null sequence quarantines regardless of op; orphan deletes tagged, held, never dropped (ADR)
"""

from pyspark.sql import DataFrame


def snapshot_to_cdc(snapshot: DataFrame, sequence_col: str) -> DataFrame:
    raise NotImplementedError


def unify_cdc_streams(cdc_events: DataFrame, synthesized: DataFrame) -> DataFrame:
    raise NotImplementedError


def tag_orphan_deletes(events: DataFrame, keys: list[str]) -> DataFrame:
    raise NotImplementedError
