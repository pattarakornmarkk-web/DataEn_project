"""DQ rule engine: registry rules -> boolean flag columns (_is_* convention).

Rule types (unit spec §1.7): null_key, duplicate, domain, format, plausibility
(future-ts with allowed skew), try_cast. Op-aware exceptions supported (CDC deletes).
Validation is side-effect-free: clean rows pass through with values untouched.
"""

from pyspark.sql import DataFrame


def apply_rules(df: DataFrame, rules: list[dict]) -> DataFrame:
    """Apply all registry rules for a table; add one _is_<rule> flag column per rule."""
    raise NotImplementedError
