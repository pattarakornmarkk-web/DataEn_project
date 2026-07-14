"""Contract type vocabulary -> Spark types. The ONLY place that knows this mapping."""

from __future__ import annotations

import re

from pyspark.sql import types as T

from retail_lakehouse.config.contracts import ConfigError

ROW_ID = "_row_id"  # differential row identity (approved adapter strategy)

_DECIMAL_RE = re.compile(r"^decimal\((\d+),(\d+)\)$")

_SCALAR = {
    "string": T.StringType(),
    "int": T.IntegerType(),
    "bigint": T.LongType(),
    "double": T.DoubleType(),
    "date": T.DateType(),
    "timestamp": T.TimestampType(),
    "boolean": T.BooleanType(),
}


def spark_type(ctype: str) -> T.DataType:
    if ctype in _SCALAR:
        return _SCALAR[ctype]
    match = _DECIMAL_RE.match(ctype)
    if match:
        return T.DecimalType(int(match[1]), int(match[2]))
    raise ConfigError(f"unknown contract type {ctype!r}")


def typed_schema(column_types: dict[str, str], include_row_id: bool = True) -> T.StructType:
    """Schema for contract-coerced data. Always explicit — never infer."""
    fields = [T.StructField(col, spark_type(ctype), True) for col, ctype in column_types.items()]
    if include_row_id:
        fields.insert(0, T.StructField(ROW_ID, T.LongType(), False))
    return T.StructType(fields)


def raw_schema(columns, include_row_id: bool = True) -> T.StructType:
    """Schema for landed (bronze-like) data: every business column is a string."""
    fields = [T.StructField(col, T.StringType(), True) for col in columns]
    if include_row_id:
        fields.insert(0, T.StructField(ROW_ID, T.LongType(), False))
    return T.StructType(fields)
