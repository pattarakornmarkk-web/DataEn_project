"""Retail Lakehouse library — all business logic lives here (pytest-covered).

Rule: pure functions, DataFrame in -> DataFrame out. No spark.table(), no dbutils,
no globals inside library code. SDP declarations in transforms/ call these functions.
"""

__version__ = "0.1.0"
