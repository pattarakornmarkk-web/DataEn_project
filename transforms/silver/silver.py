# SILVER — validated/quarantine MVs (DQ compiler), SCD2 history dims (CDC + SCD2
# compilers), held-orphans table (ADR-0006). Registration only; logic in the wheel.
from pyspark import pipelines as dp

from retail_lakehouse.spark import declarations

declarations.register_silver(dp)
