# GOLD — current dims, fct_sales (oracle-in-executor revenue + FX + as-of product
# join), daily aggregates, funnel, dq_reconciliation. Registration only.
from pyspark import pipelines as dp

from retail_lakehouse.spark import declarations

declarations.register_gold(dp)
