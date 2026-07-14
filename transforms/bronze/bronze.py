# BRONZE — Auto Loader streaming tables for all six sources (multi-emitter flows
# included). All logic lives in the wheel; this file only registers.
from pyspark import pipelines as dp

from retail_lakehouse.spark import declarations

declarations.register_bronze(dp)
