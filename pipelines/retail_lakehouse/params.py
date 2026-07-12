# SDP bootstrap: runs ONCE at pipeline start. Calls
# retail_lakehouse.config.params.from_spark_conf(spark) — which resolves parameters
# AND runs the R1 catalog guard — so a mis-targeted deploy fails before any table updates.
#
# Import rule: transforms/ NEVER import this file (no cross-glob imports); everything
# they need comes from the retail_lakehouse wheel.
#
# TODO: invoke config.params.from_spark_conf at module level
