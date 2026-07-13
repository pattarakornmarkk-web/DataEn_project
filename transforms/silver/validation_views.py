# SILVER | _v_<source>_validated x6 — temp views applying the registry rule engine.
# Generated from conf/dq_rules.yml. Op-aware exception for customer_updates deletes;
# customer_activity runs observe-mode. Every downstream consumer filters on `reason`.
#
# TODO: @dp.view declarations via the Spark rule compiler (next slice) — must produce
#       verdicts identical to dq.engine.validate_batch (the pure oracle)
