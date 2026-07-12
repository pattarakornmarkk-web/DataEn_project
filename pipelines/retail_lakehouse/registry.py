# RESOLVED (ADR-0008): registries are read from PACKAGED wheel data via
# retail_lakehouse.config.loader — never from workspace file paths. This file only
# warms/validates the registries at pipeline start so config errors fail the update
# early, with a clear message, before any transform evaluates.
#
# TODO: call loader.load_sources/load_contracts/load_dq_rules at module level
