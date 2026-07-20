"""Audit framework: run-level counts and standing invariants, computed from tables.

Audit reads the catalog directly rather than the SDP event log — the event log is
not a stable public contract, so keeping it off the critical path (R4) means a
platform change cannot silently break auditing. Event-log enrichment is a v1.2
roadmap item and would land as its own isolated module.
"""
