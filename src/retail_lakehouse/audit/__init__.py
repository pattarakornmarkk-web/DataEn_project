"""Audit framework: event-log harvesting, run audit rows, standing invariants.

R4 note: ALL event-log schema knowledge is isolated in event_log.py — the SDP event
log is not a stable public contract; when it changes, only that module changes.
"""
