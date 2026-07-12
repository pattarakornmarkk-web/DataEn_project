"""Scenario catalog access — THE single interpreter of mock_data/scenarios/*.yml.

Consumers (exactly three, no other interpretation allowed):
  1. source-emulator entry point (writes landing files in the workspace)
  2. tests/conftest.py load_scenario fixture (in-memory DataFrames)
  3. integration assertions (reference scenarios by name for expected outcomes)

Scenario YAML ships inside the wheel (retail_lakehouse/_data/scenarios — ADR-0008).
"""
