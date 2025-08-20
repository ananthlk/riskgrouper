# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

## [1.2.0] - 2025-08-19

### Changed
- **Refined Pharmacy Feature Engineering:**
  - Implemented a hybrid approach for pharmacy data to capture both discrete fill events and continuous medication adherence.
  - `events.sql` now creates simple `PHARMACY_FILL` events with boolean flags (e.g., `is_fill_antipsychotic`). The complex recursive "days-in-hand" logic was removed from this script.
  - `daily_aggregation.sql` now contains two distinct pharmacy-related calculations:
    1. It calculates rolling counts of the simple fill events from the upstream `events` table.
    2. It performs the complex, recursive "days-in-hand" calculation directly from the raw pharmacy claims table, ensuring point-in-time correctness by applying the pharmacy data lag.
- Updated all relevant READMEs and script comments to reflect the new architecture.

## [1.1.0] - 2025-08-19

### Changed
- **Major Rearchitecture of Data Pipeline:**
  - Implemented a **point-in-time correct** data aggregation strategy to prevent data leakage.
  - The `events.sql` script now generates a complete historical log of all member events without applying a premature data lag.
  - The `daily_aggregation.sql` script was completely overhauled to use this event log. It now calculates features using event-relative rolling lookback windows (90 and 180 days).
  - This ensures that all features for a given member-day are generated using only the data that would have been historically available at that specific point in time, respecting the 4-month claim data lag.

## [1.0.0] - 2025-08-18

### Added
- Initial project setup.
- Core Python scripts for risk modeling (`RiskGrouper.py`), Snowflake connection (`snowflake_connector.py`), and segment analysis (`segment_analysis.py`).
- SQL scripts for data preparation (`events.sql`, `daily_aggregation.sql`, `data_split.sql`).
- Initial data validation script (`data_validation.sql`) and runner.
- Configuration managed via `.env` file.
- Added comprehensive docstrings and comments to all scripts.
