# SQL Scripts for Risk Grouper Data Pipeline

This document outlines the SQL scripts used in the data preparation pipeline for the Risk Grouper project. The pipeline is orchestrated by the main `main.py` script, which executes these SQL files in the correct order.

## Architecture Overview

The data pipeline has been re-architected to ensure **point-in-time correctness** and prevent data leakage. It uses a two-stage process:

1.  **`events.sql`**: This script creates a preliminary, comprehensive log of all discrete member events (`EVENTS_WITH_LABELS_RX`). This includes medical claims, care notes, and simple pharmacy fills. It does **not** apply any data availability lags at this stage. It also generates the final look-ahead labels (e.g., `y_ed_30d`) for model training.

2.  **`daily_aggregation.sql`**: This is the core of the feature engineering. It consumes the event log from the previous step and also reads directly from raw source tables. Its key responsibilities are:
    *   Creating a single row for every member for every day.
    *   Calculating all features using **event-relative rolling lookback windows** (e.g., 90 and 180 days).
    *   Applying **data availability lags** (`$MEDICAL_CLAIM_LAG_MONTHS`, `$PHARMACY_CLAIM_LAG_MONTHS`) to ensure that features for any given day are calculated using only data that would have been realistically available at that time.
    *   Implementing a **hybrid pharmacy feature strategy**: It calculates both rolling counts of simple pharmacy fills and performs a complex, stateful "days-in-hand" calculation.

## Execution

The `main.py` script executes these two SQL files sequentially. There is no longer a need to run multiple granular scripts like `data_prep.sql` or `data_split.sql`, as their logic has been consolidated into the main two files and the Python-based modeling script.

## Version Log

| Version | Date       | Author | Changes                                      |
|---------|------------|--------|----------------------------------------------|
| 2.0     | 2025-08-19 | Copilot| Major re-architecture to a two-step, point-in-time correct pipeline. Implemented hybrid pharmacy features. Consolidated logic from older scripts. |
| 1.1     | 2025-08-19 | Copilot| Added data lag variables for medical (4 months) and pharmacy (2 months) claims in `events.sql` to simulate realistic data availability. |
| 1.0     | 2025-08-19 | Copilot| Initial version of all SQL scripts.          |
```          |
