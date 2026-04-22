# Design Note

## 1. System Overview

This pipeline is designed as a scheduled batch workflow. Raw betting data lands in a controlled input area, a validation job separates valid and invalid records, and a feature job builds customer-level features from validated bets. The local exercise represents the core processing layer inside a broader ML platform rather than the whole system.

## 2. Components And Responsibilities

- **Raw betting data landing area** receives source CSV files and acts as the system entry point.
- **Batch scheduler / job trigger** runs the workflow on a regular cadence or after a file arrives.
- **Validation job** enforces schema, domain, numeric, and derived-value rules, then outputs valid bets, invalid bets, and a machine-readable report.
- **Invalid-record quarantine / review queue** stores rejected records for investigation and operational follow-up.
- **Validated bets layer** holds curated records that downstream jobs can trust.
- **Customer feature generation job** aggregates each customer's first 20 valid bets into a feature row.
- **Versioned feature output / feature store / serving table** publishes stable feature datasets for downstream consumers.
- **Schema contract / metadata / configuration** defines the allowed input fields, rule versions, and feature definitions used by both jobs.
- **Logging / monitoring / alerting** captures run status, rule failures, data-volume changes, and operational incidents.
- **Downstream consumers** include batch model training, batch scoring or operational decisioning, and BI / analytics / CRM workflows.
- **Rerun / backfill / correction path** allows the pipeline to be re-executed if source files change or a defect is fixed.

Each component has a simple contract: raw data goes in, validated or derived data comes out, and metadata governs both validation and feature logic.

## 3. Batch vs Streaming

Batch processing is the right fit here because the brief is file-based, local, reproducible, and centered on customer aggregates rather than low-latency decisions. Streaming could make sense in production for near-real-time feature freshness or operational decisioning, but it would add complexity around late events, ordering, and state management that is unnecessary for this task.

## 4. Schema Safety, Invalid Records, And Feature Consistency

Schema safety comes from validating required columns, allowed values, numeric constraints, and derived values before feature generation runs. Rule versions and feature definitions should be tracked in metadata so downstream users know exactly which contract produced a dataset.

Invalid records are isolated into a quarantine path rather than dropped silently. That makes data quality visible, allows operators to investigate root causes, and prevents corrupted inputs from contaminating curated layers.

Feature consistency comes from using a single governed definition of the feature window, ordering logic, and aggregation rules. Producers and consumers should depend on the same documented schema and versioned dataset contract.

## 5. Downstream Interfaces, Reruns, And Operations

Downstream systems consume the curated feature table through a stable schema, versioned output location, or feature-store-style table. Batch training jobs can read full historical snapshots, while scoring, CRM, or BI workflows can read the latest approved version.

Reruns and backfills should be idempotent: the same input and rule version should produce the same outputs. If source data changes or a validation bug is fixed, the pipeline should be able to reprocess the affected file range and publish a corrected validated-bets layer and feature dataset.

In production, I would log run start/end, row counts, invalid row counts by rule, customers emitted, and feature-window edge cases. I would alert on job failure, unusual invalid-rate spikes, missing input files, schema drift, and unexpected drops in customer coverage.

## 6. Trade-Offs And Assumptions

- Batch is preferred over streaming for simplicity and reproducibility.
- CSV is acceptable for this submission because it is easy to inspect and avoids extra dependencies; parquet would be stronger in a larger production pipeline.
- Invalid records are surfaced explicitly, not hidden.
- Feature generation depends on the validated layer so downstream consumers do not need to re-implement data quality rules.
- The local assignment uses a minimal stack, but the same responsibilities would map naturally into a larger production ML platform.
