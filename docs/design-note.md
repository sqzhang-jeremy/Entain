# Design Note

## 1. System Overview

Figure 1(./architecture-diagram.png) shows the local validation and feature pipeline placed inside a broader batch ML system. Raw betting data lands in a controlled input area, a validation job separates valid and invalid records, and a feature job builds customer-level features from validated bets for downstream consumers.

## 2. Components And Responsibilities

- **Raw betting data landing area** exists to receive source files safely and preserve the original input.  
`in:` source betting files.  
`out:` immutable raw file plus receipt metadata.
- **Batch scheduler / job trigger** exists to start the workflow in a controlled, repeatable way.  
`in:` cadence, file-arrival signal, or configuration.  
`out:` run request with run id.
- **Validation job** exists to enforce the data contract before downstream use.  
`in:` raw file plus schema/rule version.  
`out:` valid bets, invalid bets, validation report.
- **Invalid-record quarantine / review queue** exists to isolate bad data and make it visible to operators.  
`in:` invalid rows plus failure reasons.  
`out:` review dataset or operator action queue.
- **Validated bets layer** exists to provide a curated dataset that downstream jobs can trust.  
`in:` validated rows plus run id.  
`out:` curated, versioned validated-bets snapshot.
- **Customer feature generation job** exists to transform trusted bets into customer-level ML features.  
`in:` validated-bets snapshot plus feature-definition version.  
`out:` customer-features snapshot plus feature-build report.
- **Versioned feature output / feature store / serving table** exists to publish stable, consumable features.  
`in:` feature snapshot plus dataset version.  
`out:` immutable published version plus latest-approved pointer.
- **Schema contract / metadata / configuration** exists to govern validation and feature logic consistently.  
`in:` rule definitions, schema versions, feature definitions.  
`out:` governed contracts consumed by jobs and downstream users.
- **Logging / monitoring / alerting** exists to make the pipeline observable and operable.  
`in:` run events, quality metrics, anomalies.  
`out:` logs, dashboards, alerts.
- **Downstream consumers** exist to turn the curated features into business value.  
`in:` approved versioned feature dataset.  
`out:` training sets, scoring batches, BI extracts, CRM activation inputs.
- **Rerun / backfill / correction path** exists to recover from source changes or logic defects.  
`in:` corrected source data, bug fix, or new rule version.  
`out:` rerun request and corrected dataset versions.

## 3. Batch vs Streaming

Batch is the right fit here because the feature logic is historical and aggregate: it depends on each customer's first 20 bets rather than a single low-latency event. Streaming could make sense later for near-real-time scoring or in-session decisioning where freshness matters, but it would add complexity around ordering, state, and late-arriving events that is unnecessary for this task.

## 4. Schema Safety, Versioning, And Invalid Records

Schema safety comes from validating required columns, allowed values, numeric constraints, and derived values before feature generation runs. Rule versions and feature definitions should be tracked in metadata so downstream users know exactly which contract produced a dataset.

Each validation and feature run should write to an immutable versioned location keyed by run id plus rule version or feature-definition version. Reruns and backfills should publish a new version rather than overwrite old outputs, and a latest-approved pointer or manifest should move only after a successful run.

Invalid records are isolated into a quarantine path rather than dropped silently. That makes data quality visible, allows operators to investigate root causes, and prevents corrupted inputs from contaminating curated layers.

Feature consistency comes from using one governed definition of the feature window, ordering logic, and aggregation rules. Producers and consumers should depend on the same documented schema and versioned dataset contract.

## 5. Downstream Interfaces, Reruns, And Operations

Downstream systems consume the curated feature table through a stable schema, versioned output location, or feature-store-style table. Batch training jobs can read full historical snapshots, while scoring, CRM, or BI workflows can read the latest approved version.

Reruns and backfills should be idempotent: the same input and same rule or feature version should produce the same outputs. If source data changes or a validation bug is fixed, the pipeline should reprocess the affected range and publish corrected validated-bets and customer-features versions without deleting historical outputs.

In production, observability should be split clearly:

- **Logs:** run start/end, run id, input file id or hash, rule version, feature version.
- **Metrics:** raw rows, valid rows, invalid rows by rule, customer coverage, customers with invalids in the first 20, partial-window count, runtime.
- **Alerts:** job failure, missing input, schema drift, invalid-rate spike, freshness SLA miss, unexpected customer-coverage drop.

## 6. Trade-Offs And Assumptions

- Batch is preferred over streaming for simplicity and reproducibility.
- `build-features` re-validates raw input to keep the CLI self-contained. In a larger ML system, the feature job would consume the validated-bets layer directly to avoid duplicate validation and preserve a cleaner staged architecture.
- CSV is easy to inspect and diff, but it gives up typed schema preservation compared with Parquet; that trade-off is acceptable for this submission size.
- Invalid records are surfaced explicitly, not hidden.
- The local assignment uses a minimal stack, but the same responsibilities map naturally into a larger production ML platform.

