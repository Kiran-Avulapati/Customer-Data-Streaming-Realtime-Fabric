# Project Documentation — Real-Time Customer Data Platform

## 1. Purpose

This document provides a detailed technical reference for the Real-Time Customer Data Platform: design decisions, data contracts, processing logic, and operational procedures. For a high-level overview, see [`README.md`](../README.md).

---

## 2. System Context

| Component | Role |
|---|---|
| Customer API | External source system emitting customer records |
| Fabric Eventstream | Real-time ingestion layer; delivers events into Bronze |
| Spark Structured Streaming | Continuous transformation engine across all layers |
| Delta Lake / OneLake | Transactional storage for all Medallion layers |
| Fabric Spark Job Definition | Production orchestration and scheduling |
| Power BI | Downstream analytics and reporting |

---

## 3. Medallion Layers

### 3.1 Bronze — `bronze.api_raw_data`

- Receives raw events directly from Eventstream, unmodified.
- Serves as the immutable source of truth and streaming source for Silver.
- Periodically compacted with `OPTIMIZE bronze.api_raw_data` to reduce small-file overhead from continuous micro-batch writes.

**Schema (raw):** source JSON payload as delivered by the Customer API, including authentication-related fields (password, salt, hash variants) that are filtered out downstream.

### 3.2 Silver — `silver.api_silver_data`

Processing steps:

1. Read Bronze as a streaming Delta source.
2. Validate and filter malformed or incomplete records.
3. Normalize nested JSON into flat columns.
4. Extract standardized customer attributes.
5. Append processing metadata: `injection_timestamp`, `processing_timestamp`.

**Checkpoint:** `Files/checkpoint/api_silver`

### 3.3 Gold Layer

Five independent streaming queries consume Silver and write to five Gold tables. Each query has its own trigger, checkpoint, and write strategy (MERGE or append).

---

## 4. Gold Table Specifications

### 4.1 `gold.gold_customers`

| Property | Detail |
|---|---|
| Business key | `user_id` |
| Write strategy | `foreachBatch` + Delta MERGE (upsert) |
| Dedup logic | Within each micro-batch, latest record per `user_id` selected by `injection_timestamp` |
| Excluded fields | `password`, `salt`, `md5`, `sha1`, `sha256` |
| Checkpoint | `Files/checkpoints/silver_to_gold_customers` |

MERGE logic:
```sql
MERGE INTO gold_customers t
USING batch_updates s
ON t.user_id = s.user_id
WHEN MATCHED THEN UPDATE SET *
WHEN NOT MATCHED THEN INSERT *
```

### 4.2 `gold.gold_data_quality`

Scores each customer profile 0–7 based on presence of: First Name, Last Name, Email, Phone Number, Country, City, Age.

| Score range | Classification |
|---|---|
| 7 | Complete |
| 5–6 | Mostly Complete |
| 3–4 | Partially Complete |
| 0–2 | Poor |

**Checkpoint:** `Files/checkpoints/quality`

### 4.3 `gold.gold_pipeline_metrics`

Tracks per-record processing latency:

```
processing_latency_seconds = processing_timestamp - injection_timestamp
```

Used for pipeline health monitoring and SLA tracking.

**Checkpoint:** `Files/checkpoints/metrics`

### 4.4 `gold.gold_demographics`

Buckets customers into age groups. Backed by a state table for incremental recomputation.

| Age range | Group label |
|---|---|
| < 18 | Under 18 |
| 18–25 | 18-25 |
| 26–35 | 26-35 |
| 36–50 | 36-50 |
| > 50 | 51+ |

**State table:** `gold.gold_demographic_state`
**Checkpoint:** `Files/checkpoints/demographics`

### 4.5 `gold.gold_geography`

Tracks customer distribution by Country, State, City. Backed by a state table for incremental updates.

**State table:** `gold.gold_geography_state`
**Checkpoint:** `Files/checkpoints/geography`

---

## 5. Incremental Processing Model

State-driven Gold tables (demographics, geography) avoid full recomputation on every batch:

```
New Batch → Compare against Current State → Calculate Deltas → Update Gold → Update State
```

Upsert-driven Gold tables (customers) use Delta MERGE directly against the target table rather than a separate state table, since the target table *is* the current state.

---

## 6. Checkpointing & Fault Tolerance

Each of the six streaming queries maintains an independent Delta checkpoint directory:

| Query | Checkpoint Path |
|---|---|
| Bronze → Silver | `Files/checkpoint/api_silver` |
| Silver → Gold Customers | `Files/checkpoints/silver_to_gold_customers` |
| Silver → Data Quality | `Files/checkpoints/quality` |
| Silver → Pipeline Metrics | `Files/checkpoints/metrics` |
| Silver → Demographics | `Files/checkpoints/demographics` |
| Silver → Geography | `Files/checkpoints/geography` |

Independent checkpoints mean a failure or restart in one query does not affect or require replay of the others.

---

## 7. Production Orchestration

All six streaming queries are defined in a single application: `src/rt_project_streaming_prd.py`, deployed as the Fabric Spark Job Definition **`RT_Project_Streaming_PRD`**.

```python
# Pseudocode structure
bronze_to_silver_query = ...
silver_to_gold_customers_query = ...
silver_to_gold_quality_query = ...
silver_to_gold_metrics_query = ...
silver_to_gold_demographics_query = ...
silver_to_gold_geography_query = ...

spark.streams.awaitAnyTermination()
```

- `awaitAnyTermination()` keeps the job process alive as long as any query is running.
- Retry/restart behavior is configured at the Spark Job Definition level in Fabric, enabling recovery after failure without manual intervention.

---

## 8. Data Validation Procedure

1. Confirm record propagation Bronze → Silver → Gold while streaming is active.
2. Query each Gold and state table to confirm row counts reconcile.
3. Check for duplicate `user_id`s in `gold_customers` (expected: 0).
4. Allow processing to settle, then re-verify equal record counts across downstream tables.

---

## 9. Repository Layout

```
RT-Project/
├── README.md
├── notebooks/
│   ├── dev/
│   ├── test/
│   └── prd/
├── src/
│   ├── bronze_to_silver.py
│   ├── silver_to_gold.py
│   └── rt_project_streaming_prd.py
├── documentation/
│   └── project-documentation.md
└── architecture/
    └── architecture-diagram.png
```

---

## 10. Operational Notes

- Run `OPTIMIZE` on Bronze periodically (e.g., scheduled maintenance notebook) to control small-file growth from continuous ingestion.
- Monitor `gold_pipeline_metrics` for latency drift as an early signal of backpressure.
- State tables (`gold_demographic_state`, `gold_geography_state`) should be periodically reviewed for size growth as the customer base scales.

---

## 11. Roadmap

- [ ] Real-time Power BI dashboard refresh
- [ ] Alerting on data-quality degradation
- [ ] Advanced streaming monitoring (query progress, throughput dashboards)
- [ ] Schema evolution handling
- [ ] CI/CD and infrastructure-as-code for Fabric artifacts
- [ ] Additional business KPIs and automated data-quality rules
