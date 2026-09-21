<div align="center">

# ⚡ Real-Time Customer Data Platform

### An always-on streaming Lakehouse built on Microsoft Fabric

[![Microsoft Fabric](https://img.shields.io/badge/Microsoft%20Fabric-Real--Time%20Data-742774?style=for-the-badge&logo=microsoft&logoColor=white)](#)
[![Spark](https://img.shields.io/badge/Spark-Structured%20Streaming-E25A1C?style=for-the-badge&logo=apachespark&logoColor=white)](#)
[![Delta Lake](https://img.shields.io/badge/Delta%20Lake-Lakehouse-00ADD8?style=for-the-badge)](#)
[![PySpark](https://img.shields.io/badge/PySpark-Streaming-F7B500?style=for-the-badge&logo=python&logoColor=white)](#)
[![Power BI](https://img.shields.io/badge/Power%20BI-Analytics-F2C811?style=for-the-badge&logo=powerbi&logoColor=black)](#)

**Customer API → Eventstream → Bronze → Silver → Gold → Power BI**

</div>

---

## 📖 Table of Contents

- [Overview](#-overview)
- [Architecture](#-architecture)
- [Platform Stats](#-platform-stats)
- [Data Flow](#-data-flow)
- [Gold Layer Datasets](#-gold-layer-datasets)
- [Incremental Processing & Delta MERGE](#-incremental-processing--delta-merge)
- [Checkpoints & Recovery](#-checkpoints--recovery)
- [Tech Stack](#-tech-stack)
- [Project Structure](#-project-structure)
- [Production Deployment](#-production-deployment)
- [Validation](#-validation)
- [Power BI Dashboards](#-power-bi-dashboards)
- [Roadmap](#-roadmap)

---

## 🧭 Overview

This project implements an **end-to-end, always-on real-time customer data platform** using Microsoft Fabric. A Customer API continuously streams customer records, which are ingested through Fabric Eventstream and progressively refined across the **Medallion Architecture** (Bronze → Silver → Gold) using Spark Structured Streaming — landing in Power BI-ready analytical tables.

| Capability | Detail |
|---|---|
| 🔴 Ingestion | Real-time, event-driven via Fabric Eventstream |
| 🔄 Processing | Spark Structured Streaming, `foreachBatch` |
| 💾 Storage | Delta Lake on OneLake |
| 🔁 Upserts | Delta MERGE, keyed on `user_id` |
| 🧠 State | Dedicated state tables for demographics & geography |
| 📍 Recovery | Independent checkpoints per streaming query |
| 📊 Consumption | Power BI, SQL analytics endpoint |

---

## 🏗️ Architecture

```text
                     ┌──────────────────┐
                     │   CUSTOMER API   │
                     └────────┬─────────┘
                              ▼
                 ┌────────────────────────┐
                 │   FABRIC EVENTSTREAM   │
                 └────────────┬───────────┘
                              ▼
                 🥉  bronze.api_raw_data
                              │  Spark Structured Streaming
                              ▼
                 🥈  silver.api_silver_data
                              │  Spark Structured Streaming
                              ▼
   ┌───────────────────────────────────────────────────┐
   │                      🥇 GOLD                       │
   │  gold_customers   gold_data_quality   gold_metrics │
   │        gold_demographics       gold_geography      │
   └───────────────────────┬───────────────────────────┘
                              ▼
                         📊 POWER BI
```

---

## 📊 Platform Stats

| Metric | Value |
|---|---|
| Streaming queries running concurrently | 6 |
| Medallion layers | 3 (Bronze / Silver / Gold) |
| Gold analytical tables | 5 |
| State tables | 2 (demographics, geography) |
| Independent checkpoints | 6 |
| Duplicate `user_id`s at reconciliation | 0 |
| Business key | `user_id` |
| Dedup strategy | Latest `injection_timestamp` |
| Orchestration | `spark.streams.awaitAnyTermination()` |

*Fill in throughput/latency numbers (e.g. events/sec, avg `processing_latency_seconds`) once you have production metrics — they make this section genuinely compelling.*

---

## 🔄 Data Flow

**1. Customer API → Eventstream** — source system emits customer records; Eventstream handles continuous ingestion.

**2. Bronze (`bronze.api_raw_data`)** — raw events land untouched, preserving full source fidelity.

**3. Silver (`silver.api_silver_data`)**
```
Bronze → Read Delta Stream → Validate/Filter → Normalize JSON
      → Extract Attributes → Add Metadata (injection/processing timestamps) → Silver
```

**4. Gold** — five continuously updated datasets, each with its own streaming query, checkpoint, and MERGE/append logic.

---

## 🥇 Gold Layer Datasets

<details>
<summary><strong>👤 gold_customers</strong> — primary business-ready customer table</summary>

- Latest-record selection + deduplication on `user_id`
- Delta MERGE via `foreachBatch` (update if exists, insert if new)
- **Excludes** sensitive fields: password, salt, MD5, SHA1, SHA256
- Checkpoint: `Files/checkpoints/silver_to_gold_customers`
</details>

<details>
<summary><strong>✅ gold_data_quality</strong> — profile completeness scoring</summary>

Evaluates: First Name, Last Name, Email, Phone, Country, City, Age

| Score | Classification |
|---|---|
| 7 | 🟢 Complete |
| 5–6 | 🟡 Mostly Complete |
| 3–4 | 🟠 Partially Complete |
| 0–2 | 🔴 Poor |

Checkpoint: `Files/checkpoints/quality`
</details>

<details>
<summary><strong>⏱️ gold_pipeline_metrics</strong> — processing latency</summary>

`processing_latency_seconds = processing_timestamp − injection_timestamp`

Checkpoint: `Files/checkpoints/metrics`
</details>

<details>
<summary><strong>👥 gold_demographics</strong> — age-group segmentation</summary>

| Age | Group |
|---|---|
| < 18 | Under 18 |
| 18–25 | 18-25 |
| 26–35 | 26-35 |
| 36–50 | 36-50 |
| > 50 | 51+ |

State table: `gold_demographic_state` · Checkpoint: `Files/checkpoints/demographics`
</details>

<details>
<summary><strong>🌍 gold_geography</strong> — location segmentation</summary>

Attributes: Country, State, City
State table: `gold_geography_state` · Checkpoint: `Files/checkpoints/geography`
</details>

---

## 🔁 Incremental Processing & Delta MERGE

```
New Batch → Transform → Latest Record → Dedup → foreachBatch → Delta MERGE → Gold
```

```sql
IF user_id EXISTS  → UPDATE
IF user_id NEW      → INSERT
```

State-driven tables (demographics, geography) compare each incoming batch against stored state to compute incremental changes before updating Gold.

---

## 📍 Checkpoints & Recovery

| Streaming Query | Checkpoint Path |
|---|---|
| Bronze → Silver | `Files/checkpoint/api_silver` |
| Silver → Gold Customers | `Files/checkpoints/silver_to_gold_customers` |
| Data Quality | `Files/checkpoints/quality` |
| Pipeline Metrics | `Files/checkpoints/metrics` |
| Demographics | `Files/checkpoints/demographics` |
| Geography | `Files/checkpoints/geography` |

Each query recovers independently on failure — no shared state, no cross-query blocking.

---

## 🛠️ Tech Stack

| Layer | Technology |
|---|---|
| Platform | Microsoft Fabric |
| Ingestion | Fabric Eventstream |
| Processing | Apache Spark, PySpark |
| Storage | Delta Lake, OneLake |
| Orchestration | Fabric Spark Job Definition |
| Analytics | Power BI, SQL |

---

## 📁 Project Structure

```
RT-Project/
├── README.md
├── scripts/
│   ├── bronze_to_silver.py
│   ├── silver_to_gold.py
│   └── rt_project_streaming_prd.py
├── documentation/
│   └── project-documentation.md
└── architecture/
    └── architecture-diagram.png
```

---

## 🚀 Production Deployment

All streaming logic is consolidated into a single application, `rt_project_streaming_prd.py`, deployed as the Spark Job Definition **`RT_Project_Streaming_PRD`**. It runs Bronze→Silver and all five Silver→Gold queries concurrently via `spark.streams.awaitAnyTermination()`, with retry behavior configured at the job level for always-on recovery.

---

## ✅ Validation

- Record propagation verified Bronze → Silver → Gold
- **Zero** duplicate `user_id`s in `gold_customers`
- Reconciled record counts across all Gold and state tables
- `OPTIMIZE bronze.api_raw_data` run to compact small files from continuous ingestion

---

## 📈 Power BI Dashboards

| Dashboard | Metrics |
|---|---|
| 👤 Customer Overview | Count, profiles, attributes |
| ✅ Data Quality | Completeness score, quality distribution, trends |
| 👥 Demographics | Distribution by age group |
| 🌍 Geography | By country, state, city |
| ⏱️ Pipeline Monitoring | Processing latency, streaming metrics |

---

## 🗺️ Roadmap

- [ ] Real-time Power BI dashboard refresh
- [ ] Alerting on data-quality degradation
- [ ] Advanced streaming monitoring
- [ ] Schema evolution handling
- [ ] CI/CD + infrastructure-as-code
- [ ] Additional business KPIs

---

<div align="center">

**Built with ❤️ using Microsoft Fabric & PySpark**

</div>
