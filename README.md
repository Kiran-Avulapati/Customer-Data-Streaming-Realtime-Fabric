# ⚡ Real-Time Customer Data Platform

<p align="center">

  <img src="https://img.shields.io/badge/Microsoft%20Fabric-Real--Time%20Data%20Engineering-742774?style=for-the-badge&logo=microsoft" alt="Microsoft Fabric"/>

  <img src="https://img.shields.io/badge/Spark-Structured%20Streaming-E25A1C?style=for-the-badge&logo=apachespark" alt="Spark"/>

  <img src="https://img.shields.io/badge/Delta%20Lake-Lakehouse-00ADD8?style=for-the-badge" alt="Delta Lake"/>

  <img src="https://img.shields.io/badge/PySpark-Streaming-F7B500?style=for-the-badge&logo=python" alt="PySpark"/>

  <img src="https://img.shields.io/badge/Power%20BI-Analytics-F2C811?style=for-the-badge&logo=powerbi" alt="Power BI"/>

</p>

<p align="center">

  <strong>🚀 An End-to-End, Always-On Real-Time Data Engineering Platform</strong>

</p>

<p align="center">
  Customer API → Eventstream → Bronze → Silver → Gold → Power BI
</p>

---

## 📌 Overview

This project implements an **end-to-end, always-on real-time customer data platform using Microsoft Fabric**.

A **Customer API** continuously provides customer records. Fabric Eventstream ingests the incoming events into the Lakehouse Bronze layer, while **Spark Structured Streaming** continuously transforms and processes the data through the Silver and Gold layers.

The platform follows the **Medallion Architecture** and uses modern Data Engineering patterns including:

- ⚡ Real-time event ingestion
- 🔄 Spark Structured Streaming
- 🏛️ Medallion Architecture
- 💾 Delta Lake
- 🔁 Delta MERGE / Upsert
- 🧹 Data deduplication
- 📦 `foreachBatch`
- 📍 Streaming checkpoints
- 🧠 Stateful processing
- 📊 Data-quality monitoring
- ⏱️ Pipeline processing metrics
- 🚀 Fabric Spark Job Definition
- 📈 Power BI-ready Gold datasets

---

# 🏗️ Architecture

```text
                         ┌──────────────────┐
                         │   CUSTOMER API   │
                         └────────┬─────────┘
                                  │
                                  ▼
                     ┌────────────────────────┐
                     │   FABRIC EVENTSTREAM   │
                     │   Real-Time Ingestion   │
                     └────────────┬───────────┘
                                  │
                                  ▼
              ┌────────────────────────────────────┐
              │             🥉 BRONZE              │
              │                                    │
              │       bronze.api_raw_data          │
              │                                    │
              │          Raw Event Data            │
              └────────────────┬───────────────────┘
                               │
                               │ Spark Structured
                               │ Streaming
                               ▼
              ┌────────────────────────────────────┐
              │             🥈 SILVER              │
              │                                    │
              │       silver.api_silver_data      │
              │                                    │
              │     Cleaned & Standardized Data    │
              └────────────────┬───────────────────┘
                               │
                               │ Spark Structured
                               │ Streaming
                               ▼
       ┌────────────────────────────────────────────────────────┐
       │                    🥇 GOLD LAYER                        │
       │                                                        │
       │  ┌──────────────────┐   ┌──────────────────────────┐  │
       │  │ 👤 Customers     │   │ ✅ Data Quality          │  │
       │  │                  │   │                          │  │
       │  │ gold_customers   │   │ gold_data_quality        │  │
       │  └──────────────────┘   └──────────────────────────┘  │
       │                                                        │
       │  ┌──────────────────┐   ┌──────────────────────────┐  │
       │  │ ⏱️ Metrics       │   │ 👥 Demographics           │  │
       │  │                  │   │                          │  │
       │  │ pipeline_metrics │   │ gold_demographics         │  │
       │  └──────────────────┘   └──────────────────────────┘  │
       │                                                        │
       │  ┌──────────────────────────────────────────────────┐ │
       │  │ 🌍 Geography                                      │ │
       │  │                                                  │ │
       │  │ gold_geography                                    │ │
       │  └──────────────────────────────────────────────────┘ │
       └─────────────────────────┬──────────────────────────────┘
                                 │
                                 ▼
                         ┌───────────────┐
                         │   📊 POWER BI │
                         │   Analytics   │
                         └───────────────┘
🎯 Project Goals

The platform was designed to solve the following Data Engineering requirements:

Requirement	Implementation
Real-time ingestion	Fabric Eventstream
Raw data storage	Bronze Delta table
Data transformation	Spark Structured Streaming
Data standardization	Silver layer
Incremental processing	Structured Streaming + Delta
Upserts	Delta MERGE
Deduplication	user_id + latest timestamp
Data quality	Completeness scoring
Demographics	Age-group transformation
Geography	Stateful geographic processing
Pipeline monitoring	Processing latency metrics
Recovery	Streaming checkpoints
Production execution	Fabric Spark Job Definition
Analytics	Power BI-ready Gold layer
🛠️ Technology Stack
<p align="center">
Technology	Role
🟣 Microsoft Fabric	End-to-end Data Engineering platform
⚡ Fabric Eventstream	Real-time ingestion
🔥 Apache Spark	Distributed processing
🐍 PySpark	Streaming & transformation logic
💾 Delta Lake	Transactional Lakehouse storage
🏞️ OneLake	Unified data storage
📊 Power BI	Analytics & visualization
🧮 SQL	Data validation & analysis
🚀 Spark Job Definition	Production streaming execution
</p>
🔄 Data Flow
1️⃣ Customer API

The Customer API continuously provides customer records.

Customer API
      │
      ▼
Fabric Eventstream

The API acts as the source system while Eventstream handles continuous event ingestion.

2️⃣ Bronze Layer
bronze.api_raw_data

The Bronze layer stores incoming source-level events.

Responsibilities
Receive events from Eventstream
Persist raw data
Preserve source-level information
Provide a streaming source for Silver processing
Customer API
     ↓
Eventstream
     ↓
bronze.api_raw_data
🥈 Silver Layer
silver.api_silver_data

The Silver layer transforms raw events into a standardized customer dataset.

Processing
Bronze
  ↓
Read Delta Stream
  ↓
Validate / Filter
  ↓
Normalize JSON
  ↓
Extract Customer Attributes
  ↓
Add Processing Metadata
  ↓
Silver
Processing Metadata
injection_timestamp
processing_timestamp
Checkpoint
Files/checkpoint/api_silver
🥇 Gold Layer

The Silver layer feeds five continuously updated analytical datasets.

                         SILVER
                           │
          ┌────────────────┼────────────────┐
          │                │                │
          ▼                ▼                ▼
     Customers        Data Quality       Metrics
          │
          ├──────────────────────┐
          ▼                      ▼
     Demographics            Geography
👤 Gold Customers
gold.gold_customers

The primary business-ready customer dataset.

Key Features
Customer-level analytical dataset
user_id as business key
Latest-record processing
Deduplication
Delta MERGE
Incremental upserts
Processing Pattern
Silver
  ↓
Latest Record
  ↓
Deduplication
  ↓
foreachBatch
  ↓
Delta MERGE
  ↓
gold.gold_customers
Sensitive Fields Excluded

The following authentication-related fields are not exposed in the Gold customer dataset:

Password
Salt
MD5
SHA1
SHA256
Checkpoint
Files/checkpoints/silver_to_gold_customers
✅ Data Quality
gold.gold_data_quality

The Data Quality pipeline evaluates customer profile completeness.

Evaluated Attributes
First Name
Last Name
Email
Phone Number
Country
City
Age
Quality Classification
Score	Classification
7	🟢 Complete
5–6	🟡 Mostly Complete
3–4	🟠 Partially Complete
0–2	🔴 Poor
Checkpoint
Files/checkpoints/quality
⏱️ Pipeline Metrics
gold.gold_pipeline_metrics

The pipeline captures processing-related metrics.

Processing Latency
processing_latency_seconds

Calculated using:

processing_timestamp - injection_timestamp

This provides visibility into the processing time of incoming records.

Checkpoint
Files/checkpoints/metrics
👥 Demographics
gold.gold_demographics

Customers are categorized into age groups.

<18      → Under 18
18–25    → 18-25
26–35    → 26-35
36–50    → 36-50
>50      → 51+
State Table
gold.gold_demographic_state

The state table enables incremental demographic processing.

Checkpoint
Files/checkpoints/demographics
🌍 Geography
gold.gold_geography

The Geography pipeline processes customer location information.

Attributes
Country
State
City
State Table
gold.gold_geography_state

The state table supports incremental geographic processing.

Checkpoint
Files/checkpoints/geography
🔁 Incremental Processing

A core design principle of the platform is incremental processing.

Instead of rebuilding entire Gold datasets for every incoming batch:

New Streaming Batch
        │
        ▼
Transformation
        │
        ▼
Latest Record Selection
        │
        ▼
Deduplication
        │
        ▼
foreachBatch
        │
        ▼
Delta MERGE
        │
        ▼
Gold Table

This enables continuously updated analytical datasets.

🔀 Delta MERGE Strategy

The customer business key is:

user_id

The pipeline uses Delta MERGE to implement upserts.

IF user_id exists
       ↓
    UPDATE

IF user_id does not exist
       ↓
    INSERT

This allows existing customer records to be updated while new customers are inserted.

🧹 Deduplication Strategy

Customer uniqueness is maintained using:

user_id

When multiple records exist for the same customer, the latest record is selected using:

injection_timestamp
Validation Result
Duplicate customer IDs = 0
📍 Streaming Checkpoints

Each streaming query maintains an independent checkpoint.

Streaming Process	Checkpoint
Bronze → Silver	Files/checkpoint/api_silver
Gold Customers	Files/checkpoints/silver_to_gold_customers
Data Quality	Files/checkpoints/quality
Pipeline Metrics	Files/checkpoints/metrics
Demographics	Files/checkpoints/demographics
Geography	Files/checkpoints/geography

Independent checkpoints allow each streaming query to maintain its own processing progress and recovery state.

🧠 State Management

Two dedicated state tables are used:

gold.gold_demographic_state
gold.gold_geography_state

The state tables store previously processed customer information.

Incoming batches are compared against the stored state to support incremental updates.

New Batch
   │
   ▼
Current State
   │
   ▼
Compare Old vs New
   │
   ▼
Calculate Changes
   │
   ▼
Update Gold
   │
   ▼
Update State
🚀 Production Deployment

All streaming transformations were consolidated into a single PySpark application:

Files/rt_project_streaming_prd.py
Spark Job Definition
RT_Project_Streaming_PRD

The production application contains:

Bronze → Silver

Silver → Gold
    ├── Customers
    ├── Data Quality
    ├── Pipeline Metrics
    ├── Demographics
    └── Geography

The application uses:

spark.streams.awaitAnyTermination()

to keep the multiple streaming queries running as an always-on streaming application.

Retry behavior is configured through the Spark Job Definition for application recovery.

📊 Data Validation

The pipeline was validated while the always-on streaming workloads were active.

Validation covered:

Record Propagation
Bronze → Silver → Gold
Gold Tables
gold_customers
gold_data_quality
gold_pipeline_metrics
gold_demographics
gold_geography
State Tables
gold_demographic_state
gold_geography_state
Data Quality
Duplicate customer IDs = 0
Final Reconciliation

After allowing streaming processing to settle, final reconciliation confirmed:

Equal record counts across the downstream analytical tables.

This validated that records were propagating correctly through the complete streaming architecture.

⚡ Delta Optimization

The Bronze Delta table was optimized using:

OPTIMIZE bronze.api_raw_data

The optimization compacted small files generated by continuous ingestion and improved query performance.

📈 Power BI Analytics

The Gold layer is designed for direct analytical consumption.

Potential Power BI dashboards include:

👤 Customer Overview
Customer count
Customer profiles
Customer attributes
✅ Data Quality
Completeness score
Quality distribution
Data-quality trends
👥 Demographics
Customer distribution by age group
🌍 Geography
Customers by country
Customers by state
Customers by city
⏱️ Pipeline Monitoring
Processing latency
Streaming processing metrics
📁 Project Structure
RT-Project/
│
├── README.md
│
├── notebooks/
│   ├── dev/
│   ├── test/
│   └── prd/
│
├── src/
│   ├── bronze_to_silver.py
│   ├── silver_to_gold.py
│   └── rt_project_streaming_prd.py
│
├── documentation/
│   └── project-documentation.md
│
└── architecture/
    └── architecture-diagram.png

The exact repository structure can be adapted to match the final exported Fabric artifacts.

🧩 Key Engineering Concepts

This project demonstrates practical implementation of:

⚡ Real-time Data Engineering
🏛️ Medallion Architecture
🔄 Spark Structured Streaming
💾 Delta Lake
🔁 Delta MERGE
📦 foreachBatch
🧹 Deduplication
📍 Streaming checkpoints
🧠 Stateful processing
✅ Data-quality engineering
⏱️ Pipeline metrics
🔧 Delta optimization
☁️ Microsoft Fabric Lakehouse
🚀 Spark Job Definitions
📊 Power BI-ready data
🏆 Project Highlights
End-to-End
Source → Ingestion → Storage → Transformation → Analytics
Real-Time
Continuous Event Ingestion
            ↓
Continuous Processing
            ↓
Continuously Updated Gold Tables
Incremental
New Records
    ↓
Transform
    ↓
MERGE
    ↓
Updated Analytical State
Production-Oriented
Streaming Application
        ↓
Spark Job Definition
        ↓
Always-On Execution
        ↓
Checkpoint-Based Recovery
🎓 What This Project Demonstrates

This project demonstrates the ability to design and implement a complete real-time Data Engineering solution rather than isolated transformations.

It combines:

Event-driven ingestion + Lakehouse architecture + Spark Structured Streaming + Delta Lake + Incremental Processing + Data Quality + State Management + Production Deployment + BI Consumption

🚀 Future Enhancements

Potential future enhancements include:

Advanced Power BI real-time dashboards
Alerting for data-quality degradation
Advanced streaming monitoring
Schema evolution handling
Automated CI/CD deployment
Infrastructure-as-code
Additional business KPIs
Automated data-quality rules
Streaming performance dashboards
📌 Final Project Summary

Built an end-to-end, always-on real-time customer data platform using Microsoft Fabric, implementing Eventstream-based ingestion, Medallion Architecture, Spark Structured Streaming, Delta Lake MERGE, incremental state management, data-quality processing, pipeline metrics, demographic and geographic analytics, and Power BI-ready Gold datasets.

⭐ Key Takeaway
                 REAL-TIME CUSTOMER DATA PLATFORM

Customer API
     ↓
Eventstream
     ↓
🥉 Bronze
     ↓
🥈 Silver
     ↓
🥇 Gold
 ┌────┼────┬────┬────┐
 ↓    ↓    ↓    ↓    ↓
👤   ✅   ⏱️   👥   🌍
Customer Quality Metrics Demo Geography
     ↓
  Power BI
Built with ❤️ using Microsoft Fabric & PySpark
