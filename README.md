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
