# 🎧 Spotify Data Pipeline (Azure + Databricks)

## 📌 Overview

This project implements an end-to-end **data engineering pipeline** for processing Spotify-like music streaming data using a **Medallion Architecture (Bronze → Silver → Gold)**.

The pipeline is designed to handle **incremental ingestion, backfilling, scalable transformations, and analytics-ready data modeling** using Azure Data Factory and Databricks.

---

## 🏗️ Architecture

**Source → ADF Ingestion → Bronze Layer → Databricks (Silver) → Databricks (Gold)**

---

## ⚙️ Tech Stack

* **Azure Data Factory (ADF)** – Data ingestion & orchestration
* **Databricks** – Data processing & transformations
* **PySpark** – Data transformation logic
* **Delta Lake** – Reliable and scalable storage
* **SQL** – Querying and validation
* **Jinja Templates** – Dynamic SQL and reusable pipeline logic

---

## 🥉 Bronze Layer (Raw Data Ingestion)

* Implemented ingestion pipelines using **Azure Data Factory**
* Designed **incremental loading** using watermark logic
* Built **backfilling mechanism** to process historical data efficiently
* Developed an **incremental loop pipeline** to:

  * Avoid re-running pipelines manually for each dataset
  * Automate ingestion of new incoming files
* Leveraged **Jinja templating** to dynamically generate SQL queries and pipeline parameters

👉 Purpose: Store raw, immutable data for traceability and reprocessing
<img src="Screenshot 2026-04-29 153257.png" width="800"/>
<img src="Screenshot 2026-04-29 153153.png" width="800"/>
---

## 🥈 Silver Layer (Data Transformation – Databricks)

* Processed data using **Databricks with PySpark**
* Applied transformations including:

  * Data cleansing
  * Deduplication using window functions
  * Null handling
  * Schema standardization
* Implemented **incremental processing** to handle only new/updated data
* Stored processed data using **Delta Lake format**
* Enabled structured datasets for downstream analytics

👉 Purpose: Create clean, reliable, and query-ready datasets
<img src="Screenshot 2026-05-06 180702.png" width="800"/>
---

## 🥇 Gold Layer (Data Aggregation – Databricks)

* Built Gold layer pipelines in Databricks
* Created **aggregated datasets** for business insights
* Generated analytics such as:

  * Top artists
  * Listening trends over time
  * Genre popularity

👉 Purpose: Deliver business-level insights and reporting-ready data
<img src="Screenshot 2026-05-06 180754.png" width="800"/>
---

## 🔄 Key Features

* ✅ Incremental data ingestion using watermark logic
* ✅ Automated backfilling for historical data
* ✅ Dynamic SQL generation using Jinja templates
* ✅ End-to-end pipeline orchestration with ADF
* ✅ Scalable data processing using PySpark
* ✅ Reliable storage using Delta Lake
* ✅ Medallion architecture implementation

---

## 📊 Data Flow Summary

1. Data ingested via ADF using incremental and backfill strategies
2. Raw data stored in Bronze layer
3. Data transformed and cleaned in Silver layer using Databricks
4. Aggregated datasets created in Gold layer for analytics

---

## 🧠 Learnings

* Built an end-to-end **data pipeline with real-world scenarios**
* Gained hands-on experience in **incremental loading and backfilling**
* Implemented **Medallion Architecture using Azure and Databricks**
* Used **Jinja templating for dynamic and reusable pipeline logic**
* Worked with **PySpark and Delta Lake for scalable data processing**

---

## 👨‍💻 Author

Raghav Manhas
