
# Agile Sprint – Retail Inventory Forecasting using PySpark & Hive on EMR

## 📌 Project Overview
This project demonstrates an **end-to-end batch data engineering pipeline** for **Retail Inventory Forecasting**, built using **PySpark and Hive on AWS EMR**, executed through **Agile Scrum methodology**.

- **Sprint Cadence:** 8 hours/day  
- **Total Sprints:** 5 (+ Sprint 0 – Infra Setup)  
- **Core Stack:** PySpark, Hive, AWS EMR, S3, Airflow  
- **Goal:** Optimize and automate retail inventory forecasting using scalable batch pipelines.

---

## 🗂 Dataset Overview

| Parameter | Description |
|---------|-------------|
| Dataset Files | `transactions.csv`, `items.csv`, `cities.json` |
| Total Size | ~2.5M records |
| Transactions Volume | ~2.2M rows |
| Formats | CSV (transactions, items), JSON (cities metadata) |
| Data Quality | Nulls, duplicates, anomalies, minor outliers |
| Domain | Retail Analytics |

---

## 🏗 Architecture Highlights

- **Data Lake:** Amazon S3 (Raw → Staging → Curated)
- **Processing Engine:** Apache Spark (PySpark) on EMR
- **Metadata:** Hive Metastore
- **Orchestration:** Apache Airflow
- **Monitoring & Security:** IAM, Kerberos, CloudWatch

---

## 🔁 Agile Sprint Breakdown

---

## 🟦 Sprint 0 – Infrastructure & Security Foundation (DevOps)

### Objectives
Provision a secure, scalable EMR environment with fault tolerance and observability.

### Sprint Tasks
- Provision EMR Cluster:
  - 6 nodes (`m5.xlarge`)
  - 1 Master + 5 Core nodes
- Configure:
  - VPC, Subnet, NAT Gateway
  - Auto-scaling policy
  - IAM Role (S3 + CloudWatch full access)
  - SSH Key pair access
- S3 Data Lake Setup:
  - `s3://retail-capstone/raw/`
  - `s3://retail-capstone/staging/`
  - `s3://retail-capstone/curated/`
- Enable:
  - Kerberos authentication
  - CloudWatch EMR logging
  - Hive Metastore
  - Spark History Server
- Fault Tolerance:
  - Daily snapshot on master
  - Spark checkpointing

### Deliverables
- ✔ CloudFormation / Terraform script (optional)
- ✔ EMR logs screenshot
- ✔ VPC & network architecture diagram

---

## 🟩 Sprint 1 – Raw Zone Ingestion & Schema Registration

### Objectives
Land raw data into S3 and register schema-driven Hive tables.

### Sprint Tasks
- Upload datasets to: s3://retail-capstone/raw/

- Read CSV files using PySpark:
- Explicit schema definitions
- Custom delimiter handling
- Register Hive External Tables:
- `transactions_raw`
- `items_raw`
- `cities_raw`
- Perform schema validation using Spark DataFrame APIs

### Deliverables
- ✔ Hive external table DDLs
- ✔ PySpark ingestion script
- ✔ Schema validation report

---

## 🟨 Sprint 2 – Data Quality & Cleansing Pipeline

### Objectives
Improve data reliability through cleansing and enrichment.

### Sprint Tasks
**Data Quality Checks**
- Null detection
- Duplicate detection (InvoiceID, ItemID)
- Inconsistent pricing
- Outliers in:
- Quantity
- UnitPrice (percentile-based)

**Cleansing Logic**
- Fill missing city values using JSON metadata
- Remove zero or negative-priced transactions

**Feature Engineering**
- Add derived columns:
- `WeekNumber`
- `Quarter`
- `IsHighValueOrder`

**Storage**
- Write cleaned data as **Parquet**
- Partitioned by `Year-Month`
- Save to: s3://retail-capstone/staging/

- **Metadata**
- Register `transactions_cleaned` Hive table

### Deliverables
- ✔ PySpark cleansing script
- ✔ Data quality metrics report
- ✔ Staging Hive table DDL

---

## 🟧 Sprint 3 – Business Logic & Metrics Aggregation

### Objectives
Generate analytical features and metrics for inventory forecasting.

### Metrics Computed
- Revenue by:
- Month
- City
- SKU
- Average quantity sold per city
- Top 5 selling items per region

### Sprint Tasks
- Join transaction data with city metadata
- Aggregate results and store in: s3://retail-capstone/curated/analytics/

- - Create Hive analytics table:
- `inventory_metrics`

### Deliverables
- ✔ Aggregation PySpark script
- ✔ Sample metrics snapshots
- ✔ Curated Hive schema

---

## 🟥 Sprint 4 – Workflow Orchestration using Airflow

### Objectives
Automate the entire batch pipeline with monitoring and alerts.

### DAG Workflow
1. Ingest
2. Cleanse
3. Aggregate
4. Validate Output

### Airflow Components
- `S3KeySensor` – Detect new files
- `SparkSubmitOperator` – Trigger EMR Spark jobs
- `PythonOperator` – Metric validation
- Retry logic, SLA definition, email alerts

### Scheduling
- **Daily at 01:00 UTC**

### Deliverables
- ✔ Airflow DAG script
- ✔ Task execution logs
- ✔ Airflow UI screenshots

---

## 🟪 Sprint 5 – Business Insights & Visualization

### Objectives
Translate data into actionable business insights.

### KPIs
- Monthly revenue trends
- Region-wise stock shortage indicators
- Top 10 most profitable SKUs

### Output Formats
- JSON / CSV extracts
- Optional dashboards using:
- Amazon QuickSight
- Apache Superset
- Excel

### Deliverables
- ✔ KPI exports
- ✔ Dashboard screenshots
- ✔ Business insights documentation

---

## ✅ Final Sprint Review – Project Handoff Artifacts

| Artifact | Description |
|-------|-------------|
| EMR Infra Proof | Logs, screenshots, VPC diagram |
| PySpark Scripts | Ingestion, cleansing, aggregation |
| Hive Schemas | Raw, staging, curated tables |
| Airflow DAG | Fully parameterized batch pipeline |
| KPI Outputs | CSV/JSON or visualization dashboards |
| Agile Artifacts | Scrum board, burndown chart, retrospective |

---

## 🎯 Key Outcome
A **production-grade, scalable retail inventory forecasting pipeline** leveraging Big Data best practices, Agile delivery, and cloud-native architecture.

---

> **Author:** Data Engineering Capstone  
> **Tech Stack:** AWS EMR, PySpark, Hive, S3, Airflow  
> **Methodology:** Agile Scrum

---
