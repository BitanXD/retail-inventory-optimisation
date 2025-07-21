Agile Sprint – Retail Inventory Forecasting Using PySpark + Hive on EMR
Scrum Theme: Optimize and automate retail inventory forecasting using scalable batch pipelines on EMR & Hive, under an 8-hour Agile Scrum sprint per day (5 total sprints).

Dataset Overview
Parameter
Description
Dataset Files
transactions.csv (2.2M rows), items.csv, cities.json
Format
CSV (for transactions, items), JSON (for cities metadata)
Data Quality
Contains nulls, duplicates, anomalies, and minor outliers
Volume
~2.5M records total (retail domain)


 Sprint 0: Infrastructure & Security Foundation Setup (DevOps Support)
 Sprint Tasks:
Provision EMR Cluster (6 nodes – m5.xlarge, 1 master + 5 core)


Configure:


Subnet, VPC, NAT Gateway


Auto-scaling policy


IAM Role with S3 & CloudWatch full access


Key pair access for SSH


S3 Data Lake Setup:


Buckets: retail-capstone/raw/, staging/, curated/


Enable:


Kerberos authentication


CloudWatch EMR Logging


Hive Metastore + Spark History Server


Daily snapshot on master + checkpointing


   Deliverable: CloudFormation template or Terraform script (optional), cluster logs screenshot, network diagram

 Sprint 1: Raw Zone Ingestion & Schema Registration
 Sprint Tasks:
Upload raw files to s3://retail-capstone/raw/


Read CSVs using PySpark with explicit schema and delimiter handling


Register Hive external tables on raw S3 paths:


transactions_raw, items_raw, cities_raw


Perform schema validation using Spark DataFrame functions


   Deliverable: Hive table DDLs, Spark script, schema report

 Sprint 2: Data Quality and Cleansing Pipeline – PySpark
 Sprint Tasks:
Identify:


Nulls, Duplicates (on InvoiceID, ItemID), Inconsistent Pricing


Outliers in Quantity, UnitPrice via percentiles


Cleanse Data:


Fill missing cities from JSON


Remove invalid/zero-priced transactions


Add columns:


WeekNumber, Quarter, IsHighValueOrder


Save to s3://retail-capstone/staging/ in partitioned Parquet format (Year-Month)


Register transactions_cleaned Hive table


   Deliverable: Cleansing PySpark script, data quality report, staging table DDL

 Sprint 3: Business Logic & Metrics Aggregation – Feature Engineering
 Sprint Tasks:
Metrics Calculated:


Revenue by Month, City, SKU


Average Quantity per City


Top 5 selling items per region


Save aggregates to curated/analytics/


Create inventory_metrics Hive table


JSON city metadata joined to transactions


   Deliverable: Aggregation script, metrics snapshot, Hive analytics schema

 Sprint 4: Airflow-based Workflow Orchestration
 Sprint Tasks:
Setup Airflow DAG with steps:


Ingest → Cleanse → Aggregate → Verify Output


Use:


S3KeySensor to detect new uploads


SparkSubmitOperator for Spark jobs


Custom PythonOperator for metric validation


Set retries, SLA, email-on-failure


Schedule: Daily at 1 AM UTC


   Deliverable: DAG script, screenshots, task logs, Airflow UI proof

 Sprint 5: Business Insight Extraction and Visualization
 Sprint Tasks:
KPIs:


Monthly Revenue Trends


Region-wise Item Shortage


Top 10 profitable SKUs


Export as JSON/CSV


Optionally use:


Superset, Quicksight, or Excel


Prepare demo dashboards for stakeholders


   Deliverable: KPI sheet, visualization screenshots, insights document

 Final Sprint Review – Project Handoff Artifacts
Artifact
Details
   EMR Infra Logs + Diagram
Cluster setup proof, VPC/network setup diagram
   PySpark Scripts
Ingestion, Cleansing, Aggregation
   Hive Schema
Raw, Staging, Curated tables
   Airflow DAG
Scheduled batch job (with parameters)
   KPI Dashboard (optional)
CSV/JSON Export or visualization link
   Agile Artifacts
Scrum Board, Burndown Chart, Sprint Retrospective


