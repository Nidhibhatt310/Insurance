# Real-Time Insurance Data Platform

This project demonstrates a scalable data engineering solution built on Databricks for processing real-time and batch insurance data.

## Problem Statement

Insurance companies deal with high-volume, real-time data from multiple systems such as claims, policies, and risk events. This data is often fragmented, inconsistent, and difficult to process efficiently.

This project solves these challenges by building a unified data platform that enables:

- Real-time data ingestion using Kafka
- Batch data ingestion for master data
- Data standardization and transformation
- Flexible data modeling using Raw Data Vault (RDV)
- Analytics-ready star schema for reporting (Power BI)

## Architecture Overview

Kafka (Streaming) + Batch Data → Bronze Layer → Standardization → RDV → Star Schema → BI

## Tech Stack

- Databricks (Structured Streaming, Delta Lake)
- PySpark
- Kafka
- Databricks Asset Bundles
- Azure Data Lake (ADLS Gen2)