# E-Commerce Data Engineering Platform

## 📌 Project Overview
E-commerce businesses generate data across multiple operational systems and external sources. These data's is often stored in different formats and systems, making it
difficult to provide a consistent and reliable dataset for analytics.

This project builds an end-to-end **Cloud Data Platform using AWS** that brings these different data sources together and transforms them into reliable,
analytics-ready datasets for the downstream analytics.

This platform ingests data from various sources including rds, api and external files and processes it in AWS Glue (PySpark)
through a **Medallion Architecture** (Bronze → Silver → Gold). The final Gold layer
organizes the data into **dimensions, facts, and analytical marts** that can
be consumed by Snowflake for downstream analytics.

The primary goal of the project is not just to move data from one system
to another, but to design a pipeline that can handle the challenges that
occur in a continuously changing data environment — including **incremental
data ingestion, updates, deletes, duplicate records, schema changes, data
quality issues, and efficient downstream processing**.


## Architecture
