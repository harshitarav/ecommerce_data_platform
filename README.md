# E-Commerce Data Engineering Platform

## 📌 Project Overview
E-commerce businesses generate data across multiple operational systems and external sources. These data's is often stored in different formats and systems, making it difficult to provide a consistent and reliable dataset for analytics.

This project builds an end-to-end **Cloud Data Platform using AWS** that brings these different data sources together and transforms them into reliable, analytics-ready datasets for the downstream analytics.

This platform ingests data from various sources including rds, api and external files and processes it in AWS Glue (PySpark)
through a **Medallion Architecture** (Bronze → Silver → Gold). The final Gold layer organizes the data into **dimensions, facts, and analytical marts** that can be consumed by Snowflake for downstream analytics.

The primary goal of the project is not just to move data from one system to another, but to design a pipeline that can handle the challenges that occur in a continuously changing data environment — including **incremental data ingestion, updates, deletes, duplicate records, schema changes, data quality issues, and efficient downstream processing**. 

The pipeline follows a scheduled batch-processing approach, where data is collected and processed in discrete pipeline runs orchestrated by Apache Airflow. Within each batch run, incremental processing is used to identify and process only newly arrived or changed records. Watermarks and AWS Glue Job Bookmarks are used to track previously processed data, while change manifests are generated to propagate affected records to downstream layers. The initial execution performs a full load to establish the baseline dataset, while subsequent batch runs process only the incremental changes, including inserts, updates, and deletes. This approach reduces unnecessary data processing, improves pipeline efficiency, and allows the platform to handle continuously changing source data while maintaining reliable downstream datasets.


## Architecture
<img width="1536" height="1024" alt="image" src="https://github.com/user-attachments/assets/72a85511-aefa-476b-bd90-04de3feee986" />

## 🛠️ Tools & Technologies
- **Amazon RDS (MySQL)** — Incremental database ingestion
- **External CSV Files** — Batch file ingestion
- **REST API** — API ingestion
- **Python** — Ingestion ETL implementation
- **AWS Lambda** — Serverless ingestion
- **Amazon S3** — Data Lake storage for Bronze, Silver, Gold
- **AWS Glue** — ETL processing
- **PySpark** — Distributed data transformation
- **AWS Glue Data Catalog** — Metadata and schema management
- **AWS Glue Crawlers** — Schema discovery
- **SQL** — Database and warehouse processing
- **Snowflake** — Analytical data warehouse
- **Snowflake SQL & Stored Procedures** — Incremental loading and MERGE operations
- **Apache Airflow** — Batch workflow orchestration
- **Docker** — Lambda packaging and dependency builds
- **Git & GitHub** — Version control
- **GitHub Actions** — CI/CD deployment




