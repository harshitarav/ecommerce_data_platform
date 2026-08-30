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
- **Amazon RDS (MySQL)** – Incremental database ingestion
- **External CSV Files** – Batch file ingestion
- **REST API** – API ingestion
- **Python** – Ingestion ETL implementation
- **AWS Lambda** – Serverless ingestion
- **Amazon S3** – Data Lake storage for Bronze, Silver, Gold
- **AWS Glue** – ETL processing
- **PySpark** – Distributed data transformation
- **AWS Glue Data Catalog** – Metadata and schema management
- **AWS Glue Crawlers** – Schema discovery
- **SQL** – Database and warehouse processing
- **Snowflake** – Analytical data warehouse
- **Snowflake SQL & Stored Procedures** – Incremental loading and MERGE operations
- **Apache Airflow** – Batch workflow orchestration
- **Docker** – Lambda packaging and dependency builds
- **Git & GitHub** – Version control
- **GitHub Actions** – CI/CD deployment

## Data Pipeline
The pipeline follows a **scheduled batch-processing architecture with incremental processing**. 
Data is ingested from multiple sources, stored in the Bronze layer, transformed and validated 
through the Silver layer, and modeled into business-ready datasets in the Gold layer before 
being loaded into Snowflake.
### Bronze Layer — Raw Data
The Bronze layer acts as the raw landing layer in **Amazon S3**, preserving source data with minimal transformation for reliable downstream processing and recovery.

**Ingestion:**
- Amazon RDS - Database source
- External CSV files - File-based source
- REST API responses - Shipment/vendor data source

**Processing:**
- Initial full loads for historical data
- Subsequent runs use Incremental ingestion for newly added or updated source records to avoid unnecessary reprocessing.
- RDS incremental extraction using watermark-based processing
- External CSV files are checked using file-level hash comparison to identify unchanged files.
- API responses are retained in their raw JSON structure
- Source data is organized by source type and dataset

**Validation:**
- Bronze focuses on preserving source data rather than applying business rules or aggressive data cleansing.
- AWS Glue Crawlers discover schemas and update the **Glue Data Catalog**.
  
### Silver Layer — Cleaned & Validated Data
The Silver layer transforms raw Bronze data into clean, standardized and trusted datasets using **AWS Glue and PySpark**.

**Transformations:**
- Data type and schema standardization
- Column normalization and data formatting
- NULL handling 
- Duplicate detection and deduplication
- Flattening and structuring nested API data
- Business-rule based transformations
- Incremental inserts and updates
- Soft-delete handling
- Composite-key support
- Hash-bucket based partition pruning for efficient incremental processing
- Partition-aware incremental processing
- Change manifest generation to identify affected records for downstream processing

**Validation:**
- Primary-key uniqueness checks
- Mandatory-field NULL checks
- Data type and format validation
- Length and value-range validation
- Timestamp and status consistency checks
- Domain/business-rule validation
- Validation failures are classified by severity and can stop downstream processing.

### Gold Layer — Business-Ready Data
The Gold layer transforms validated Silver data into business-oriented dimensional models and analytical marts optimized for reporting and analytics.

**Transformations:**
- Dimension table creation
- Fact table creation
- Business-level aggregations
- Daily and summary analytical marts
- Sales, payment, inventory, shipment and tracking analytics
- Derived business metrics such as delivery duration, delivery delays and late-delivery indicators
- Incremental processing using Silver change manifests and Gold watermarks
- Incremental UPSERT processing for affected business entities and based on Silver change tracking

**Validation:**
- Referential integrity between fact and dimension tables
- Customer, product and seller relationship validation
- Payment-to-customer and payment-to-order validation
- Aggregation consistency checks
- Inventory available/reserved/safety-stock reconciliation
- Critical validation failures prevent the pipeline from progressing.

The resulting Gold datasets are loaded into Snowflake for analytical consumption.

## ⭐ Star Schema Design
The Gold layer follows a **star-schema-based dimensional model** designed for analytical querying and reporting.
### Fact Tables

Fact tables capture measurable business events at a defined grain:

- **`fact_sales`** - One row per order item sold; combines order, product, seller, shipment and review information.
- **`fact_payments`** - One row per order; contains aggregated payment information such as payment type, installments and payment value.
- **`fact_shipment_delivery`** - One row per vendor shipment / tracking number; contains shipment status, carrier, delivery indicators and delay metrics.
- **`fact_tracking_event`** - One row per tracking event; captures shipment status, event timestamp and location.

### Dimension Tables

Dimension tables provide descriptive business context for analytical queries:

- **`dim_customer`** — Customer attributes such as location, customer identifiers and loyalty points.
- **`dim_product`** — Product attributes including category, dimensions, weight and product metadata.
- **`dim_seller`** — Seller location and seller attributes.
- **`dim_inventory`** — Product inventory by warehouse, including available, reserved and safety stock.

### Analytical Marts

Additional Gold-level aggregate tables are created for commonly required analytical queries:

- **`fact_sales_daily`** — Daily sales metrics including total orders, items sold, sales, average order value, freight and late deliveries.
- **`inventory_summary`** — Product-level inventory summary by warehouse.
- **`shipment_delivery_daily`** — Daily shipment metrics by carrier, including delivered, in-transit and delayed shipments.


### Key Modeling Decisions

- **Fact grain is explicitly defined** to prevent double-counting and maintain consistent aggregations.
- Business KPIs such as `total_order_value`, `delivery_days`, `delivery_delay_days`, `late_delivery_flag` and `review_category` are derived in the Gold layer.
- Dimensions and facts are maintained separately to provide reusable business context for analytical queries.
- Gold tables support **incremental INSERT, UPDATE and DELETE processing** using business keys and change tracking.
- Gold data is partitioned using deterministic hash buckets for efficient incremental processing.
