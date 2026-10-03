
# Retail_q / Data Engineering Project

This project is an end-to-end data engineering initiative aimed at building a modern Lakehouse architecture (Medallion Architecture). The project utilizes **Databricks Delta Live Tables (DLT)** for downstream transformation and data quality enforcement.

## 🏗 Architecture Overview

The project is built on Databricks + Unity Catalog, designed with a layered architecture:

- **Source:** Data from business systems like PostgreSQL (Neon) and Salesforce.
- **Bronze (Raw):** Stores unmodified source data in Delta Lake format.
- **Silver (Cleaned):** Cleanses, normalizes, and enforces data quality rules on Bronze data using DLT.
- **Gold (Marts):** Business-level aggregated wide tables (Planned).

## 🔄 Hybrid Ingestion Strategy

This project employs a **hybrid ingestion strategy**, leveraging two different approaches based on the source system's capabilities:

| Source System | Ingestion Method | CDC / SCD Support | Reasoning |
| :--- | :--- | :--- | :--- |
| **PostgreSQL (Neon)** | **Lakeflow Connect** | Native SCD1 & SCD2 | Structured database with native CDC support. Managed connector simplifies pipeline maintenance. |
| **Salesforce** | **Auto Loader (CloudFiles)** | None (Manual) | CSV export lacks native CDC. Requires manual column normalization and downstream SCD handling. |
| **Blob Storage** | **Auto Loader (CloudFiles)** | None (Manual) | Flat file ingestion with schema evolution handled by Auto Loader. |

### 🔍 Comparison with Reference Architecture

The reference implementation relies solely on **Lakeflow Connect** for Salesforce ingestion. This project uses Auto Loader for Salesforce to demonstrate manual ingestion and downstream SCD handling.

| Aspect | Reference Architecture | This Project |
| :--- | :--- | :--- |
| **Salesforce Ingestion** | Lakeflow Connect (Managed) | Auto Loader (Manual CSV Export) |
| **SCD for `account`** | **SCD Type 2** (Full history) | Flat / Append-only (No history tracking) |
| **SCD for `opportunity`** | **SCD Type 1** (Overwrite) | Flat / Append-only (No history tracking) |
| **Column Availability** | Rich schema with system fields (`Id`, `IsDeleted`, etc.) | Limited to exported CSV columns |
| **Handling Missing Joins** | Native `AccountId` present in Opportunity | Manually mocked `account_name` to enable JOINs |

**Key Takeaway:**
- **Lakeflow Connect** is ideal for production-grade, low-maintenance pipelines where CDC and SCD are required out-of-the-box.
- **Auto Loader** provides maximum flexibility and control but requires manual schema management, data mocking, and downstream SCD implementation (planned via DLT `AUTO CDC`).

## 📁 Project Structure

### Storage Structure (Bronze Layer)
```text
retail_q (Unity Catalog)
├── postgres_bronze (Schema from Lakeflow Connect)
│   ├── inventory
│   └── product_catalog
└── bronze (Schema from Auto Loader)
    └── source (Volume)  <-- Underlying storage structure
        ├── raw_data/                 <-- Business source data (CSV files)
        │   ├── blob_transactions/
        │   ├── salesforce_acc/
        │   └── salesforce_oppo/
        ├── _schemas/                 <-- Auto Loader schema inference records (System files)
        │   ├── blob_transactions/
        │   ├── salesforce_acc/
        │   └── salesforce_oppo/
        └── _checkpoints/             <-- Auto Loader incremental load checkpoints (System files)
            ├── blob_transactions/
            ├── salesforce_acc/
            └── salesforce_oppo/
```

### DLT Pipeline Structure (Silver Layer)
```text
retail_q.silver
├── account.py            <-- Salesforce Account cleaning
├── inventory.py          <-- PostgreSQL Inventory cleaning
├── opportunity.py        <-- Salesforce Opportunity cleaning
├── product_catalog.py    <-- PostgreSQL Product Catalog cleaning
└── transactions.py       <-- Blob Transactions cleaning & type casting
```

## 🚀 Data Ingestion Strategies

### 1. PostgreSQL Ingestion (Lakeflow Connect)
- **Goal:** Sync data from PostgreSQL (e.g., `inventory`, `product_catalog`) into the Bronze layer.
- **Approach:** Databricks **Lakeflow Connect**.
- **SCD Handling:** Lakeflow Connect natively supports **SCD Type 1 (overwrite)** and **SCD Type 2 (historical tracking)**. This significantly simplifies CDC pipelines without writing complex `MERGE` logic manually. For `product_catalog`, the system-generated CDC columns (`__START_AT`, `__END_AT`) are preserved and used to derive `is_active` in the Silver layer.

### 2. Salesforce & Blob Ingestion (Auto Loader)
*Completed!*
Utilizing Databricks **Auto Loader (CloudFiles)** combined with PySpark streaming to incrementally load CSV files from Unity Catalog Volumes into Delta tables.

**Key Implementation Details & Code Snippet:**
- **Incremental Loading:** Configured independent `checkpointLocation` and `schemaLocation` for each stream.
- **Column Name Normalization:** Delta Lake rejects column names with spaces or special characters. Regex is used to clean names upon ingestion.
- **Data Mocking:** Missing relational fields (e.g., `account_name`) are populated via code to ensure downstream joining logic.

#### A. Column Name Normalization & Incremental Load
```python
import re

# 1. Define Auto Loader stream
df_acc = (spark.readStream
  .format("cloudFiles")
  .option("cloudFiles.format", "csv")
  .option("header", "true")
  .option("cloudFiles.schemaLocation", "dbfs:/Volumes/retail_q/bronze/source/_schemas/salesforce_acc")
  .load("dbfs:/Volumes/retail_q/bronze/source/raw_data/salesforce_acc/") 
)

# 2. Clean column names (handling spaces, slashes, etc.)
for col_name in df_acc.columns:
    clean_name = re.sub(r'[ /-]', '_', col_name).lower()
    clean_name = re.sub(r'_+', '_', clean_name).strip('_')
    df_acc = df_acc.withColumnRenamed(col_name, clean_name)

# 3. Write to Bronze Table with Checkpoint
(df_acc.writeStream
  .option("checkpointLocation", "dbfs:/Volumes/retail_q/bronze/source/_checkpoints/salesforce_acc")
  .trigger(availableNow=True) 
  .toTable("retail_q.bronze.salesforce_acc")
)
```

#### B. Data Mocking: Injecting Missing `account_name`
Since the source Opportunity CSV lacks relational keys, we simulate the Salesforce UI behavior by assigning specific `account_name` values based on row numbers to enable downstream JOINs.

```python
from pyspark.sql.functions import col, when, monotonically_increasing_id, row_number
from pyspark.sql.window import Window

# 1. Read the existing Bronze table
df_oppo = spark.read.table("retail_q.bronze.salesforce_oppo")

# 2. Generate row numbers to mimic Salesforce UI selection
windowSpec = Window.orderBy(monotonically_increasing_id())
df_with_row = df_oppo.withColumn("row_num", row_number().over(windowSpec))

# 3. Assign account_name based on specific row ranges (mocking the UI batch update)
df_oppo_mocked = df_with_row.withColumn(
    "account_name",
    when((col("row_num") >= 3) & (col("row_num") <= 7), "Fresh Stores Pvt Ltd 4")   
    .when((col("row_num") >= 27) & (col("row_num") <= 31), "Global Traders Pvt Ltd 5") 
    .otherwise("Elite Wholesale Pvt Ltd 19")  
).drop("row_num")

# 4. Overwrite the Bronze table with the new mocked column
# Note: mergeSchema is enabled to allow adding new columns to the existing Delta table
(df_oppo_mocked.write
 .mode("overwrite")
 .option("mergeSchema", "true") 
 .saveAsTable("retail_q.bronze.salesforce_oppo")
)
```

## 🥈 Silver Layer: Transformation (Delta Live Tables)

All Silver transformations are implemented in **DLT (Python)**. The pipeline reads from Bronze tables (`spark.readStream`) and applies:

1. **Column Standardization:** Trimming, upper-casing (`F.upper`, `F.trim`), and title-casing (`F.initcap`) for consistency.
2. **Type Casting:** Converting string columns to proper numeric types (e.g., `amount` -> double, `quantity` -> int).
3. **Business Logic Enrichment:**
   - Deriving `inventory_status` (`LOW_STOCK` / `HEALTHY`).
   - Deriving `deal_size` (`ENTERPRISE` / `MID_MARKET` / `SMALL`).
   - Deriving `product_segment` (`PREMIUM` / `MID_RANGE` / `BUDGET`).
   - Deriving `gross_amount` in transactions.
   - Parsing non-standard timestamps (`dd-MMM-yyyy hh.mm.ss a`).

### Data Quality Expectations

Data quality is enforced at the Silver layer using DLT Expectations. This ensures only reliable data reaches downstream consumers.

#### `account.py`
- **Drop:** `non-null account_name` (Ensures every account has a name as the primary identifier).
- **Warn:** `valid type` (Validates that the account type belongs to a predefined set).

#### `inventory.py`
- **Drop:** `non-null inventory_id`, `non-null product_id`, `non-null store_id` (Primary/foreign keys must exist).
- **Warn:** `valid stock_quantity` (Flags records where `stock_quantity <= 0`).

#### `opportunity.py`
- **Drop:** `non-null opportunity_name` (Primary key must exist).
- **Warn:** `valid amount` (Flags negative amounts).
- **Warn:** `valid stage` (Ensures the stage matches the expected sales pipeline stages).

#### `product_catalog.py`
- **Drop:** `valid_product_id`, `valid_product_name`, `valid_launch_date` (Non-null and non-empty core fields).
- **Warn:** `valid_category`, `valid_price`, `valid_supplier` (Business logic checks).

#### `transactions.py`
- **Drop:** `non-null transaction_id`, `non-null product_id` (Keys must exist).
- **Warn:** `valid quantity`, `valid selling_price`, `valid discount_amount`, `valid payment_mode` (Validates numeric ranges and allowed categorical values).

**Execution Order & Validation Logic:**

The `@dp.expect` rules are applied **after** all transformation logic (`select`, `cast`, `when/otherwise`) is executed, but **before** the data is written to the Silver tables.

This means:
- **Not** testing the raw Bronze data (which may contain strings or unvalidated values).
- **Not** testing the already-materialized Silver table.
- **Testing the in-flight, transformed DataFrame** — the exact output of the `return` statement.

**If the pipeline runs successfully:**
- Data has passed all `@dp.expect` checks (or been dropped by `@dp.expect_or_drop`).
- The resulting Silver table contains only records that satisfy the defined data quality rules.
- Any records failing `@dp.expect` (not `_or_drop`) are still written to the table but flagged in the pipeline metrics.

#### DLT vs. dbt: Data Quality Validation Timing

A key architectural difference between this project (using DLT) and a traditional dbt-based approach is **when** data quality rules are executed.

| Aspect | dbt (Test After Materialization) | DLT (Validate Before Write) |
| :--- | :--- | :--- |
| **Execution Timing** | Runs **after** the model is materialized (table/view is already created). | Runs **before** the data is written to the target table. |
| **Mechanism** | SQL-based tests (e.g., `not_null`, `unique`, custom SQL) query the built table. | Python decorators (`@dp.expect`, `@dp.expect_or_drop`) validate the in-flight DataFrame. |
| **Handling Failures** | Raises an error or warning **after** the table has been populated. | Drops the record (`_or_drop`) or flags it (`expect`) **before** it reaches the table. |
| **Impact on Downstream** | Downstream consumers might briefly see bad data if tests fail after the run. | Downstream consumers **never** see dropped records; they only see validated data. |
| **Cost** | Requires a separate query execution against the materialized table. | Piggybacks on the existing transformation execution (more efficient). |

**Why This Matters:**
- **dbt** is a "post-mortem" check: "Did we build a bad table? Let's find out and alert."
- **DLT** is a "gatekeeper" check: "Should this record even enter the table? If not, drop it now."

This project adopts the **DLT** approach to ensure the Silver layer is always clean and trustworthy from the moment data lands.

### Data Quality Strategy: Drop vs. Quarantine

A critical design decision in the Silver layer is how to handle records that fail data quality expectations.

**Option 1: `@dp.expect_or_drop` (Hard Drop)**
- **Behavior:** Invalid records are permanently discarded before writing to the Silver table.
- **Pros:** The Silver table is guaranteed to be 100% clean.
- **Cons:** The dropped records are "lost forever" with no trace for debugging.
- **Use Case:** Core master data where a missing key renders the record completely useless (e.g., missing `product_id`).

**Option 2: Quarantine Table (Soft Drop)**
- **Behavior:** Invalid records are filtered into a separate `_quarantine` table instead of being discarded.
- **Pros:** 
  - The main Silver table remains clean for downstream consumers.
  - Bad records are preserved for auditing, debugging, and potential re-processing.
- **Cons:** Requires maintaining an additional DLT table.
- **Use Case:** Business-critical data where you want to investigate upstream issues without blocking the pipeline.

**Implementation Example: Quarantine for Missing Product Names**

```python
# Main Silver Table: Only clean records (using expect_or_drop)
@dp.table(name="retail_q.silver.product_catalog")
@dp.expect_or_drop("valid_product_name", "product_name IS NOT NULL AND LENGTH(TRIM(product_name)) > 0")
def product_catalog():
    return spark.readStream.table("retail_q.postgres_bronze.product_catalog")...

# Quarantine Table: Only invalid records (manually filtered)
@dp.table(name="retail_q.silver.product_catalog_quarantine")
def product_catalog_quarantine():
    return spark.readStream.table("retail_q.postgres_bronze.product_catalog") \
        .filter(F.col("product_name").isNull() | (F.length(F.trim(F.col("product_name"))) == 0))
```

**Why This Matters:**
- **Data Observability:** You can always answer the question "What happened to the dropped records?"
- **Pipeline Resilience:** A single bad record does not block the entire pipeline, but it is also not silently ignored.
- **Upstream Accountability:** Quarantine tables provide concrete evidence to share with upstream teams for root-cause analysis.

## 🛠 Development Workflow: Preview Before DLT

To ensure transformation logic is correct before deploying to the DLT pipeline, a **batch preview** approach is strictly followed. This avoids wasting cluster resources and time on iterative DLT runs.

**Standard Procedure:**
1. Write the transformation logic using `spark.read.table(...)` (Batch mode) in a standard Databricks Notebook.
2. Preview the output using `display()` to validate data quality, type casting, and business logic.
3. Once the logic is verified, change `spark.read` to `spark.readStream` and encapsulate the code inside the `@dp.table` decorator for the DLT pipeline.

**Example: Previewing Transactions Transformation**
```python
from pyspark.sql import functions as F

# 1. Use standard batch read instead of readStream for previewing
source_df = spark.read.table("retail_q.bronze.blob_transactions")

# 2. Apply the exact transformation logic intended for the DLT pipeline
preview_df = source_df.select(
    F.col("transaction_id"),
    F.col("opportunity_name"),
    F.col("product_id"),
    F.col("store_id"),
    F.col("quantity").cast("int").alias("quantity"),
    F.col("selling_price").cast("double").alias("selling_price"),
    (F.col("quantity").cast("int") * F.col("selling_price").cast("double")).alias("gross_amount"),
    F.col("discount_amount").cast("double").alias("discount_amount"),
    F.to_timestamp(F.col("transaction_timestamp"), "dd-MMM-yyyy hh.mm.ss a").alias("transaction_timestamp"),
    F.col("payment_mode"),
    F.col("sales_channel")
)

# 3. Display the result to verify the output
display(preview_df.limit(10))
```

## ⚠️ Lessons Learned

1. **Delta Lake Column Name Restrictions:**
   - `Delta Lake` by default rejects column names containing spaces, `/`, or other illegal characters.
   - **Solution:** Immediately clean and rename columns after reading the stream in Auto Loader.

2. **Auto Loader vs. SCD:**
   - Auto Loader only performs **"faithful ingestion"**; it allows duplicate IDs to enter the Bronze layer by default.
   - **SCD logic is not handled here.** It should be processed in the Silver layer using **DLT's `AUTO CDC`** or `MERGE INTO`.

3. **Volume File Movement:**
   - Moving files in Unity Catalog Volumes is strict; `mv` often fails due to hidden placeholder files.
   - **Solution:** Use `dbutils.fs.mv` or a combination of `cp -r` and `rm -rf`.

4. **Timestamp Parsing in DLT:**
   - Before applying `F.to_timestamp`, always preview the actual string format using a batch `spark.read` to avoid `NULL` propagation.

## 📝 TODO & Architecture Decisions

- [x] **Architecture Decision:** This project uses **DLT (Delta Live Tables)** instead of dbt for transformation and data quality enforcement.
- [x] Implement Bronze ingestion using **Lakeflow Connect** for PostgreSQL and **Auto Loader** for Salesforce/Blob.
- [x] Implement Silver layer DLT pipelines with data quality rules.
- [ ] Implement **DLT `AUTO CDC`** for SCD Type 2 handling on `product_catalog`.
- [ ] Build the Gold layer data models using DLT to generate `dim_products` and `fct_order_items`.


## 📚 Reference Architecture

The reference implementation for this project is based on the following tutorial:

- **YouTube:** [Retail Data Engineering Project in Databricks](https://www.youtube.com/watch?v=QHwszePV3GY&list=PLShYG8gCK1IU&index=2&t=4629s)