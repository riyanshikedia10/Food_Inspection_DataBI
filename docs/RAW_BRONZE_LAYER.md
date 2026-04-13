# Raw & Bronze Layer Documentation

## Food Inspection Data Pipeline — Chicago & Dallas

---

## 1. Overview

The Raw and Bronze layers form the ingestion backbone of the Medallion Architecture pipeline. Raw captures an exact copy of source CSV files as Delta tables, while Bronze adds audit/lineage metadata without altering any data values.

| Layer  | Purpose                          | Transformations               | Tables Created                    |
|--------|----------------------------------|-------------------------------|-----------------------------------|
| Raw    | Exact copy of source CSVs        | Column name sanitization only | `raw_zone.dallas`, `raw_zone.chicago` |
| Bronze | Lineage-enriched staging         | Audit columns appended        | `bronze.bronze_dallas`, `bronze.bronze_chicago` |

---

## 2. Environment Setup (`00_metadata_and_environment_setup`)

### 2.1 Dynamic Catalog

All notebooks use a Databricks widget `catalog_name` (default: `food_inspection`) so the pipeline is portable across environments.

```python
dbutils.widgets.text("catalog_name", "food_inspection")
catalog_name = dbutils.widgets.get("catalog_name")
```

### 2.2 Schema Creation

Five schemas are created under the catalog:

| Schema     | Purpose                                      |
|------------|----------------------------------------------|
| `raw_zone` | Untouched source data as Delta tables         |
| `bronze`   | Audit-enriched copies of raw data             |
| `silver`   | Cleaned, validated, and standardized data     |
| `gold`     | Dimensional model (facts, dims, bridge)       |
| `metadata` | Pipeline control, execution logs, DQX logs    |

### 2.3 Metadata Tables

Three metadata tables are created:

**`metadata.pipeline_control`** — Master registry of source datasets:
- `table_name`, `file_name`, `active_flag`, `city`, `created_date`, `modified_date`
- Seeded with two rows: `dallas` / `Dallas.csv` and `chicago` / `Chicago.csv`

**`metadata.execution_log`** — Audit trail for every pipeline step:
- `table_name`, `city`, `execution_time`, `status`, `source_row_count`, `target_row_count`, `file_location`, `created_date`

**`metadata.dqx_execution_log`** — DQX validation metrics per Silver run:
- `table_name`, `city`, `execution_time`, `total_records`, `passed_records`, `failed_records`, `created_date`

### 2.4 Common Helpers (`_common_helpers`)

Shared utilities loaded via `%run ./_common_helpers` in every notebook:

| Function               | Purpose                                                    |
|------------------------|------------------------------------------------------------|
| `log_execution()`      | Append a row to `metadata.execution_log`                   |
| `log_dqx()`            | Append a row to `metadata.dqx_execution_log`               |
| `rollback_table()`     | Delta time-travel rollback to previous version             |
| `get_previous_version()` | Find the version number immediately before current        |
| `validate_row_count()` | Assertion-style source vs. target count check              |

---

## 3. Raw Layer

### 3.1 Purpose

Ingest CSV files from Databricks Volumes into Delta tables with **zero data transformations**. The only modification is sanitizing column names to comply with Delta Lake naming requirements.

### 3.2 Source Files

| File         | Source Path                                                     | Description                                |
|--------------|-----------------------------------------------------------------|--------------------------------------------|
| `Dallas.csv` | `/Volumes/food_inspection/raw_zone/volume_raw/Dallas.csv`       | Dallas food establishment inspections      |
| `Chicago.csv`| `/Volumes/food_inspection/raw_zone/volume_raw/Chicago.csv`      | Chicago food inspections (pipe-delimited violations) |

### 3.3 Pipeline Steps (identical for both cities)

**Step 1 — Read CSV from Volume**
```python
df = spark.read \
    .option("header", "true") \
    .option("inferSchema", "true") \
    .option("multiLine", "true") \
    .option("escape", '"') \
    .csv(source_path)
df = df.withColumn("_raw_ingestion_timestamp", current_timestamp())
```
- `inferSchema=true` lets Spark auto-detect types
- `multiLine=true` + `escape='"'` handles embedded newlines and quoted fields
- `_raw_ingestion_timestamp` records when the row was ingested

**Step 2 — Column Name Sanitization**

Delta Lake does not allow spaces or special characters in column names. The following replacements are applied:

| Character | Replacement |
|-----------|-------------|
| Space     | `_`         |
| Hyphen `-`| `_`         |
| Asterisk `*` | (removed) |
| Slash `/` | `_`         |
| Parens `()` | (removed) |
| Hash `#`  | `num`       |

**Step 3 — Write to Delta Table**
```python
df.write.format("delta") \
    .mode("overwrite") \
    .option("overwriteSchema", "true") \
    .saveAsTable(raw_table)
```
- `overwrite` mode ensures idempotent re-runs
- `overwriteSchema=true` allows schema evolution across runs

**Step 4 — Row Count Validation**

Source count (from DataFrame) is compared to target count (read-back from Delta). Mismatches set `status = "FAILED"`.

**Step 5 — Execution Logging**

A row is appended to `metadata.execution_log` with source/target counts and status.

**Step 6 — Fail-Fast**

If validation failed, the notebook raises an `Exception` to halt downstream processing.

### 3.4 Raw Layer Output

| Table                         | Columns (Dallas) | Columns (Chicago) |
|-------------------------------|-------------------|--------------------|
| `raw_zone.dallas`             | 78 (25 violation slot groups × 3 + core) + 1 audit | Varies by source |
| `raw_zone.chicago`            | 17 source columns + 1 audit | — |

---

## 4. Bronze Layer

### 4.1 Purpose

Read from Raw Delta tables and add **audit/lineage columns** without modifying any data values. This creates a clear separation between "as-ingested" and "ready-for-cleaning" data.

### 4.2 Pipeline Steps

**Step 1 — Read Raw Delta Table**
```python
df = spark.read.table(f"{catalog_name}.{raw_schema}.chicago")
```

**Step 2 — Add Audit Columns**

Three columns are appended:

| Column                       | Value                    | Purpose                    |
|------------------------------|--------------------------|----------------------------|
| `_source_city`               | `"Chicago"` or `"Dallas"` | Identifies data origin     |
| `_bronze_ingestion_timestamp`| `current_timestamp()`    | When Bronze was written    |
| `_source_file`               | `"Chicago.csv"` etc.     | Traces back to source file |

**Step 3 — Write to Bronze Delta Table**

Same overwrite pattern as Raw.

**Step 4–5 — Validation & Logging**

Identical to Raw: row count validation, execution log entry.

**Step 6 — Rollback on Failure**

If validation fails, `rollback_table()` from `_common_helpers` restores the previous Delta version before raising an exception. This prevents partial/corrupt data from persisting.

```python
if status == "FAILED":
    rollback_table(f"{catalog_name}.{bronze_schema}.bronze_chicago")
    raise Exception("Row count mismatch for Bronze Chicago. Check execution_log.")
```

### 4.3 Bronze Layer Output

| Table                       | Source                  | Additional Columns |
|-----------------------------|-------------------------|--------------------|
| `bronze.bronze_chicago`     | `raw_zone.chicago`      | `_source_city`, `_bronze_ingestion_timestamp`, `_source_file` |
| `bronze.bronze_dallas`      | `raw_zone.dallas`       | `_source_city`, `_bronze_ingestion_timestamp`, `_source_file` |

---

## 5. Data Flow Diagram

```
CSV Files (Volumes)
    │
    ▼
┌──────────────────────┐
│     RAW LAYER        │  Column name cleanup + _raw_ingestion_timestamp
│  raw_zone.dallas     │
│  raw_zone.chicago    │
└──────────┬───────────┘
           │
           ▼
┌──────────────────────┐
│    BRONZE LAYER      │  + _source_city, _bronze_ingestion_timestamp, _source_file
│  bronze.bronze_dallas│
│  bronze.bronze_chicago│
└──────────┬───────────┘
           │
           ▼
    Silver Layer (see SILVER_LAYER.md)
```

---

## 6. Key Design Decisions

1. **Overwrite mode** — Both Raw and Bronze use `mode("overwrite")` for full-refresh idempotency. This simplifies re-runs but means historical loads require Delta time-travel.

2. **Schema inference at Raw** — `inferSchema=true` means types are auto-detected. This is acceptable for a two-source pipeline but would need explicit schemas at scale.

3. **Audit columns prefixed with `_`** — The underscore convention makes it easy to filter out metadata columns during downstream processing (e.g., `[c for c in df.columns if not c.startswith("_")]`).

4. **Rollback on failure** — Bronze notebooks roll back to the previous Delta version on validation failure, not to version 0. This preserves prior successful loads.

5. **Execution logging on both success and failure** — Every run produces an audit trail regardless of outcome, enabling debugging and compliance reporting.
