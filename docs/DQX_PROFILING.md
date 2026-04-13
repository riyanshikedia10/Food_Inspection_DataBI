# DQX Profiling Documentation

## Food Inspection Data Pipeline — Data Quality Assessment

---

## 1. Overview

Data quality profiling is the first analytical step after ingestion. It runs against **Bronze layer** tables to understand structure, completeness, validity, uniqueness, and consistency of both datasets before any transformations are applied.

### Tools Used

| Tool                    | Purpose                                           |
|-------------------------|---------------------------------------------------|
| `databricks-labs-dqx`   | Automated profiling and rule generation           |
| Custom PySpark functions | Deep-dive profiling with results logged to metadata |

### Output

All profiling metrics are logged to `metadata.dqx_profile_results` with dimensions: `table_name`, `city`, `column_name`, `profiling_dimension`, `metric_name`, `metric_value`.

---

## 2. DQX Automated Profiling

### 2.1 Setup

```python
%pip install databricks-labs-dqx
from databricks.labs.dqx.profiler.profiler import DQProfiler
from databricks.labs.dqx.profiler.generator import DQGenerator
from databricks.sdk import WorkspaceClient

ws = WorkspaceClient()
profiler = DQProfiler(ws)
generator = DQGenerator(ws)
```

### 2.2 Chicago DQX Profile

```python
chi_df = spark.table(f"{catalog_name}.{bronze_schema}.bronze_chicago")
chi_summary, chi_profiles = profiler.profile(chi_df)
chi_rules = generator.generate_dq_rules(chi_profiles)
```

The DQX profiler generates a summary of each column's distribution, null rates, and type information. The generator then produces suggested DQ rules based on the profile.

### 2.3 Dallas DQX Profile

```python
dal_df = spark.table(f"{catalog_name}.{bronze_schema}.bronze_dallas")
dal_summary, dal_profiles = profiler.profile(dal_df)
dal_rules = generator.generate_dq_rules(dal_profiles)
```

---

## 3. Chicago Profiling — Detailed Findings

### 3.1 Dataset Overview

| Metric              | Value                    |
|---------------------|--------------------------|
| Total rows          | Varies by download       |
| Data columns        | 17 (excluding audit)     |
| Audit columns       | 4 (`_raw_ingestion_timestamp`, `_source_city`, `_bronze_ingestion_timestamp`, `_source_file`) |

### 3.2 Completeness Analysis

Per-column null and empty string counts are computed and logged. Key findings:

| Column              | Key Finding                                      |
|---------------------|--------------------------------------------------|
| `Violations`        | Many inspections have NULL/empty violation text   |
| `Location`          | Some records missing geocoding                    |
| `AKA_Name`          | Significant null rate (optional field)            |
| `License_num`       | Some records have value `0` (treated as missing)  |

### 3.3 Uniqueness Analysis

- **`Inspection_ID`**: Checked for true uniqueness — duplicate IDs indicate data quality issues
- **`License_num = 0`**: Counted as suspicious (placeholder rather than real license)

### 3.4 Cardinality Analysis

Distinct value counts for categorical columns:

| Column              | Purpose                                          |
|---------------------|--------------------------------------------------|
| `Facility_Type`     | High cardinality — many establishment types      |
| `Risk`              | 3 levels: Risk 1 (High), Risk 2 (Medium), Risk 3 (Low) |
| `Inspection_Type`   | Multiple types (Canvass, Complaint, Re-Inspection, etc.) |
| `Results`           | Pass, Fail, Pass w/ Conditions, No Entry, etc.   |
| `City`              | Should be primarily "CHICAGO" — others are anomalies |
| `State`             | Should be "IL" — non-IL records are out-of-state |

### 3.5 Validity Checks

**Zip Code Format**: Must match `^\d{5}(-\d{4})?$` (5-digit or ZIP+4). Invalid formats and nulls are counted separately.

**Inspection Date**: Null check — all records should have a date.

**Latitude/Longitude Range**: Chicago coordinates should fall approximately within lat 41.0–43.0, lon -89.0 to -86.0. Records outside this range are counted.

**Zip Length Distribution**: Distribution of zip code string lengths to identify truncated or padded values.

### 3.6 Consistency Checks

**Out-of-state records**: Records where `State != 'IL'` are counted. These may be legitimate (suburban areas) or data entry errors.

**PASS with Urgent/Critical violations** (VR-009): Per assignment rules, a PASS result should not coexist with Urgent or Critical violation terms. These records are counted and will be quarantined in Silver.

**Missing violations**: Inspections with NULL or empty violation blobs — these receive a default `NO_VIOLATION` row in Silver.

### 3.7 Violation Structure Analysis

Chicago violations are pipe-delimited with format `{code}. {description} - Comments: {detail}`. Profiling validates:

- Pipe delimiter consistency
- Violations per inspection (min, max, avg distribution)
- Date range of inspections

---

## 4. Dallas Profiling — Detailed Findings

### 4.1 Dataset Overview

| Metric                | Value                              |
|-----------------------|------------------------------------|
| Total rows            | Varies by download                 |
| Core columns          | ~15 (non-violation)                |
| Violation columns     | 75 (25 slots × 3: Description, Points, Detail + Memo) |
| Total columns         | ~90+                               |

### 4.2 Completeness Analysis

Core columns are profiled for null/empty rates. Violation columns are analyzed separately due to their sparse nature (most rows use only a few of the 25 slots).

### 4.3 Uniqueness Analysis

**Dallas has no native unique ID.** Key combination analysis:

| Key Combination                              | Duplicate Count |
|----------------------------------------------|-----------------|
| `name + date + zip_code`                     | Many duplicates |
| `name + date + address`                      | Fewer           |
| `name + date + address + type`               | Even fewer      |
| `name + date + address + type + score`       | Minimal after dedup |
| After full dedup + 4-col key                 | Near zero       |

**Decision**: Use MD5 hash of `name + date + address + type + score` in Silver, with row-number tiebreaker for residual collisions.

### 4.4 Inspection Score Analysis

| Metric   | Value         |
|----------|---------------|
| Min      | Negative (clamped to 0 in Silver) |
| Max      | Up to 100+    |
| Avg      | ~85–90        |
| Stddev   | ~15–20        |

**Score distribution buckets**: Negative, 0–59, 60–69, 70–79, 80–89, 90–100

### 4.5 Validity Checks

**Zip Code Format**: Same 5-digit validation as Chicago. Non-DFW zip prefixes (outside 750–755, 760–762) are identified.

**Inspection Date**: Null check performed.

**Lat_Long_Location**: Checked for parseability against pattern `^(\([-0-9.]+,\s*[-0-9.]+\))$`.

### 4.6 Consistency Checks

**VR-008: Score ≥ 90 with > 3 violations**: Per assignment rules, high-scoring inspections should not have more than 3 violations. Violating records are counted and will be quarantined in Silver.

**Urgent/Critical terms**: Total count of violation entries containing "urgent" or "critical" across all 25 description columns.

**Address component coverage**: Completeness of `street_number`, `street_direction`, `street_name`, `street_type`, `street_unit`.

**Column 20 memo typo**: Known double-space issue in `Violation_Memo___20` column name — documented for defensive handling in Silver.

### 4.7 Violation Structure Analysis

**Wide format**: 25 slots × 4 columns (Description, Points, Detail, Memo).

**Violation density**: Distribution of how many of the 25 slots are actually populated per inspection.

**Shortcode extraction**: Descriptions typically start with `*XX` shortcode (e.g., `*01`, `*12`).

| Metric                    | Finding                          |
|---------------------------|----------------------------------|
| Total non-null descriptions | Counted                        |
| With `*XX` shortcode      | ~99%+ coverage                   |
| Without shortcode          | ~38 orphan descriptions          |
| Distinct shortcodes        | ~25–30                           |
| Distinct regulatory codes  | ~30+ (228.xxx format)            |

**Shortcode → Regulatory code mapping**: Paired from same violation slot to build the hierarchy between `*XX` shortcodes and `228.xxx` regulatory codes.

---

## 5. Cross-City Comparison

A comprehensive comparison table documents schema differences:

| Attribute         | Chicago                      | Dallas                          |
|-------------------|------------------------------|---------------------------------|
| Unique ID         | `Inspection_ID` (native)     | NONE — generate MD5 hash        |
| Restaurant Name   | `DBA_Name`                   | `Restaurant_Name`               |
| AKA Name          | `AKA_Name`                   | Not available                   |
| License           | `License_num`                | Not available                   |
| Facility Type     | `Facility_Type` (granular)   | Not available                   |
| Risk Category     | `Risk` (Risk 1/2/3)         | Not available                   |
| Address           | Single `Address` field       | 5 street components             |
| City/State        | From data columns            | Not available — default DALLAS/TX |
| Zip               | `Zip` (int/string)           | `Zip_Code` (string)             |
| Inspection Score  | Not available — derive from Results | `Inspection_Score` (native) |
| Violations        | Pipe-delimited blob          | 25 column groups (wide)         |
| Violation Points  | Not available                | `Violation_Points_1..25`        |
| Lat/Long          | Separate columns             | Combined `(lat,long)` string    |

---

## 6. DQX Validation Rules Summary

These rules are implemented as quarantine logic in the Silver layer based on profiling findings:

| Rule ID | Description                                         | Applies To | Source            |
|---------|-----------------------------------------------------|------------|-------------------|
| VR-001  | Restaurant/DBA name cannot be null/empty            | Both       | Assignment        |
| VR-002  | Inspection date cannot be null                       | Both       | Assignment        |
| VR-003  | Inspection type cannot be null/empty                 | Both       | Assignment        |
| VR-004  | Zip code must be valid 5-digit or ZIP+4 format       | Both       | Assignment        |
| VR-005  | Violation score cannot exceed 100 (Dallas)           | Dallas     | Assignment        |
| VR-006  | Chicago inspection results cannot be null            | Chicago    | Assignment        |
| VR-007  | Every inspection must have ≥1 unique violation       | Both       | Assignment (Silver dedup) |
| VR-008  | Score ≥ 90 → max 3 violations (Dallas)              | Dallas     | Assignment        |
| VR-009  | PASS cannot have Urgent/Critical violations          | Chicago    | Assignment        |

---

## 7. Profiling Metadata Storage

All metrics are stored in `metadata.dqx_profile_results`:

```sql
CREATE TABLE IF NOT EXISTS metadata.dqx_profile_results (
    table_name          STRING,
    city                STRING,
    column_name         STRING,
    profiling_dimension STRING,   -- completeness, validity, uniqueness, consistency
    metric_name         STRING,
    metric_value        STRING,
    profile_time        TIMESTAMP,
    created_date        DATE
)
```

This enables historical tracking of data quality trends across pipeline runs.
