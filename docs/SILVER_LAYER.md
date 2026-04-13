# Silver Layer Documentation

## Food Inspection Data Pipeline — Chicago & Dallas

---

## 1. Overview

The Silver layer is the data quality and standardization engine of the pipeline. It reads from Bronze, applies DQX validation rules, cleans and normalizes data, and produces city-specific inspection and violation tables plus unified views.

| Notebook              | Purpose                                           |
|-----------------------|---------------------------------------------------|
| `06a_Silver_Chicago`  | Clean, validate, and parse Chicago inspections     |
| `06b_Silver_Dallas`   | Clean, validate, deduplicate, and unpivot Dallas   |
| `06c_Silver_Unified`  | Create unified views across both cities            |

### Silver Tables Produced

| Table/View                              | Description                                      |
|-----------------------------------------|--------------------------------------------------|
| `silver.chicago_inspections`            | Cleaned Chicago inspection records               |
| `silver.chicago_violations`             | Parsed violation rows from pipe-delimited blob   |
| `silver.dallas_inspections`             | Cleaned Dallas inspection records                |
| `silver.dallas_violations`              | Unpivoted violation rows from 25 column groups   |
| `silver.quarantine_chicago`             | Rejected Chicago rows with quarantine reasons    |
| `silver.quarantine_dallas`              | Rejected Dallas rows with quarantine reasons     |
| `silver.vw_inspections_unified` (VIEW)  | UNION ALL of both city inspections               |
| `silver.vw_violations_unified` (VIEW)   | UNION ALL of both city violations                |

---

## 2. Chicago Silver Pipeline (`06a_Silver_Chicago`)

### 2.1 Column Renaming

All columns are renamed from PascalCase to snake_case for consistency:

| Bronze Column       | Silver Column          |
|---------------------|------------------------|
| `Inspection_ID`     | `inspection_id`        |
| `DBA_Name`          | `dba_name`             |
| `AKA_Name`          | `aka_name`             |
| `License_num`       | `license_number`       |
| `Facility_Type`     | `facility_type`        |
| `Risk`              | `risk`                 |
| `Address`           | `address`              |
| `City`              | `city`                 |
| `State`             | `state`                |
| `Zip`               | `zip_code`             |
| `Inspection_Date`   | `inspection_date_raw` → `inspection_date` |
| `Inspection_Type`   | `inspection_type`      |
| `Results`           | `results`              |
| `Violations`        | `violations_raw`       |
| `Latitude`          | `latitude`             |
| `Longitude`         | `longitude`            |

### 2.2 Type Casting

| Column              | Cast                        |
|---------------------|-----------------------------|
| `inspection_id`     | STRING                      |
| `license_number`    | STRING                      |
| `zip_code`          | STRING (trimmed)            |
| `inspection_date`   | DATE (`MM/dd/yyyy` format)  |
| `latitude`          | DOUBLE                      |
| `longitude`         | DOUBLE                      |

### 2.3 Data Cleansing

1. **String trimming** — All string columns are trimmed of leading/trailing whitespace.
2. **License number = 0 → NULL** — Zero-value licenses are treated as missing data.
3. **Out-of-state flagging** — Records where `state != 'IL'` are flagged with `is_out_of_area = True` (kept, not dropped).

### 2.4 Derived Fields

**Violation Score** — Per assignment requirements, the score is derived from the `results` column:

| Results              | Derived Score |
|----------------------|---------------|
| `Pass`               | 90            |
| `Pass w/ Conditions` | 80            |
| `Fail`               | 70            |
| `No Entry`           | 0             |
| All other types      | NULL          |

### 2.5 DQX Validation Rules (Quarantine)

Each rule tags a `_quarantine_reason` column. Records with any reason are routed to `quarantine_chicago`; clean records proceed to `chicago_inspections`.

| Rule ID | Rule                                          | Quarantine Reason            |
|---------|-----------------------------------------------|------------------------------|
| VR-001  | `dba_name` cannot be null/empty               | `dba_name_null`              |
| VR-002  | `inspection_date` cannot be null              | `inspection_date_null`       |
| VR-003  | `inspection_type` cannot be null/empty        | `inspection_type_null`       |
| VR-004  | `zip_code` must match `^\d{5}(-\d{4})?$`     | `zip_invalid`                |
| VR-006  | `results` cannot be null/empty (Chicago-only) | `results_null`               |
| VR-009  | PASS cannot have Urgent/Critical violations   | `pass_with_urgent_critical`  |

Multiple violations are semicolon-concatenated (e.g., `"zip_invalid; results_null"`).

### 2.6 Violation Parsing

Chicago stores violations as a **pipe-delimited blob** in a single column. Each violation follows the pattern:
```
{code}. {description} - Comments: {detail}
```

**Parsing logic:**
1. Split on `|` delimiter
2. Trim each item
3. Extract `violation_code` via regex: `^(\d+)\.`
4. Extract `violation_description` via regex: `^\d+\.\s*([^-]+?)(?:\s*-\s*Comments:|$)`
5. Extract `violation_detail` via regex: `-\s*Comments:\s*(.*)$`
6. `violation_points` and `violation_memo` are set to NULL (not available in Chicago)
7. Filter out rows where `violation_code` is empty

**Deduplication:** Violations are deduplicated on `(inspection_id, violation_code, violation_description)` per assignment requirement ("every inspection must have at least 1 unique violation and duplicate violations to be loaded as distinct").

**Zero-violation handling:** Inspections with no parsed violations receive a default row with `violation_code = 'NO_VIOLATION'` and `violation_description = 'No violation recorded'`.

---

## 3. Dallas Silver Pipeline (`06b_Silver_Dallas`)

### 3.1 Full Duplicate Removal

Dallas has no native unique ID. The first step drops **fully duplicate rows** (all non-audit columns match):
```python
dal_core_cols = [c for c in dallas_bronze.columns if not c.startswith("_")]
dallas = dallas_bronze.dropDuplicates(dal_core_cols)
```

### 3.2 Column Renaming & Type Casting

Similar to Chicago — PascalCase → snake_case. Key casts:

| Column              | Cast                        |
|---------------------|-----------------------------|
| `inspection_date`   | DATE (`MM/dd/yyyy` format)  |
| `inspection_score`  | INT                         |
| `zip_code`          | STRING (trimmed)            |

### 3.3 Data Cleansing

1. **String trimming** — All core string columns trimmed.
2. **Negative scores clamped to 0** — Negative inspection scores are set to 0.
3. **Lat/Long parsing** — Combined `(lat, long)` string is parsed into separate `latitude` and `longitude` DOUBLE columns via regex.
4. **Out-of-range coordinate flagging** — Coordinates outside Dallas bounds (lat 32–34) are flagged with `is_coords_out_of_range = True`.
5. **Out-of-DFW zip code flagging** — Zip codes not matching DFW prefixes (750–755, 760–762) are flagged with `is_out_of_area = True`.

### 3.4 Synthetic Inspection ID (MD5 Hash)

Since Dallas has no native unique ID, a composite key is generated:
```python
dallas = dallas.withColumn("inspection_id",
    md5(concat_ws("||",
        coalesce(trim(col("restaurant_name")), lit("")),
        coalesce(col("inspection_date").cast("string"), lit("")),
        coalesce(trim(col("street_address")), lit("")),
        coalesce(trim(col("inspection_type")), lit("")),
        coalesce(col("inspection_score").cast("string"), lit(""))
    ))
)
```

**Residual collision tie-breaking:** After hashing, any remaining collisions are resolved by keeping the earliest `_bronze_ingestion_timestamp` via `row_number()`.

### 3.5 Violation Count Computation

The number of non-null violation descriptions across 25 slots is counted per row for use in VR-008 validation:
```python
density_expr = sum(
    when(col(f"`{c}`").isNotNull() & (trim(col(f"`{c}`").cast("string")) != ""), 1).otherwise(0)
    for c in violation_desc_cols
)
dallas = dallas.withColumn("n_violations", density_expr)
```

### 3.6 DQX Validation Rules (Quarantine)

| Rule ID | Rule                                              | Quarantine Reason                   |
|---------|----------------------------------------------------|-------------------------------------|
| VR-001  | `restaurant_name` cannot be null/empty             | `restaurant_name_null`              |
| VR-002  | `inspection_date` cannot be null                   | `inspection_date_null`              |
| VR-003  | `inspection_type` cannot be null/empty             | `inspection_type_null`              |
| VR-004  | `zip_code` must match `^\d{5}(-\d{4})?$`          | `zip_invalid`                       |
| VR-005  | `inspection_score` cannot exceed 100               | `score_over_100`                    |
| VR-008  | Score ≥ 90 cannot have > 3 violations              | `vr008_score90_over3_violations`    |

### 3.7 Violation Unpivoting

Dallas stores violations in a **wide format** with 25 slot groups, each having:
- `Violation_Description___N`
- `Violation_Points___N`
- `Violation_Detail___N`
- `Violation_Memo___N`

These are unpivoted into a single long-format table. Known issues handled:
- **Column 20 double-space typo**: The memo column for slot 20 has a known naming inconsistency (`Violation__Memo___20` instead of `Violation_Memo___20`), handled defensively.

**Violation code extraction:**
1. Extract `*XX` shortcode from description: `^(\*?\s?\d+)`
2. Normalize: remove spaces (`* 21` → `*21`), zero-pad single digits (`*6` → `*06`)
3. Fallback: descriptions without a shortcode get `*00`
4. Extract `228.xxx` regulatory code from `violation_detail`

**Zero-violation handling:** Same as Chicago — default `NO_VIOLATION` row inserted.

---

## 4. Unified Silver Views (`06c_Silver_Unified`)

### 4.1 `vw_inspections_unified`

A `UNION ALL` view that normalizes column names between Chicago and Dallas:

| Unified Column        | Chicago Source                | Dallas Source                    |
|-----------------------|-------------------------------|----------------------------------|
| `establishment_name`  | `dba_name`                    | `restaurant_name`                |
| `aka_name`            | `aka_name`                    | NULL                             |
| `license_number`      | `license_number`              | NULL                             |
| `facility_type`       | `facility_type`               | `'Food Establishment'` (default) |
| `risk_category`       | `risk`                        | NULL                             |
| `address`             | `address`                     | `street_address`                 |
| `city`                | `city`                        | `'DALLAS'` (default)             |
| `state`               | `state`                       | `'TX'` (default)                 |
| `inspection_result`   | `results`                     | NULL                             |
| `inspection_score`    | `violation_score` (derived)   | `inspection_score` (native)      |
| `source_city`         | `'Chicago'`                   | `'Dallas'`                       |

### 4.2 `vw_violations_unified`

A simpler `UNION ALL` — both cities already share the same violation schema after Silver processing:
- `inspection_id`, `violation_code`, `violation_description`, `violation_detail`, `violation_memo`, `violation_points`, `source_city`

### 4.3 Summary Logging

Combined totals are logged to `metadata.dqx_execution_log` and a full summary is printed showing counts for both cities.

---

## 5. Schema Standardization Summary

The assignment requires standardizing schemas between Chicago and Dallas. Key decisions:

| Attribute         | Chicago Approach              | Dallas Approach                 | Unified Handling            |
|-------------------|-------------------------------|---------------------------------|-----------------------------|
| Unique ID         | Native `Inspection_ID`        | MD5 hash of 5 fields            | Both stored as `inspection_id` (STRING) |
| Violations        | Pipe-delimited blob → parsed  | 25 column groups → unpivoted    | Same schema: code + desc + detail + memo + points |
| Violation Codes   | Numeric (e.g., `7`, `18`)     | `*XX` shortcode (e.g., `*01`)   | Both stored in `dim_violation`, distinguished by `source_city` |
| Inspection Score  | Derived from Results          | Native column                   | Both as `inspection_score` (INT) |
| Address           | Single field                  | 5 components → `street_address` | Both as `address` (STRING)  |
| City/State        | From data                     | Defaulted to DALLAS/TX          | Both present                |
