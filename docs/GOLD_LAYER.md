# Gold Layer Documentation

## Food Inspection Data Pipeline — Dimensional Model

---

## 1. Overview

The Gold layer implements a **star schema dimensional model** following Kimball methodology. It reads from the unified Silver views and produces dimension tables, a fact table, and a bridge table for the many-to-many inspection–violation relationship.

### Gold Schema Tables

| Table                              | Type       | Rows (approx.) | Description                                    |
|------------------------------------|------------|-----------------|------------------------------------------------|
| `dim_date`                         | Dimension  | ~3,500          | Calendar dimension derived from inspection dates |
| `dim_violation`                    | Dimension  | ~500+           | Distinct violation codes + descriptions by city |
| `dim_establishment`                | Dimension (SCD2) | Varies    | Establishment master with slowly changing dimensions |
| `fact_inspection`                  | Fact       | Matches unified | One row per inspection event                   |
| `bridge_inspection_violation`      | Bridge     | Varies          | Many-to-many link between inspections and violations |

---

## 2. Dimensional Model Design

```
                    ┌──────────────────┐
                    │    dim_date      │
                    │  (date_sk PK)    │
                    └────────┬─────────┘
                             │
┌──────────────────┐    ┌────┴────────────────┐    ┌──────────────────────────────┐
│ dim_establishment│    │  fact_inspection     │    │ bridge_inspection_violation   │
│ (establishment_sk│◄───│  (inspection_sk PK)  │───►│ (bridge_sk PK)               │
│  PK, SCD Type 2) │    │  establishment_sk FK │    │  inspection_sk FK            │
└──────────────────┘    │  date_sk FK          │    │  violation_sk FK             │
                        └─────────────────────┘    └──────────────┬───────────────┘
                                                                  │
                                                   ┌──────────────┴───────────────┐
                                                   │      dim_violation           │
                                                   │  (violation_sk PK)           │
                                                   └──────────────────────────────┘
```

---

## 3. Dimension Tables

### 3.1 `dim_date`

A standard calendar dimension derived from distinct `inspection_date` values.

| Column          | Type    | Description                      |
|-----------------|---------|----------------------------------|
| `date_sk`       | INT     | Surrogate key (YYYYMMDD format)  |
| `full_date`     | DATE    | The actual date                  |
| `day_of_month`  | INT     | Day number (1–31)                |
| `day_of_week`   | INT     | Day of week (1=Sun, 7=Sat)       |
| `day_name`      | STRING  | Full day name (Monday, etc.)     |
| `month_number`  | INT     | Month number (1–12)              |
| `month_name`    | STRING  | Full month name (January, etc.)  |
| `quarter`       | INT     | Quarter (1–4)                    |
| `year`          | INT     | Calendar year                    |
| `is_weekend`    | BOOLEAN | True if Saturday or Sunday       |

**Key design:** The surrogate key uses a computed integer `YYYY*10000 + MM*100 + DD` rather than an auto-increment, making it human-readable and directly joinable without a lookup.

### 3.2 `dim_violation`

Stores distinct violation code + description combinations from both cities.

| Column                  | Type    | Description                              |
|-------------------------|---------|------------------------------------------|
| `violation_sk`          | BIGINT  | Surrogate key (auto-incremented)         |
| `violation_code`        | STRING  | Code (e.g., `7` for Chicago, `*01` for Dallas) |
| `violation_description` | STRING  | Description text                         |
| `source_city`           | STRING  | `Chicago` or `Dallas`                    |

**Key design decisions:**
- `NO_VIOLATION` rows are excluded — they don't represent real violations
- Chicago and Dallas violation codes are stored together, distinguished by `source_city`
- The assignment states "Dallas violation codes and Chicago violation codes don't need to match however, create a dim that stores both sets"

### 3.3 `dim_establishment` (SCD Type 2)

The establishment dimension implements **Slowly Changing Dimension Type 2** to track historical changes.

| Column                  | Type    | Description                              |
|-------------------------|---------|------------------------------------------|
| `establishment_sk`      | BIGINT  | Surrogate key                            |
| `establishment_nk`      | STRING  | Natural key (MD5 hash)                   |
| `establishment_name`    | STRING  | Business name                            |
| `aka_name`              | STRING  | Also Known As name (Chicago only)        |
| `license_number`        | STRING  | License number (Chicago only)            |
| `facility_type`         | STRING  | Type of establishment                    |
| `risk_category`         | STRING  | Risk level (Chicago only)                |
| `address`               | STRING  | Street address                           |
| `city`                  | STRING  | City name                                |
| `state`                 | STRING  | State code                               |
| `zip_code`              | STRING  | ZIP code                                 |
| `latitude`              | DOUBLE  | Geographic latitude                      |
| `longitude`             | DOUBLE  | Geographic longitude                     |
| `source_city`           | STRING  | `Chicago` or `Dallas`                    |
| `effective_start_date`  | DATE    | SCD2 — when this version became active   |
| `effective_end_date`    | DATE    | SCD2 — `9999-12-31` for current records  |
| `is_current`            | BOOLEAN | SCD2 — True for the active version       |

**Natural Key Generation:**
```python
md5(concat_ws("||",
    UPPER(TRIM(establishment_name)),
    UPPER(TRIM(address)),
    TRIM(zip_code),
    source_city
))
```

**SCD2 Implementation:**

The pipeline supports both initial load and incremental merge:

**Initial Load** (when `dim_establishment` is empty):
- All establishments are inserted with `is_current = True`, `effective_end_date = '9999-12-31'`
- Surrogate keys are assigned via `row_number()`

**Incremental Merge** (when records already exist):
1. **Close changed records** — Using Delta `MERGE`, existing current records where tracked attributes have changed are updated: `is_current = False`, `effective_end_date = current_date()`
2. **Insert new versions** — Changed and net-new records are inserted with `is_current = True`, `effective_start_date = current_date()`

**Tracked attributes for change detection:**
- `establishment_name`, `aka_name`, `facility_type`, `risk_category`, `address`, `zip_code`

---

## 4. Fact Table

### 4.1 `fact_inspection`

The central fact table — one row per inspection event.

| Column              | Type    | Description                                |
|---------------------|---------|--------------------------------------------|
| `inspection_sk`     | BIGINT  | Surrogate key                              |
| `establishment_sk`  | BIGINT  | FK → `dim_establishment.establishment_sk`  |
| `date_sk`           | INT     | FK → `dim_date.date_sk`                    |
| `inspection_id`     | STRING  | Business key (native or MD5)               |
| `inspection_type`   | STRING  | Type of inspection                         |
| `inspection_result` | STRING  | Result (Chicago only; NULL for Dallas)     |
| `inspection_score`  | INT     | Score (native for Dallas, derived for Chicago) |
| `violation_count`   | INT     | Pre-computed count of violations           |
| `source_city`       | STRING  | `Chicago` or `Dallas`                      |

**Construction logic:**
1. Generate `establishment_nk` on inspections using same hash logic as `dim_establishment`
2. Compute `date_sk` as `YYYY*10000 + MM*100 + DD`
3. Pre-compute `violation_count` from `vw_violations_unified` (excluding `NO_VIOLATION`)
4. Join to `dim_establishment` (current records only) and `dim_date` for surrogate key lookups
5. Assign `inspection_sk` via `row_number()`

---

## 5. Bridge Table

### 5.1 `bridge_inspection_violation`

Resolves the many-to-many relationship between inspections and violations.

| Column              | Type    | Description                              |
|---------------------|---------|------------------------------------------|
| `bridge_sk`         | BIGINT  | Surrogate key                            |
| `inspection_sk`     | BIGINT  | FK → `fact_inspection.inspection_sk`     |
| `violation_sk`      | BIGINT  | FK → `dim_violation.violation_sk`        |
| `violation_points`  | DOUBLE  | Points assigned (Dallas only)            |
| `violation_detail`  | STRING  | Detailed violation description/comments  |
| `violation_memo`    | STRING  | Inspector memo (Dallas only)             |

**Construction logic:**
1. Filter out `NO_VIOLATION` rows from unified violations
2. Join to `fact_inspection` on `inspection_id` → get `inspection_sk`
3. Join to `dim_violation` on `(violation_code, violation_description, source_city)` → get `violation_sk`
4. Filter out any null `violation_sk` (unmatched violations)
5. Assign `bridge_sk` via `row_number()`

---

## 6. Referential Integrity Checks

The Gold notebook includes post-load integrity verification:

| Check                           | Description                                    | Expected |
|---------------------------------|------------------------------------------------|----------|
| Fact → dim_establishment        | No orphan `establishment_sk` in fact           | 0        |
| Fact → dim_date                 | No orphan `date_sk` in fact                    | 0        |
| Bridge → fact_inspection        | No orphan `inspection_sk` in bridge            | 0        |
| Bridge → dim_violation          | No orphan `violation_sk` in bridge             | 0        |

---

## 7. Key Design Decisions

1. **Bridge table for violations** — A many-to-many relationship exists between inspections and violations. The bridge pattern avoids fan-out issues in the fact table while preserving violation-level detail (points, detail, memo).

2. **Pre-computed violation count** — `violation_count` is stored as a degenerate dimension on the fact table for efficient aggregation without joining through the bridge.

3. **Smart surrogate keys for dim_date** — Using `YYYYMMDD` integer format allows direct computation from dates without a lookup table join during ETL.

4. **SCD2 on dim_establishment** — Tracks changes to establishment attributes over time. This is the assignment-required SCD2 implementation.

5. **Source city as discriminator** — Present on `dim_violation`, `dim_establishment`, and `fact_inspection` to enable city-specific analysis while maintaining a unified model.

6. **Degenerate dimensions** — `inspection_type`, `inspection_result`, and `source_city` are kept directly on the fact table rather than in separate dimensions, as they have low cardinality and are frequently used in queries.
