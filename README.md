# Food Inspection Data Pipeline

## Chicago & Dallas - Medallion Architecture on Databricks

A full data engineering pipeline that ingests, profiles, cleans, models, and visualizes food inspection data from Chicago and Dallas using the Medallion Architecture (Raw → Bronze → Silver → Gold) on Databricks with Delta Lake.

---

## Table of Contents

- [Project Overview](#project-overview)
- [Architecture](#architecture)
- [Repository Structure](#repository-structure)
- [Prerequisites](#prerequisites)
- [Setup Instructions](#setup-instructions)
- [Pipeline Execution Order](#pipeline-execution-order)
- [Data Sources](#data-sources)
- [Dimensional Model](#dimensional-model)
- [DQX Validation Rules](#dqx-validation-rules)
- [BI Dashboard](#bi-dashboard)
- [Documentation](#documentation)
- [Team](#team)

---

## Project Overview

This project analyzes food inspection data from two U.S. cities with fundamentally different data schemas:

- **Chicago**: 17 columns, violations stored as a pipe-delimited blob, native `Inspection_ID`
- **Dallas**: 90+ columns, violations in 25 wide-format slot groups, no native unique ID

The pipeline unifies both datasets into a star schema dimensional model suitable for BI reporting and analytics.

---

## Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                     DATA SOURCES                                 │
│  Chicago CSV (Volumes)              Dallas CSV (Volumes)         │
└────────────┬──────────────────────────────────┬──────────────────┘
             │                                  │
             ▼                                  ▼
┌────────────────────────┐     ┌────────────────────────────┐
│   RAW LAYER            │     │   RAW LAYER                │
│   raw_zone.chicago     │     │   raw_zone.dallas          │
│   (exact CSV copy)     │     │   (exact CSV copy)         │
└────────────┬───────────┘     └──────────────┬─────────────┘
             │                                │
             ▼                                ▼
┌────────────────────────┐     ┌────────────────────────────┐
│   BRONZE LAYER         │     │   BRONZE LAYER             │
│   bronze.bronze_chicago│     │   bronze.bronze_dallas     │
│   (+ audit columns)    │     │   (+ audit columns)        │
└────────────┬───────────┘     └──────────────┬─────────────┘
             │                                │
             ▼                                ▼
┌────────────────────────┐     ┌────────────────────────────┐
│   SILVER LAYER         │     │   SILVER LAYER             │
│   chicago_inspections  │     │   dallas_inspections       │
│   chicago_violations   │     │   dallas_violations        │
│   quarantine_chicago   │     │   quarantine_dallas        │
└────────────┬───────────┘     └──────────────┬─────────────┘
             │                                │
             └───────────┬────────────────────┘
                         ▼
              ┌─────────────────────┐
              │   UNIFIED VIEWS     │
              │   vw_inspections_   │
              │     unified         │
              │   vw_violations_    │
              │     unified         │
              └──────────┬──────────┘
                         ▼
              ┌─────────────────────────────────┐
              │         GOLD LAYER               │
              │   dim_date                       │
              │   dim_violation                  │
              │   dim_establishment (SCD Type 2) │
              │   fact_inspection                │
              │   bridge_inspection_violation    │
              └─────────────────────────────────┘
                         │
                         ▼
              ┌─────────────────────┐
              │   BI DASHBOARD      │
              │   (Power BI /       │
              │    Tableau)         │
              └─────────────────────┘
```

---

## Repository Structure

```
├── _common_helpers.ipynb              # Shared utility functions (logging, rollback, validation)
├── 00_metadata_and_environment_setup.ipynb  # Catalog, schema, and metadata table creation
├── 01_raw_dallas.ipynb                # Raw ingestion — Dallas CSV → Delta
├── 02_raw_chicago.ipynb               # Raw ingestion — Chicago CSV → Delta
├── 03_bronze_chicago.ipynb            # Bronze — add audit columns to Chicago
├── 04_bronze_dallas.ipynb             # Bronze — add audit columns to Dallas
├── 05_DQX_profiling.ipynb             # Data quality profiling (both cities)
├── 06a_Silver_Chicago.ipynb           # Silver — clean, validate, parse Chicago
├── 06b_Silver_Dallas.ipynb            # Silver — clean, validate, unpivot Dallas
├── 06c_Silver_Unified.ipynb           # Silver — create unified views
├── 08_Gold.ipynb                      # Gold — dimensional model (dims, fact, bridge)
├── assets/
│   ├── Food_Inspection_Dimensional_Model_Final_Project.png           # Dimensional Modeling ER Diagram
│   ├── Full_Project_Mapping.xlsx           # Mapping Document
├── docs/
│   ├── RAW_BRONZE_LAYER.md            # Raw & Bronze layer documentation
│   ├── SILVER_LAYER.md                # Silver layer documentation
│   ├── GOLD_LAYER.md                  # Gold layer documentation
│   ├── DQX_PROFILING.md              # DQX profiling documentation
│   └── project_report.pdf             # Unified project report (LaTeX-generated)
└── README.md                          # This file
```

---

## Prerequisites

| Tool              | Version / Details                              |
|-------------------|------------------------------------------------|
| Databricks        | Runtime 14.x+ with Unity Catalog enabled       |
| Catalog           | `food_inspection` (or custom via widget)       |
| Volumes           | `raw_zone.volume_raw` with CSV files uploaded  |
| Python packages   | `databricks-labs-dqx` (installed in notebook)  |
| BI Tool           | Power BI Desktop or Tableau Desktop            |
| Data Modeling     | ER Studio / Navicat (for dimensional model design) |

---

## Setup Instructions

### Step 1: Create the Databricks Catalog

```sql
CREATE CATALOG IF NOT EXISTS food_inspection;
```

### Step 2: Upload Source Data

1. Download the Chicago food inspection CSV from the [Chicago Data Portal](https://data.cityofchicago.org/)
2. Download the Dallas food inspection CSV from the [Dallas OpenData Portal](https://www.dallasopendata.com/)
3. Create the volume:
   ```sql
   CREATE VOLUME IF NOT EXISTS food_inspection.raw_zone.volume_raw;
   ```
4. Upload both CSV files to `/Volumes/food_inspection/raw_zone/volume_raw/`:
   - Rename Chicago file to `Chicago.csv`
   - Rename Dallas file to `Dallas.csv`

### Step 3: Import Notebooks to Databricks Workspace

1. Clone this repository or upload all `.ipynb` files to your Databricks workspace
2. Ensure all notebooks are in the **same directory** (required for `%run ./_common_helpers`)

### Step 4: Run the Pipeline

Execute notebooks in order (see [Pipeline Execution Order](#pipeline-execution-order) below).

### Step 5: Connect BI Tool

Connect Power BI or Tableau to the `food_inspection.gold` schema tables:
- `dim_date`, `dim_violation`, `dim_establishment`, `fact_inspection`, `bridge_inspection_violation`

---

## Pipeline Execution Order

Run notebooks in this exact sequence. Each notebook is idempotent and can be re-run safely.

| Order | Notebook                              | Dependencies                    |
|-------|---------------------------------------|---------------------------------|
| 1     | `00_metadata_and_environment_setup`   | None                            |
| 2     | `01_raw_dallas`                       | Step 1 + CSV in Volume          |
| 3     | `02_raw_chicago`                      | Step 1 + CSV in Volume          |
| 4     | `03_bronze_chicago`                   | Step 3                          |
| 5     | `04_bronze_dallas`                    | Step 2                          |
| 6     | `05_DQX_profiling`                    | Steps 4–5                       |
| 7     | `06a_Silver_Chicago`                  | Step 4                          |
| 8     | `06b_Silver_Dallas`                   | Step 5                          |
| 9     | `06c_Silver_Unified`                  | Steps 7–8                       |
| 10    | `08_Gold`                             | Step 9                          |

**Note:** `_common_helpers` is not run directly — it is loaded via `%run` by other notebooks.

---

## Data Sources

| City    | Records  | Source                                                         |
|---------|----------|----------------------------------------------------------------|
| Chicago | ~250K+   | [Chicago Data Portal — Food Inspections](https://data.cityofchicago.org/) |
| Dallas  | ~40K+    | [Dallas OpenData — Restaurant Inspections](https://www.dallasopendata.com/) |

---

## Dimensional Model

**Star Schema** with a bridge table for the many-to-many inspection–violation relationship.

| Table                          | Type               | Key                          |
|--------------------------------|--------------------|------------------------------|
| `dim_date`                     | Dimension          | `date_sk` (YYYYMMDD INT)    |
| `dim_violation`                | Dimension          | `violation_sk` (BIGINT)     |
| `dim_establishment`            | Dimension (SCD2)   | `establishment_sk` (BIGINT) |
| `fact_inspection`              | Fact               | `inspection_sk` (BIGINT)    |
| `bridge_inspection_violation`  | Bridge             | `bridge_sk` (BIGINT)        |

---

## DQX Validation Rules

| ID     | Rule                                            | Cities   |
|--------|-------------------------------------------------|----------|
| VR-001 | Restaurant/DBA name not null                    | Both     |
| VR-002 | Inspection date not null                         | Both     |
| VR-003 | Inspection type not null                         | Both     |
| VR-004 | Valid zip code format (5-digit or ZIP+4)         | Both     |
| VR-005 | Violation score ≤ 100                            | Dallas   |
| VR-006 | Inspection results not null                      | Chicago  |
| VR-007 | ≥1 unique violation per inspection               | Both     |
| VR-008 | Score ≥ 90 → max 3 violations                   | Dallas   |
| VR-009 | PASS cannot have Urgent/Critical violations      | Chicago  |

Failed records are quarantined (not dropped) in `silver.quarantine_chicago` and `silver.quarantine_dallas`.

---

## BI Dashboard

The dashboard is built in Power BI / Tableau connected to the Gold layer and includes:

- Inspection results by result type, inspection type, risk category, and facility type
- Violation analysis by code, description, and frequency
- Business-level inspection history (DBA, AKA, License)
- Geographic analysis by location and zip code
- Detailed inspection report with violations and inspector comments

---

## Documentation

| Document                  | Description                                      |
|---------------------------|--------------------------------------------------|
| `RAW_BRONZE_LAYER.md`    | Raw & Bronze layer implementation details        |
| `SILVER_LAYER.md`        | Silver layer cleaning, validation, and parsing   |
| `GOLD_LAYER.md`          | Gold layer dimensional model and SCD2            |
| `DQX_PROFILING.md`       | Data quality profiling findings and rules        |
| `project_report.pdf`     | Comprehensive LaTeX-generated project report     |

---

## Team

- Team Members: Preksha Praveen | Riyanshi Kedia | Pradyumna Reddy Cherla
- Course: DAMG7370 Designing Advanced Data Architectures for Business Intelligence | Northeastern University

---

## License

Academic use only. Northeastern University coursework.
