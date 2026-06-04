# Marathos - Ultra-Marathon Data Platform

A school project in Big Data & Cloud coure built for **Marathos**, a global company hosting ultra-marathon events worldwide. The goal is to build a data platform and pipeline to support data-driven decisions for business stakeholders.

---

## Video presentation
*Follow link below to youtube to watch presentationvideo*

[![Klicka här för att se presentationen](https://github.com/LisaYllander92/databricks-marathos-lab/blob/main/marathos_logo.png?raw=true)](https://youtu.be/OIPmX9rVIHo)

---

## Tech Stack

- **Databricks** — data platform and pipeline orchestration
- **PySpark & Spark SQL** — data transformation and processing
- **Delta Lake** — storage layer for all tables
- **Plotly** — dashboard visualizations
- **Git & GitHub** — version control

---

## Project Structure

```
marathos_<fname>_<lname>
├── dimensional_modeling/    # Star schema built in dbdiagram
├── explorations/            # EDA notebooks and Verify Genie
├── transformations/
│   ├── bronze/              # Raw ingestion pipelines
│   ├── silver/              # Cleaning and OBT transformation
│   └── gold/                # Dimensional model and marts
└── utils/
    └── utils.py             # Shared utility functions
```

---

## Architecture

This project follows the **medallion architecture**:

### Bronze
Raw data ingested as streaming tables from CSV files stored in Unity Catalog volumes:
- `raw_marathos` — main ultra-marathon dataset
- `stockholm_trail_classic_2024` — LLM-generated synthetic marathon dataset
- `country_codes` — LLM-generated table for athlete country codes

### Silver
A cleaned **One Big Table (OBT)** — `marathos_obt` — produced by unioning the bronze datasets and applying the following transformations:
- Snake_case column renaming
- Event/performance unit validation (km/mi/mile → h, h → km, d removed)
- Event type classification (distance / time)
- Date parsing with `try_to_date` for messy date formats
- Performance conversion to seconds (`performance_seconds`) and km (`performance_km`)
- Average speed validation (2.0–20.8 km/h based on world record)
- Age validation (18–100 years at time of event)
- Age category normalization (F prefix → W)
- Null handling with `fill_unknown` and `drop_null_rows` utility functions
- Hash-based ID generation with `xxhash64` (`event_id`, `result_id`)
- Inner join with country codes to filter invalid country values

### Gold
A dimensional model built on top of silver:

```
fct_results         → result_id, event_id, athlete_id, performance_seconds, performance_km, average_speed, club, age_category
dim_event           → event_id, event_name, event_type, event_year, start_date, end_date, distance, number_finishers
dim_athlete         → athlete_id, gender, birth_year, country_name, country_code
dim_date            → date, year, month, month_name, quarter, day, day_of_week, day_name
```

**Marts:**
- `mart_distance_events` — distance-based events, countries with 100+ results
- `mart_time_events` — time-based events, countries with 100+ results
- `mart_overview` — all events combined, used for the overview dashboard

---

## Data Sources

| Dataset | Description | Source |
|---|---|---|
| Ultra-marathon dataset | ~7.4M results from ultra-marathon events worldwide | [Kaggle](https://www.kaggle.com/datasets/aiaiaidavid/the-big-dataset-of-ultra-marathon-running) |
| Stockholm Trail Classic 2024 | 84 synthetic runners for a new marathon event | LLM-generated |
| Country codes | IOC 3-letter country codes mapped to full country names | LLM-generated |

> **Note:** The dataset contains ultra-marathon and multi-stage events only. Standard 42.195km marathon distances are not present in the data.

---

## Known Data Quality Issues

| Issue | Location | Handling |
|---|---|---|
| Impossible performance times (e.g. `0:00:00 h`) | Silver | Filtered out with `performance_seconds > 0` |
| Malformed average speed values (e.g. `18:00:00`) | Silver | Handled with `try_cast`, filtered to 2.0–20.8 km/h |
| Invalid dates (e.g. `31.04.2018`) | Silver | Handled with `try_to_date`, nulls filtered out |
| Mixed age category prefixes (F/W) | Silver | Normalized to W prefix |
| Age categories with small sample sizes (e.g. M95, W95) | Gold | Noted in dashboard |
| Countries with few results (< 100) | Gold | Filtered out in distance and time marts |
| Duplicated athletes due to mid-life changes (club/age class) | Gold | Moved `club` and `age_category` to `fct_results`. Structured dimension with `GROUP BY` and `MIN()` to guarantee 1 row per athlete. |
| Duplicated events across multiple years | Gold | Structured `dim_event` with `GROUP BY event_id` and `MAX(number_finishers)` to handle historical event changes. |

---

## Dashboard

The dashboard is built in Databricks and consists of three tabs:

- **Overview Analytics** — global KPIs, growth trends, gender distribution, top countries
- **Distance Marathon Analytics** — performance by country and age category for distance events
- **Time Marathon Analytics** — distance covered by country and age category for time events

> Link to dashboard: [Marathos Dashboard](https://dbc-193836be-420a.cloud.databricks.com/dashboardsv3/01f15e554b2d192c8e83c3d6c1742bd1/published?o=7474648376001200)

---

## Genie

A Databricks Genie space has been created for Marathos, allowing business stakeholders to ask ad-hoc questions about the data in natural language.

Genie answers have been verified manually in `explorations/verify_genie.ipynb`.

> Link to Genie space: [Marathos Genie Chat](https://dbc-193836be-420a.cloud.databricks.com/genie/rooms/01f16016b96e16839396d9a05d32fea3?o=7474648376001200)

---

## Pipeline Schedule

The pipeline is scheduled to run automatically. *(Update with schedule details after configuration)*

---

## How to Run

1. Clone the repository and connect to your Databricks workspace
2. Upload the raw CSV files to `/Volumes/marathos/default/raw/`
3. Run the bronze pipeline: `transformations/bronze/`
4. Run the silver pipeline: `transformations/silver/marathos_obt.py`
5. Run the gold pipeline: `transformations/gold/`

---

## Notes on LLM Usage

LLM was used for the following tasks:
- Generating the Stockholm Trail Classic 2024 synthetic dataset
- Generating the country codes lookup table
I also used LLM to fix smaller issues or to doublecheck code. Some notes in the project where it's been used on "larger" issues. 

All generated data has been reviewed and validated before ingestion.

---

## Sources used:
- [Joins with using](https://www.geeksforgeeks.org/sql/sql-using-clause/)
- [World Athletics - Ultra Distance Records (for speed filtering validation)](https://worldathletics.org/)
- [Hashing with xxhash64](https://docs.databricks.com/aws/en/sql/language-manual/functions/xxhash64)
- [Pyspark basics](https://docs.databricks.com/aws/en/pyspark/basics)
- [World Athletics - Ultra Distance Records (for speed filtering validation)](https://worldathletics.org/)
