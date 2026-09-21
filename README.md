# TaxiBooking-ETL

A streaming ETL pipeline for a taxi/ride-booking application, built on Databricks using **Spark Declarative Pipelines (SDP)** and deployed via a **Declarative Automation Bundle (DAB)**. The pipeline ingests real-time ride events from Azure Event Hub, enriches them with reference data from Azure Data Lake Storage (ADLS), and delivers analytics-ready tables in a medallion architecture (Bronze → Silver → Gold).

---

## What This Repo Does

The pipeline processes taxi ride booking events end-to-end through three layers:

### 1. Bronze Layer — Raw Ingestion

| File | Source | Target Table | Description |
| --- | --- | --- | --- |
| `bronze_ingestion.py` | Azure Event Hub (Kafka) | `bronze.raw_ingestion` | Streams live ride booking events from the `riding_topic` Event Hub topic using the Kafka protocol. Stores raw Kafka payload as JSON strings. |
| `ADLS_to_Bronze.py` | Azure Blob Storage (ADLS) | `bronze.car_type_mapper`, `bronze.card_brand_mapper`, `bronze.location_mapper`, `bronze.payment_method_mapper`, `bronze.vehicle_model_mapper`, `bronze.historical_bookings` | Loads reference/mapping JSON files and historical booking data from ADLS into materialized views for enrichment. |

### 2. Silver Layer — Cleansed & Enriched

| File | Target Table | Description |
| --- | --- | --- |
| `silver_stg.py` | `silver.bookings_stg` | Parses raw JSON streaming events into a structured schema (booking details, customer, driver, pickup/dropoff, pricing, payment). Merges historical bulk data with live streaming data via append flows. |
| `siver_obt.sql` | `silver.riding_app_obt` | Creates a **One Big Table (OBT)** by stream-joining `bookings_stg` with all bronze mapping tables (car types, card brands, locations, vehicle models) using a 3-minute watermark on `event_timestamp`. |

### 3. Gold Layer — Dimensional Model

| File | Target Tables | Description |
| --- | --- | --- |
| `gold_dimensions.py` | `gold.dim_passenger`, `gold.dim_rider`, `gold.dim_bookings`, `gold.dim_vehicle`, `gold.fact_taxis` | Builds a star schema with SCD Type 2 dimension tables (tracking historical changes) and an SCD Type 1 fact table. Uses Auto CDC to apply inserts, updates, and deletes from the silver OBT. |

---

## Project Structure

```
TaxiBooking-ETL/
├── databricks.yml                          # DAB bundle config (pipeline definition)
├── README.md
└── taxi_riding_ingestion/
    ├── transformations/                    # Pipeline transformation logic
    │   ├── bronze_ingestion.py             # Event Hub → Bronze (streaming)
    │   ├── ADLS_to_Bronze.py              # ADLS → Bronze (reference data)
    │   ├── silver_stg.py                  # Bronze → Silver (parse + merge)
    │   ├── siver_obt.sql                  # Silver OBT (enriched joins)
    │   └── gold_dimensions.py             # Silver → Gold (star schema)
    ├── explorations/                      # Ad-hoc analysis notebooks
    │   ├── Exploration.ipynb
    │   ├── New Exploration 2026-09-01.ipynb
    │   └── jinjs_silver.ipynb
    └── utilities/                          # Helper scripts
        └── New Utility.py
```

---

## Architecture Overview

```
Azure Event Hub          Azure Data Lake Storage (ADLS)
   (streaming)                (reference data & historical)
        │                              │
        ▼                              ▼
  ┌─────────────-┐            ┌──────────────┐
  │  BRONZE      │            │   BRONZE     │
  │ raw_ingestion│            │ mappers &    │
  │ (Kafka)      │            │ historical   │
  └──────┬──────-┘            └──────┬───────┘
         │                          │
         ▼                          ▼
  ┌──────────────────────────────────────┐
  │           SILVER                     │
  │  bookings_stg  →  riding_app_obt     │
  │  (parse JSON)    (enrich with joins) │
  └──────────────────┬───────────────────┘
                     │
                     ▼
  ┌──────────────────────────────────────-┐
  │            GOLD                       │
  │  dim_passenger  dim_rider  dim_vehicle│
  │  dim_bookings   fact_taxis            │
  │  (SCD Type 2)   (SCD Type 1)          │
  └──────────────────────────────────────-┘
```

---

## Key Technologies

* **Spark Declarative Pipelines (SDP)** — Declarative data pipeline framework with Auto CDC, streaming tables, and materialized views
* **Declarative Automation Bundles (DAB)** — Infrastructure-as-code for deploying the pipeline to Databricks
* **Azure Event Hub** — Real-time event ingestion via Kafka protocol
* **Azure Data Lake Storage (ADLS)** — Reference data and historical batch loading
* **Delta Lake** — ACID transactional storage with time travel
* **Auto CDC with SCD Type 1/2** — Change data capture for dimensional model maintenance
* **Databricks Secrets** — Secure credential management (scope: `rideapp`)

---

## Uses of This Project

* **Real-time ride analytics** — Monitor live ride bookings, driver activity, and pricing as events stream in
* **Dimensional modeling** — Star schema (`fact_taxis` + dimension tables) ready for BI dashboards and reporting
* **Historical analysis** — Backfill capability merges historical bookings with live streaming data for end-to-end analysis
* **Slowly Changing Dimensions (SCD)** — Track how passenger, driver, vehicle, and booking attributes change over time (SCD Type 2) while keeping the fact table current (SCD Type 1)
* **Operational insights** — Enriched OBT in the silver layer enables ad-hoc queries on rides, locations, car types, payments, and driver performance
* **Scalable foundation** — Medallion architecture allows incremental processing and easy extension with new data sources or transformations