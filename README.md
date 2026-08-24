# Kestrel Provisions — Analytical Foundation & Metric Layer

**AIDE Take-Home Assignment | AI and Data Engineer**

A production-grade data pipeline built with **PySpark** on **Databricks** that transforms messy, multi-source distributor data into a trusted, queryable analytical foundation with documented KPIs.

---

## Architecture

**Medallion Architecture** using Delta Lake:

```
Raw Parquet → Bronze (raw in Delta) → Silver (cleaned & conformed) → Gold (business metrics)
```

### Data Sources
| Feed | Description | Volume |
|------|-------------|--------|
| `pos_transactions` | Point-of-sale lines from retail partners | ~4.08M rows |
| `reefer_telemetry` | Temperature readings from refrigerated vehicles | ~3.71M rows |
| `wms_scan_events` | Warehouse handling scans (receive → dispatch) | ~1.50M rows |
| `erp_cdc/*` | ERP change-data-capture (outlet, product, order masters) | ~0.98M rows |

Total: **~10.3M rows**, 18 months (Jan 2025 – Jun 2026)

---

## Quick Start

### Prerequisites
- Databricks workspace (Community Edition works)
- Cluster: Single node, DBR 13.x+ with Spark 3.4+
- Python 3.10+

### 1. Generate the Dataset

```bash
pip install numpy pandas pyarrow
python generate_dataset.py --scale 1 --out data
```

### 2. Upload to Databricks
Upload the `data/` directory to DBFS, or run the generator directly on the cluster.

### 3. Run the Pipeline
Execute notebooks in order:
```
00_setup.py → 01_bronze_ingest.py → 05_silver_erp.py → 02_silver_pos.py → 
03_silver_reefer.py → 04_silver_wms.py → 06_gold_metrics.py → 07_data_quality.py
```

### 4. Query
Use the parameterised SQL queries in `sql/` or run `notebooks/08_query_runner.py`.

---

## Project Structure

```
├── README.md                          # This file
├── DECISIONS.md                       # Design decisions, trade-offs, assumptions
├── generate_dataset.py                # Data generator (fixed seed, reproducible)
├── notebooks/
│   ├── 00_setup.py                    # Cluster setup & data generation
│   ├── 01_bronze_ingest.py            # Bronze: raw Parquet → Delta
│   ├── 02_silver_pos.py               # Silver: POS cleaning & dedup
│   ├── 03_silver_reefer.py            # Silver: telemetry normalisation
│   ├── 04_silver_wms.py               # Silver: WMS scan cleaning
│   ├── 05_silver_erp.py               # Silver: ERP CDC processing
│   ├── 06_gold_metrics.py             # Gold: KPI aggregation tables
│   ├── 07_data_quality.py             # Data quality checks
│   └── 08_query_runner.py             # Interactive SQL query runner
├── sql/                               # Parameterised SQL query library
├── kpi_catalogue/
│   └── kpi_catalogue.md               # Full KPI catalogue
└── docs/
    └── data_quality_findings.md       # Data quality issues discovered
```

---

## Key Design Decisions

See [DECISIONS.md](DECISIONS.md) for full details.

---

## Tech Stack

| Component | Technology |
|-----------|-----------|
| Processing Engine | Apache Spark (PySpark) |
| Platform | Databricks Community Edition |
| Storage Format | Delta Lake |
| Source Format | Parquet (zstd compressed) |
| Language | Python 3.10, Spark SQL |

---

*Kestrel Provisions and all named individuals are fictional. All data is synthetic and generated for assessment purposes.*
