# Solution Design (Work in Progress)

This document captures the architectural choices behind the Airbnb analytics platform. Sections labeled **TODO** will be expanded as the respective phases are completed.

## 1. Business Requirements Mapping
- Primary questions: pricing intelligence, host performance, market opportunities.
- Grain assumptions (draft):
  - `fact_listing_daily_metrics`: one record per listing per date.
  - Dimensions for date, host, listing, neighborhood, property type.
- **TODO**: finalize measures, slowly changing dimension strategy, and source-to-target mappings.

## 2. Dimensional Model Decisions
- **Grain & Facts**: _to be detailed._
- **Slowly Changing Dimensions**:
  - Draft approach: Type 2 for `dim_host` and `dim_listing` with effective timestamps.
  - Lightweight Type 1 updates for low-risk attributes (e.g., `dim_property_type`).
- **Indexes/Partitioning**: _pending detailed schema design._
- **Trade-offs**: _to capture pros/cons of DuckDB vs PostgreSQL vs warehouse of choice._

## 3. Pipeline Architecture
- Stages: Extract → Validate → Transform → Load.
- Technologies:
  - Python (Pandas/Polars TBD) for transformations.
  - Pandera for validation, SQLAlchemy for DB ops.
  - Docker + Compose for reproducible execution.
- **TODO**: include diagrams and orchestration design notes.

## 4. Data Quality Strategy
- Schema validation, business rules (price thresholds, uniqueness, referential checks).
- Output artifact: `output/data_quality_report.json` summarizing each run.
- **TODO**: document severity levels, alerting thresholds, and remediation playbooks.

## 5. Analytics Layer
- SQL queries under `sql/queries/` will address pricing, hosts, and market opportunity use cases.
- **TODO**: describe metric definitions, optimization patterns, and benchmarking results.

## 6. Deployment & Monitoring
- Docker Compose will spin up the warehouse, orchestration service, and pipeline container.
- Airflow DAG (or alternative orchestrator) under `dags/` will trigger daily loads.
- Monitoring placeholders stored in `monitoring/` (dashboards, alert rules).
- **TODO**: finalize observability pipeline and uptime targets.

## 7. Open Questions / Risks
- Which warehouse backend best balances local reproducibility vs realism? (DuckDB vs Postgres)
- Data volume considerations for SCD history tables.
- Validation runtime on full exports.

This document will be revisited after each major phase to capture the final design rationale.
