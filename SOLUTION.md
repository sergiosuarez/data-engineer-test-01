# Solution Design (Work in Progress)

This document captures the architectural choices behind the Airbnb analytics platform. Sections labeled **TODO** will be expanded as the respective phases are completed.

## 1. Business Requirements Mapping
- Use cases: pricing intelligence, host performance, market opportunities.
- Final grains:
  - `fact_listing_daily_metrics`: one record per listing per day (`date_key`).
  - `fact_review`: one record per individual review (optional, enables cadence/volume metrics).
  - Conformed dimensions: date, host, listing, neighborhood, property type.
- Fact coverage: `price`, `cleaning_fee`, `availability_30/365`, `occupancy_rate`, `estimated_revenue`, `review_scores_rating`, `price_tier` to answer pricing and performance questions.

## 2. Dimensional Model Decisions
- **Tables** (see `sql/schema.sql`):
  - `dim_date`: integer key `YYYYMMDD`, calendar breakdown and `is_weekend`.
  - `dim_property_type` & `dim_neighborhood`: Type 1 dimensions (overwrites on update).
  - `dim_host` & `dim_listing`: SCD Type 2 with `effective_from`, `effective_to`, `is_current` to track critical attribute changes (superhost flag, room_type, amenities, etc.).
  - `fact_listing_daily_metrics`: listing-date fact with foreign keys to every dimension plus availability, pricing and review metrics.
  - `fact_review`: review-level fact for qualitative/cadence analysis (populated later).
- **Slowly Changing Dimensions**:
  - Host and listing rely on surrogate keys (`host_key`, `listing_key`) paired with natural keys (`host_id`, `listing_id`). Indexes `idx_dim_host_natural` and `idx_dim_listing_natural` speed up lookups of the active row.
  - Neighborhood and property type stay Type 1 because changes are rare and the historical context is less valuable.
- **Indexes & constraints**:
  - Unique constraints on natural keys + `effective_from` prevent duplicated versions.
  - Multi-column indexes in facts (`date_key`, `listing_key`, `host_key`) support pricing/location queries; neighborhood indexes filter market-opportunity analyses.
  - `fact_listing_daily_metrics` enforces uniqueness on `date_key + listing_key` to guarantee the declared grain.
- **Data types**:
  - `NUMERIC` for monetary fields avoids precision issues in financial aggregations.
  - `BIGINT` handles long Airbnb identifiers safely.
- **Trade-offs**:
  - Using a dedicated `analytics` schema instead of `public` isolates the DW inside Postgres and simplifies grants.
  - Keeping reviews in a separate fact avoids bloating the daily listing table and enables independent ingestion (reviews can refresh more frequently than inventory metrics).

## 3. Pipeline Architecture
- Stages: Extract → Validate → Transform → Load.
- Technologies:
  - Python (Pandas/Polars TBD) for transformations.
  - Pandera for validation, SQLAlchemy for DB ops.
  - Docker + Compose for reproducible execution.
- Transform stage (Fase 5):
  - Limpieza de precios (strip de símbolos, coerción numérica), homogenización de booleanos (`instant_bookable`, `host_is_superhost`), normalización de `amenities` como hash textual.
  - KPIs calculados: `occupancy_rate = 1 - availability/365`, `estimated_revenue = price * occupancy * 30`, `price_tier` con buckets [budget, standard, premium, luxury].
  - Construcción de dimensiones conformadas en memoria (host, listing, neighborhood, property_type, date) usando los datos crudos + reglas de negocio.
- Load stage:
  - `src/pipeline/load.py` usa staging tables + SQL generado dinámicamente para ejecutar SCD Type 2 en `dim_host` y `dim_listing` (detecta cambios con `COALESCE(lhs::text) IS DISTINCT FROM ...`, cierra registros vigentes y genera nuevas versiones).
  - Dimensiones Type 1 se actualizan vía `TRUNCATE + INSERT` y los hechos son append-only.
  - `load_all` orquesta todo el ciclo y puede ejecutarse como entrypoint (`python -m src.pipeline.load`).
- **TODO**: include diagrams and orchestration design notes.

## 4. Data Quality Strategy
- Implemented Pandera schemas for `listings` y `reviews`:
  - Checks de unicidad (`id`), dominios (`room_type`), rangos (`price >= 0`, `availability_365 <= 365`), y fechas válidas.
  - Reviews exige `listing_id > 0` y `date` no nulo.
- El módulo `src/pipeline/validate.py` produce `output/data_quality_report.json` con resumen de datasets válidos/ inválidos y detalle de fallas (columna, check, valor problemático).
- **TODO**: definir severidades y canalizar alertas hacia el componente de monitoring en Fase 7.

## 5. Analytics Layer
- SQL queries under `sql/queries/` address pricing, hosts, and market opportunity use cases:
  - `01_pricing_intelligence.sql`: compara precio actual vs peers (neighborhood + room_type) y clasifica listings como underpriced/fair/overpriced.
  - `02_host_performance.sql`: ranking con score ponderado (40% revenue, 25% rating, 20% volumen reviews, 15% tamaño de portafolio) y breakdown JSON.
  - `03_market_opportunities.sql`: calcula demand_score (ocupación + reviews) y supply_score (cantidad de listings) para recomendar acciones por vecindario.
- Orchestrator: `src/pipeline/orchestrator.py` ejecuta todas las etapas secuenciales con logging consolidado y manejo de errores.
- **TODO**: describe metric definitions, optimization patterns, and benchmarking results.

## 6. Deployment & Monitoring
- Docker Compose now provisions Postgres (`warehouse`), a utility container (`pipeline`) y un servicio `airflow` basado en `apache/airflow:2.9.1`. Airflow usa LocalExecutor apuntando al mismo Postgres.
- DAG `dags/etl_pipeline.py` ejecuta `src.pipeline.orchestrator.run_pipeline` diariamente (`@daily`) con reintento y logging centralizado.
- `monitoring/dashboards.json` documenta paneles/umbrales sugeridos para tiempo de ejecución, registros procesados y fallas de data quality.
- **TODO**: finalizar observability pipeline y uptime targets (integrar dashboards reales + alertas automáticas).

## 7. Open Questions / Risks
- Which warehouse backend best balances local reproducibility vs realism? (DuckDB vs Postgres)
- Data volume considerations for SCD history tables.
- Validation runtime on full exports.

This document will be revisited after each major phase to capture the final design rationale.
