# Solution Design (Work in Progress)

This document captures the architectural choices behind the Airbnb analytics platform. Sections labeled **TODO** will be expanded as the respective phases are completed.

## 1. Business Requirements Mapping
- Casos de uso: pricing intelligence, performance de hosts y oportunidades de mercado.
- Granularidades finales:
  - `fact_listing_daily_metrics`: un registro por `listing` por día (`date_key`).
  - `fact_review`: un registro por review individual (opcional, habilita métricas de sentimiento/volumen).
  - Dimensiones conformadas: fecha, host, listing, vecindario y tipo de propiedad.
- Surtido de métricas en el hecho diario: `price`, `cleaning_fee`, `availability_30/365`, `occupancy_rate`, `estimated_revenue`, `review_scores_rating`, `price_tier`. Cubren las preguntas de precios y desempeño.

## 2. Dimensional Model Decisions
- **Tablas** (ver `sql/schema.sql`):
  - `dim_date`: llave entera `YYYYMMDD`, columnas de calendario y flag `is_weekend`.
  - `dim_property_type` y `dim_neighborhood`: dimensiones tipo 1 (actualizaciones sobreescriben).
  - `dim_host` y `dim_listing`: SCD Type 2 con `effective_from`, `effective_to`, `is_current` para rastrear cambios en atributos críticos (superhost, room_type, amenities, etc.).
  - `fact_listing_daily_metrics`: hecho granular por listing-date con llaves hacia todas las dimensiones más métricas de disponibilidad, precios y reseñas.
  - `fact_review`: nivel review para análisis cualitativo/cadencia (puede poblarse más adelante).
- **Slowly Changing Dimensions**:
  - Host y listing usan surrogate keys (`host_key`, `listing_key`) y natural keys (`host_id`, `listing_id`). Índices `idx_dim_host_natural` e `idx_dim_listing_natural` aceleran búsquedas del registro activo.
  - Vecindario y tipo de propiedad son Type 1 porque los cambios son infrecuentes y no aportan valor histórico.
- **Índices y constraints**:
  - Restricciones únicas en claves naturales + `effective_from` aseguran no duplicar versiones.
  - Índices multi-columna en hechos (`date_key`, `listing_key`, `host_key`) soportan las consultas de pricing/location. Índices por vecindario permiten filtrar oportunidades de mercado.
  - `fact_listing_daily_metrics` fuerza unicidad `date_key + listing_key` para sostener el grano.
- **Elección de tipos**:
  - `NUMERIC` para importes monetarios evita errores de precisión en análisis financieros.
  - `BIGINT` para claves naturales (Airbnb usa IDs largos).
- **Trade-offs**:
  - Se define `schema analytics` en vez de `public` para aislar el DW en Postgres y facilitar permisos.
  - Mantener un hecho de reviews separado evita inflar el hecho diario y habilita pipelines independientes (reviews se actualizan con más frecuencia que calendarios).

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
