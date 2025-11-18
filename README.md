# Airbnb Analytics Data Platform

This repository hosts an end-to-end analytics pipeline that explores Airbnb listing data. The goal is to model business-ready tables, validate raw CSVs, and deliver analytical insights through modular Python code and SQL assets. The documentation will evolve as each phase of the project is completed; for now this README captures the initial structure, dataset context, and execution roadmap.

## Status

- ✅ Repository skeleton + documentación scaffolding
- ✅ Dimensional model, DB connector y utilidades de logging
- ✅ Extract & Validate con Pandera y reporte JSON
- ✅ Transform & Load: KPIs calculados, SCD2 para hosts/listings y carga append-only para facts
- ⏳ Próximo: analytical SQL, orquestación y monitoring

## Dataset

The challenge provides raw files under `data/`:

- `listings.csv`: property-level attributes, pricing, availability
- `reviews.csv`: guest feedback and ratings
- `data_dictionary.md`: reference for column semantics

Raw files must remain untouched so the pipeline can ingest them deterministically.

## Repository Layout

```text
data-engineer-test-01/
├── README.md
├── SOLUTION.md
├── requirements.txt
├── .env.example
├── data/
│   ├── listings.csv
│   ├── reviews.csv
│   └── data_dictionary.md
├── sql/
├── src/
│   ├── pipeline/
│   ├── config/
│   │   └── config.yaml
│   └── utils/
├── tests/
├── logs/
├── output/
├── dags/
├── monitoring/
├── Dockerfile
└── docker-compose.yml
```

Each folder will gain concrete implementations as we progress through the seven phases outlined below.

## Implementation Roadmap

1. **Setup (current)** – Lay down folders, config skeletons, doc placeholders.
2. **Dimensional Model** – Define star schema DDL (`sql/schema.sql`) and document decisions in `SOLUTION.md`.
3. **Utilities** – Implement reusable DB connector + logging helpers.
4. **Extract & Validate** – Build data ingestion layer with schema checks and quality reporting.
5. **Transform & Load** – Materialize dimensions/facts with SCD2 handling and fact calculations.
6. **Orchestration & SQL** – Wire Extract→Load in an orchestrator and craft the analytical SQL queries.
7. **Platform Hardening** – Airflow DAG, Docker services, monitoring dashboards.

Commit messages will mirror these milestones so reviewers can follow the evolution effortlessly.

## Local Setup

1. **Python environment**
   ```bash
   python3 -m venv .venv
   source .venv/bin/activate
   pip install --upgrade pip
   pip install -r requirements.txt
   ```
2. **Environment variables**: copy `.env.example` to `.env` and fill in warehouse credentials plus pipeline defaults.
3. **Configuration**: adjust `src/config/config.yaml` to point to the correct raw data paths, warehouse schema, and output folders.
4. **Execution**:
   - Extraer datasets locales → `python -m src.pipeline.extract`
   - Validar y generar `output/data_quality_report.json` → `python -m src.pipeline.validate`
   - Transformaciones (dimensiones + hechos en memoria) → `python -m src.pipeline.transform`
   - Pipeline completo Extract→Validate→Transform→Load (requiere Postgres arriba) → `python -m src.pipeline.load`
   - Orquestador lineal (Extract→Load + logging consolidado) → `python -m src.pipeline.orchestrator`

## Testing

Pytest cubre extracción, validación y transformaciones principales usando CSVs sintéticos. Ejecutar:

```bash
python -m pytest tests/test_extract_validate.py tests/test_transform.py
```

## Docker & Airflow

1. Copia `.env.example` → `.env` y ajusta credenciales + `AIRFLOW_FERNET_KEY`.
2. Levanta Postgres + Airflow + contenedor utilitario:
   ```bash
   docker compose up warehouse airflow pipeline
   ```
3. Airflow quedará disponible en http://localhost:8080 (usuario/clave por defecto `admin/admin`). Activa el DAG `airbnb_etl_pipeline` para correr el pipeline diario.
4. Los DAGs/artefactos se montan desde el repo, por lo que los cambios locales se reflejan al instante.

## Monitoring

- `monitoring/dashboards.json` define los paneles iniciales (duración del pipeline, registros procesados, fallas de data quality). Puedes usarlo como input para Grafana/Metabase o tu herramienta preferida.

## Contributing & Naming Conventions

- Stick to PEP8-compliant Python code with type hints.
- Prefer Pandas + Pandera for transformations/validation.
- Follow commit naming format from the project brief (e.g., `feat: add dimensional model schema`).

## Next Steps

- Integrar orquestación avanzada/monitoring (Airflow DAG + dashboards) y documentar el flujo completo.
- Preparar contenedores y pipelines automáticos para ejecutar Extract→Load en ambiente reproducible.
- Añadir métricas de observabilidad y alertas (Fase 7).

Stay tuned—este README seguirá creciendo con instrucciones de ejecución end-to-end y artefactos finales.
