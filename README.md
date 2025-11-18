# Airbnb Analytics Data Platform

This repository hosts an end-to-end analytics pipeline that explores Airbnb listing data. The goal is to model business-ready tables, validate raw CSVs, and deliver analytical insights through modular Python code and SQL assets. The documentation will evolve as each phase of the project is completed; for now this README captures the initial structure, dataset context, and execution roadmap.

## Status

- ✅ Repository skeleton created (folders, placeholder configs, docs draft)
- ⏳ Upcoming: dimensional model, ETL pipeline modules, analytical SQL, orchestration and monitoring assets

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
4. **Execution**: upcoming phases will expose CLI entrypoints under `src/pipeline/`. For now, use the roadmap above as guidance.

## Testing

A `tests/` package is reserved for unit tests (pytest). As modules land we will ensure they are covered with realistic fixtures referencing small CSV snippets.

## Contributing & Naming Conventions

- Stick to PEP8-compliant Python code with type hints.
- Prefer Pandas + Pandera for transformations/validation.
- Follow commit naming format from the project brief (e.g., `feat: add dimensional model schema`).

## Next Steps

- Flesh out the dimensional model (`sql/schema.sql`).
- Document modeling rationale and SCD approach inside `SOLUTION.md`.
- Start implementing reusable utilities for DB access and logging.

Stay tuned—this README will be expanded with concrete run instructions and architecture diagrams as the pipeline matures.
