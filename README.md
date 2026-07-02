# AQI-MLOps — Air Quality Forecasting Pipeline

Production-grade, orchestrated ML pipeline that ingests hourly air-quality and
weather data for Indian cities, forecasts AQI 6 hours ahead (XGBoost vs. LSTM
champion/challenger), serves predictions via a REST API, and monitors drift in a
closed retraining loop.

Full design: [AQI_MLOPS_PROJECT_DOCUMENTATION.md](AQI_MLOPS_PROJECT_DOCUMENTATION.md)
New to the stack? Start here: [docs/01_week1_foundation.md](docs/01_week1_foundation.md)

---

## Status

🚧 **Week 1 — local foundation.** Building the Docker Compose stack (Airflow
`LocalExecutor` + Postgres + MLflow) and the ingest DAG for Delhi.

## Prerequisites

- Docker Desktop (with WSL2 backend on Windows)
- Python 3.11+
- A free [OpenAQ API key](https://explore.openaq.org/register)

## Quick start (local)

Run from the **repo root**:

```bash
cp .env.example .env          # then fill in OPENAQ_API_KEY (already set in local .env)
docker compose --env-file .env -f infra/docker-compose.yml up -d --build
# First run builds the Airflow + MLflow images (a few minutes).
# Airflow UI:  http://localhost:8080   (airflow / airflow)
# MLflow UI:   http://localhost:5000
```

Stack (LocalExecutor, 4 services): `postgres`, `mlflow`, `airflow-webserver`,
`airflow-scheduler` (plus a one-shot `airflow-init`).

Tear down (keep data): `docker compose -f infra/docker-compose.yml down`
Tear down (wipe volumes): `docker compose -f infra/docker-compose.yml down -v`

## Repository layout

```
dags/          Airflow DAGs (ingest, features, train, forecast, monitor)
components/    Containerised pipeline steps (each with Dockerfile + src/)
infra/         docker-compose, prometheus, grafana
notebooks/     EDA and model prototypes
.github/       CI/CD workflows
```
