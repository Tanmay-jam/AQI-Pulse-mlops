# AQI-MLOps — Air Quality Forecasting Pipeline

Production-grade, orchestrated ML pipeline that ingests hourly air-quality and
weather data for Indian cities, forecasts AQI 6 hours ahead (XGBoost vs. LSTM
champion/challenger), serves predictions via a REST API, and monitors drift in a
closed retraining loop.

Full design: [AQI_MLOPS_PROJECT_DOCUMENTATION.md](AQI_MLOPS_PROJECT_DOCUMENTATION.md)

---

## Status

🚧 **Week 1 — local foundation.** Building the Docker Compose stack (Airflow +
Postgres + Redis + MLflow) and the ingest DAG for Delhi.

## Prerequisites

- Docker Desktop (with WSL2 backend on Windows)
- Python 3.11+
- A free [OpenAQ API key](https://explore.openaq.org/register)

## Quick start (local)

```bash
cp .env.example .env          # then fill in OPENAQ_API_KEY
docker compose -f infra/docker-compose.yml up -d
# Airflow UI:  http://localhost:8080   (airflow / airflow)
# MLflow UI:   http://localhost:5000
```

## Repository layout

```
dags/          Airflow DAGs (ingest, features, train, forecast, monitor)
components/    Containerised pipeline steps (each with Dockerfile + src/)
infra/         docker-compose, prometheus, grafana
notebooks/     EDA and model prototypes
.github/       CI/CD workflows
```
