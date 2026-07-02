# AQI-MLOps: Air Quality Forecasting Pipeline
### Complete Project Documentation

---

## Table of Contents

1. [Problem Statement](#1-problem-statement)
2. [Project Aim](#2-project-aim)
3. [Scope and Constraints](#3-scope-and-constraints)
4. [System Architecture](#4-system-architecture)
5. [Data Sources](#5-data-sources)
6. [ML/DL Design](#6-mldl-design)
7. [Pipeline Design — DAGs](#7-pipeline-design--dags)
8. [Component Design — Docker Services](#8-component-design--docker-services)
9. [GCP Infrastructure](#9-gcp-infrastructure)
10. [CI/CD — GitHub Actions](#10-cicd--github-actions)
11. [Monitoring and Drift Loop](#11-monitoring-and-drift-loop)
12. [Build Plan — Week-by-Week](#12-build-plan--week-by-week)
13. [Repository Structure](#13-repository-structure)
14. [Evaluation Metrics and Reporting](#14-evaluation-metrics-and-reporting)
15. [Interview Defence Points](#15-interview-defence-points)

---

## 1. Problem Statement

Air quality in Indian cities is one of the most consequential public health challenges of the decade. The Central Pollution Control Board (CPCB) operates a network of continuous ambient air quality monitoring stations (CAAQMS) that report PM2.5, PM10, NO₂, SO₂, CO, and O₃ concentrations hourly. This data is publicly accessible via the OpenAQ API.

Despite this rich data stream, most public-facing tools — including CPCB's own portal and popular apps — display only the **current** AQI reading. They provide no short-range forecast, no uncertainty estimate, and no automated alert when conditions are deteriorating. A person planning a morning run at 7 AM has no way of knowing, at 6 AM, what the AQI will be in an hour.

More critically, even in research and academic settings, AQI forecasting pipelines are typically:
- Built as standalone Jupyter notebooks without production infrastructure
- Trained once and never retrained as data distribution shifts
- Deployed, if at all, without monitoring for model degradation
- Not connected to a closed feedback loop between prediction and ground truth

This project treats AQI forecasting as a **production ML problem**, not a modelling exercise.

---

## 2. Project Aim

Build a production-grade, orchestrated ML pipeline that:

1. **Ingests** hourly live air quality and weather data from public APIs for a set of Indian cities
2. **Forecasts** AQI at each monitoring station for the next 6 hours (t+1h through t+6h) with confidence intervals
3. **Compares** a traditional ML model (XGBoost) against a deep learning model (LSTM) on the same holdout set, with automated champion/challenger promotion
4. **Serves** predictions via a REST API deployed on GCP Cloud Run
5. **Monitors** prediction drift and data quality in a closed loop — automatically triggering retraining when drift exceeds a defined threshold
6. **Visualises** forecasts on a live dashboard showing an AQI map per city with 6-hour outlook bands

The system runs on a scheduled basis without manual intervention. Every component is containerised, every model version is tracked, and every infrastructure change is deployed via CI/CD.

---

## 3. Scope and Constraints

### In Scope
- Cities: 5–8 Indian cities with reliable OpenAQ coverage (Delhi, Mumbai, Kolkata, Chennai, Bengaluru, Hyderabad, Patna, Pune — confirmed active stations)
- Pollutants: PM2.5, PM10, NO₂, CO as primary features; SO₂ and O₃ where available
- Forecast horizon: 6 hours ahead, one prediction per hour per station
- Models: XGBoost (traditional ML) and LSTM (deep learning); one active production model at a time
- Infrastructure: GCP free tier + $300 trial credit; everything containerised via Docker
- Orchestration: Apache Airflow with CeleryExecutor, 5 DAGs

### Out of Scope
- Satellite imagery (Sentinel-5P) — scoped as a future extension
- Generative/diffusion models — deferred to project phase 2
- Real-time streaming (Kafka/Pub-Sub) — pipeline is micro-batch hourly, not sub-minute streaming
- Mobile app — dashboard is web-only (Streamlit or Leaflet HTML)
- Multi-pollutant separate forecasts — we forecast composite AQI, not individual pollutant concentrations

### Constraints
- No GPU available on GCP free tier; LSTM must train on CPU in reasonable time (FD001 benchmark: ~15 min for 24-step LSTM on CPU with a small hidden size)
- Cloud Storage free tier: 5 GB — raw data for 5 cities × hourly × 30 days ≈ 300 MB; well within limit
- Cloud Run free tier: 2 million requests/month — adequate for a low-traffic dashboard and API
- Airflow runs on a single VM; no high-availability setup

---

## 4. System Architecture

### High-Level Data Flow

```
Public APIs (OpenAQ, Open-Meteo)
        │
        ▼
[DAG 1: Ingest] — hourly scheduled
        │  raw JSON → GCS bucket (raw/)
        ▼
[DAG 2: Features] — triggers after ingest
        │  Parquet feature tables → GCS bucket (features/)
        ▼
[DAG 3: Train] — weekly + on drift trigger
        │  model artifacts → MLflow registry
        │  Docker images → Artifact Registry
        ▼
[DAG 4: Forecast] — hourly
        │  predictions + CI → Postgres (forecasts table)
        ▼
[DAG 5: Monitor] — hourly
        │  drift report → Prometheus
        │  if drift > threshold → trigger DAG 3 via REST API  ◄─── closed loop
        ▼
FastAPI serving (Cloud Run)   ◄─── pulls model from MLflow registry
        │
        ▼
Streamlit dashboard — AQI map + 6-hour forecast bands
```

### Communication Between Components

All components share state through:
- **GCS bucket** — raw data, feature Parquet files, model artifacts, Evidently drift HTML reports
- **Postgres** — station metadata, hourly AQI readings, forecasts, ground truth, evaluation scores
- **MLflow registry** — model versions with `staging` and `production` aliases
- **Prometheus** — real-time serving and drift metrics
- **Airflow REST API** — drift monitor triggers training DAG programmatically

No component calls another component's container directly. All communication is via shared storage or the Airflow API. This makes each component independently restartable and testable.

---

## 5. Data Sources

### Primary: OpenAQ API v3
- **URL:** `https://api.openaq.org/v3/`
- **Auth:** Free API key (register at openaq.org)
- **Endpoint used:** `GET /v3/locations/{locationId}/measurements` — returns hourly pollutant readings per station
- **Pull strategy:** for each city, pull `latest` measurements every hour; pull last 30 days on first boot (historical backfill DAG)
- **Data schema:** `{location_id, city, parameter, value, unit, timestamp_utc}`
- **Rate limits:** 60 requests/minute on free tier — with 8 cities × 4 parameters = 32 requests/run, well within limits

### Secondary: Open-Meteo API
- **URL:** `https://api.open-meteo.com/v1/forecast`
- **Auth:** None required (fully free, no key)
- **Fields pulled:** `temperature_2m`, `relative_humidity_2m`, `wind_speed_10m`, `wind_direction_10m`, `precipitation`
- **Resolution:** hourly, 7-day forecast available — pulled for past 24h + next 6h to serve as forecast weather covariates

### Validation Layer (Pandera)
Every ingested record is validated before it touches Postgres:
- `value` must be a non-negative float
- `timestamp_utc` must be within the last 2 hours (stale data fails the DAG, not silently inserted)
- `parameter` must be in the allowed enum set
- Null fraction per batch must be below 20% — above this the DAG raises `AirflowException`, Slack/email alert fires

---

## 6. ML/DL Design

### What the Models Predict

For each station `s` at each hour `t`:

```
Input:  X = [AQI_{t-1}, AQI_{t-2}, ..., AQI_{t-24},     # 24 AQI lags
              PM25_{t-1..t-6},                             # 6 PM2.5 lags
              wind_speed_t, wind_dir_t,                    # weather at t
              hour_of_day, day_of_week, month,             # time features
              wind_speed_{t+1..t+6}, temp_{t+1..t+6}]     # forecast weather covariates

Output: ŷ = [AQI_{t+1}, AQI_{t+2}, ..., AQI_{t+6}]       # 6-step ahead forecast
```

Ground truth arrives one hour at a time and is stored in Postgres. The monitor DAG joins yesterday's `ŷ_{t+k}` against the now-realized `AQI_{t+k}`.

### Model 1: XGBoost (Traditional ML Baseline)

**Why XGBoost here:** AQI forecasting is fundamentally a tabular regression problem with strong lag autocorrelation. XGBoost handles non-linear interactions between lag features and weather covariates without manual feature engineering beyond the lags themselves. It trains in seconds on CPU and produces highly interpretable SHAP feature importances.

**Architecture:**
- Multi-output regression: one XGBoost regressor per forecast horizon (6 models), or a single multi-output wrapper
- Features: 24 AQI lags + 6 PM2.5 lags + 6 weather features + 3 time features = ~39 input features
- Hyperparameters (starting point, tuned via Optuna in the training DAG):
  - `n_estimators`: 300–600
  - `max_depth`: 4–7
  - `learning_rate`: 0.05–0.15
  - `subsample`: 0.8
  - `colsample_bytree`: 0.8
- Training data: rolling 90-day window, retrained weekly or on drift trigger
- Holdout: last 7 days (not shuffled — preserving temporal order is mandatory)

**Feature importance:** SHAP values logged to MLflow as an artifact (bar chart). The t-1 lag and wind speed are expected to dominate.

### Model 2: LSTM (Deep Learning)

**Why LSTM here:** While XGBoost sees hand-crafted lag features, the LSTM sees the raw sequence and can learn temporal patterns implicitly — for example, the daily cycle of rush-hour spikes, or the overnight temperature-inversion effect that traps pollutants. If the LSTM outperforms XGBoost on the holdout, it suggests there is temporal structure in the data beyond what the lag features capture.

**Architecture:**
```
Input: (batch, seq_len=24, n_features=8)
  → LSTM(hidden_size=64, num_layers=2, dropout=0.2)
  → Linear(64 → 6)
Output: (batch, 6)  — one value per forecast horizon
```

- Sequence length: 24 hours (1 full day)
- Input features per step: AQI, PM2.5, wind speed, wind direction, temperature, humidity (6), plus hour-of-day sine/cosine encoding (2) = 8 features
- Loss: MSE
- Optimizer: Adam, lr=1e-3, weight decay=1e-5
- Scheduler: ReduceLROnPlateau (patience=5)
- Early stopping: patience=10 on validation loss
- Training time: ~10–15 minutes on CPU for 50 epochs over 90 days of hourly data per city

**Note on uncertainty:** The LSTM produces a point forecast. Prediction intervals are estimated via Monte Carlo dropout (dropout active at inference time, 30 forward passes, take mean ± 1.96 × std). This gives calibrated confidence bands for the dashboard without requiring a separate model.

### Champion/Challenger Promotion

After every training run (weekly or drift-triggered):
1. Both models are trained on the same 90-day window
2. Both are evaluated on the same 7-day holdout
3. Primary metric: **RMSE** (equally penalises directional errors); secondary: **NASA-style asymmetric score** (penalises under-prediction of high AQI more heavily — a health-safety consideration)
4. The model with lower holdout RMSE is registered in MLflow with the `production` alias
5. The serving container polls the MLflow registry on startup — no manual redeployment needed for a model swap

If neither model beats the current champion by more than 2% relative RMSE, the current champion is retained (prevents churn from noise).

---

## 7. Pipeline Design — DAGs

### DAG 1: `dag_ingest.py`

```
Schedule:    @hourly
Max active:  1 (no concurrent runs)
Catchup:     True (backfillable)

Tasks:
  check_api_health        [HttpSensor]          — confirm OpenAQ API responds
      │
      ▼
  pull_measurements.*     [PythonOperator ×N]   — dynamic task mapping over city list
      │                                           one task per city, parallel
      ▼
  validate_schema         [PythonOperator]      — Pandera validation; raises on failure
      │
      ▼
  write_to_gcs            [PythonOperator]      — raw JSON → GCS raw/{date}/{hour}/
      │
      ▼
  insert_postgres         [PythonOperator]      — upsert into aqi_readings table
```

Key design decisions:
- `HttpSensor` with `poke_interval=60`, `timeout=600` — DAG waits up to 10 minutes for the API before failing gracefully
- Dynamic task mapping over the city list means adding a new city requires only a config change, not a DAG edit
- Upsert (`INSERT ... ON CONFLICT DO NOTHING`) ensures idempotent reruns — safe to backfill any date range

### DAG 2: `dag_features.py`

```
Schedule:    @hourly
Trigger:     ExternalTaskSensor on dag_ingest / insert_postgres

Tasks:
  wait_for_ingest         [ExternalTaskSensor]
      │
      ▼
  compute_lag_features    [PythonOperator]      — 1h, 3h, 6h, 12h, 24h lags + rolling stats
      │
      ▼
  join_weather            [PythonOperator]      — join Open-Meteo hourly data
      │
      ▼
  write_features_parquet  [PythonOperator]      — features/{city}/{date}.parquet → GCS
```

### DAG 3: `dag_train.py`

```
Schedule:    @weekly (Sunday 02:00 UTC) + triggered via REST API on drift
Catchup:     False

Tasks:
  load_training_data      [PythonOperator]      — read last 90 days of features from GCS
      │
      ├──────────────────────────────────────────────────────────┐
      ▼                                                          ▼
  train_xgboost           [DockerOperator]      train_lstm       [DockerOperator]
      │                                                          │
      ▼                                                          ▼
  evaluate_xgboost        [PythonOperator]      evaluate_lstm   [PythonOperator]
      │                                                          │
      └───────────────────────┬──────────────────────────────────┘
                              ▼
                  compare_and_promote         [PythonOperator]
                      — champion/challenger comparison
                      — promotes winner to MLflow 'production' alias
                      │
                      ▼
                  notify_slack                [PythonOperator]
                      — posts model promotion result with RMSE comparison
```

Key design decision: XGBoost and LSTM train in parallel (`DockerOperator` tasks with no dependency between them). Total training wall-clock time ≈ max(XGB_time, LSTM_time) ≈ 15 minutes.

### DAG 4: `dag_forecast.py`

```
Schedule:    @hourly (runs 10 minutes after dag_ingest)

Tasks:
  load_production_model   [PythonOperator]      — pull 'production' model from MLflow registry
      │
      ▼
  generate_forecasts      [PythonOperator]      — score t+1..t+6 for all stations
      │
      ▼
  write_forecasts         [PythonOperator]      — insert into forecasts table in Postgres
                                                  columns: station_id, forecast_time,
                                                  target_hour, predicted_aqi, lower_ci, upper_ci
```

### DAG 5: `dag_monitor.py`

```
Schedule:    @hourly (runs 5 minutes after dag_forecast)

Tasks:
  join_predictions_vs_actuals  [PythonOperator]
      — for each station: join forecasts made 1-6 hours ago against now-realized AQI
      │
      ▼
  compute_metrics              [PythonOperator]
      — RMSE, MAE, coverage (% actuals inside CI) per horizon per station
      — push to Prometheus via pushgateway
      │
      ▼
  run_evidently_drift          [PythonOperator]
      — compare last 24h feature distribution vs training baseline
      — write HTML report to GCS drift-reports/{timestamp}.html
      │
      ▼
  check_drift_threshold        [BranchPythonOperator]
      — if PSI > 0.2 or RMSE degraded > 15%: branch to trigger_retrain
      — else: branch to drift_ok (no-op)
      │
      ├── [drift OK]  → drift_ok (EmptyOperator)
      │
      └── [drift detected] → trigger_retrain [TriggerDagRunOperator]
                              — triggers dag_train via Airflow REST API
```

This DAG is the architectural centrepiece. The `BranchPythonOperator → TriggerDagRunOperator` path is what closes the feedback loop and makes the pipeline self-healing.

---

## 8. Component Design — Docker Services

### docker-compose.yml Services (VM)

| Service | Image | Role | Ports |
|---|---|---|---|
| `airflow-webserver` | `apache/airflow:2.9` | DAG UI, REST API | 8080 |
| `airflow-scheduler` | `apache/airflow:2.9` | DAG scheduling | — |
| `airflow-worker` | `apache/airflow:2.9` | Task execution (Celery) | — |
| `postgres` | `postgres:15` | Airflow metadata + AQI data | 5432 |
| `redis` | `redis:7` | Celery message broker | 6379 |
| `mlflow` | custom (`./components/mlflow/`) | Experiment tracking + registry | 5000 |
| `prometheus` | `prom/prometheus` | Metrics collection | 9090 |
| `grafana` | `grafana/grafana` | Metrics dashboard | 3000 |

### Pipeline Component Images (pushed to Artifact Registry)

Each lives in `components/<name>/` with its own `Dockerfile` and `src/`:

| Component | Built From | What It Does |
|---|---|---|
| `ingest` | `python:3.11-slim` | Calls OpenAQ API, validates with Pandera, writes to GCS + Postgres |
| `features` | `python:3.11-slim` | Reads Postgres, computes lag/weather features, writes Parquet to GCS |
| `train-xgb` | `python:3.11-slim` + XGBoost | Loads GCS features, trains, logs to MLflow |
| `train-lstm` | `python:3.11-slim` + PyTorch CPU | Loads GCS features, trains LSTM, logs to MLflow |
| `forecast` | `python:3.11-slim` | Loads MLflow model, scores stations, writes to Postgres |
| `monitor` | `python:3.11-slim` + Evidently | Joins forecasts vs actuals, runs drift, pushes to Prometheus |
| `serve` | `python:3.11-slim` + FastAPI | Loads MLflow model, serves `/predict` and `/health` endpoints |

Airflow runs the first six as `DockerOperator` tasks. The `serve` image is deployed separately to Cloud Run.

### Inter-Component Communication

```
ingest ──writes──► GCS (raw/)     ◄──reads── features
features ──writes─► GCS (features/) ◄──reads── train-xgb, train-lstm
train-* ──logs──► MLflow registry ◄──reads── forecast, serve
forecast ──writes─► Postgres (forecasts) ◄──reads── monitor, dashboard
monitor ──pushes─► Prometheus     ◄──reads── Grafana
monitor ──triggers─► Airflow REST API ──triggers─► dag_train
```

No HTTP calls between containers. All coordination via shared storage and the Airflow API.

---

## 9. GCP Infrastructure

### Services Used

| GCP Service | Purpose | Free Tier |
|---|---|---|
| Compute Engine (e2-standard-2) | Hosts Docker Compose stack | $300 credit — ~$48/month |
| Cloud Storage | Data lake, model artifacts, drift reports | 5 GB always free |
| Artifact Registry | Docker image registry | 0.5 GB free |
| Cloud Run | FastAPI serving container | 2M req/month always free |
| Cloud Build | (via GitHub Actions) | 120 build-min/day free |

### Infrastructure Setup (one-time)

```bash
# 1. Create project and set region
gcloud config set project aqi-mlops
gcloud config set compute/region us-central1

# 2. Enable APIs
gcloud services enable \
  compute.googleapis.com \
  run.googleapis.com \
  artifactregistry.googleapis.com \
  storage.googleapis.com

# 3. Create GCS bucket
gsutil mb -l us-central1 gs://aqi-mlops-data

# 4. Create Artifact Registry repo
gcloud artifacts repositories create aqi-mlops \
  --repository-format=docker \
  --location=us-central1

# 5. Create VM
gcloud compute instances create aqi-mlops-vm \
  --machine-type=e2-standard-2 \
  --zone=us-central1-a \
  --image-family=ubuntu-2204-lts \
  --image-project=ubuntu-os-cloud \
  --boot-disk-size=30GB

# 6. Set billing alert (critical)
# Console: Billing → Budgets → Create budget → $10 → alert at 50%, 90%, 100%
```

### GCS Bucket Layout

```
gs://aqi-mlops-data/
├── raw/
│   └── {city}/{YYYY-MM-DD}/{HH}/measurements.json
├── features/
│   └── {city}/{YYYY-MM-DD}.parquet
├── models/
│   └── (MLflow artifact store — managed by MLflow)
└── drift-reports/
    └── {YYYY-MM-DD-HH}.html
```

### Cloud Run Serving

```bash
# Deploy serve container (after push to Artifact Registry)
gcloud run deploy aqi-forecast-api \
  --image us-central1-docker.pkg.dev/aqi-mlops/aqi-mlops/serve:latest \
  --region us-central1 \
  --platform managed \
  --allow-unauthenticated \
  --min-instances 0 \
  --max-instances 2 \
  --memory 1Gi
```

The `--min-instances 0` setting means the container scales to zero when idle — zero compute charge. Cold start is ~3–5 seconds for the first request after idle, acceptable for a dashboard application.

---

## 10. CI/CD — GitHub Actions

### Workflow 1: `ci.yml` — on every Pull Request

```yaml
jobs:
  lint:
    - ruff check .            # linting
    - ruff format --check .   # formatting
  test:
    - pytest components/ingest/tests/
    - pytest components/features/tests/
    - pytest dags/tests/      # DAG import tests (no task execution)
  build:
    - docker build components/ingest/
    - docker build components/serve/
    # ... (build only, no push on PR)
```

### Workflow 2: `deploy.yml` — on push to `main`

```yaml
jobs:
  build-and-push:
    - Authenticate to GCP via Workload Identity Federation
      (no long-lived keys stored as secrets)
    - docker build + push each component image to Artifact Registry
    - Tag with git SHA and 'latest'

  deploy-serve:
    - gcloud run deploy aqi-forecast-api
        --image .../serve:{SHA}
    - Run smoke test: curl /health endpoint
    - Roll back if health check fails
```

### Workload Identity Federation

Instead of storing a GCP service account JSON key as a GitHub secret (insecure), the workflow uses Workload Identity Federation: GitHub Actions authenticates as a GCP service account via OIDC token exchange. This is the current GCP best practice for CI/CD authentication and a good interview talking point.

---

## 11. Monitoring and Drift Loop

### What Is Monitored

**Prediction quality (hourly):**
- RMSE per forecast horizon (t+1 through t+6) — expected to increase with horizon
- MAE per station — flags stations with chronically poor predictions
- Coverage: percentage of actual values falling inside the 90% confidence interval — should be ~90%; if lower, the LSTM MC-dropout uncertainty is underestimating

**Data drift (hourly, via Evidently):**
- Population Stability Index (PSI) on input features vs. the training data baseline
- PSI > 0.2 on any feature = significant drift → retrain trigger
- Missing value rate per feature per hour

**Serving health (via Prometheus + Grafana):**
- Request latency (p50, p95, p99)
- Requests per minute
- Error rate (non-200 responses)
- Model version currently serving

### The Closed Loop

```
Every hour:
  dag_monitor runs
      │
      ├── joins: forecast(t-k) vs actual(t) for k=1..6
      ├── computes: RMSE, MAE, CI coverage
      ├── runs: Evidently drift report on last 24h features
      ├── pushes: all metrics to Prometheus
      │
      └── if PSI > 0.2 OR rolling_RMSE > 1.15 × baseline_RMSE:
              trigger_retrain via Airflow REST API
                  POST /api/v1/dags/dag_train/dagRuns
                  body: {"conf": {"trigger_reason": "drift_detected"}}
```

The retrain run logs a new MLflow experiment with `trigger_reason=drift_detected` in its parameters — making the audit trail complete. You can see in MLflow exactly which run was triggered by drift vs. the weekly schedule.

---

## 12. Build Plan — Week-by-Week

### Week 1 — Foundation (local)
- Set up Docker Compose with Airflow + Postgres + Redis + MLflow locally
- Write `dag_ingest.py` for a single city (Delhi — highest station density on OpenAQ)
- Confirm raw data lands in a local folder (simulating GCS) and inserts into Postgres
- Deliverable: `docker compose up` brings the full stack; one DAG runs and data appears in Postgres

### Week 2 — Features + Baseline Model (local)
- Write `dag_features.py`; inspect the feature Parquet in a notebook
- Prototype XGBoost in a notebook: `notebooks/01_xgb_prototype.ipynb`
  - Understand the data shape, check autocorrelation of lags, plot rolling RMSE vs horizon
  - This is the "understand your data" step before wrapping in a DAG
- Wrap training as `dag_train.py` (XGBoost only at this stage)
- Log one run to MLflow; verify experiment appears in the UI
- Deliverable: local end-to-end pipeline, XGBoost RMSE reported in MLflow

### Week 3 — GCS + VM on GCP
- Replace all local file paths with GCS reads/writes
- Provision the e2-standard-2 VM; `git clone` the repo; `docker compose up`
- Set region = `us-central1` for all resources
- Set billing alert in GCP console immediately
- Run the ingest DAG on the live VM; verify data flows into GCS and Postgres on GCP
- Deliverable: pipeline running live in GCP, data accumulating in GCS bucket

### Week 4 — LSTM + Champion/Challenger
- Write `components/train-lstm/` — PyTorch LSTM with MC-dropout
- Prototype in `notebooks/02_lstm_prototype.ipynb` first
- Add LSTM training as a parallel branch in `dag_train.py`
- Implement champion/challenger comparison logic with MLflow `production` alias
- Deliverable: weekly DAG runs both models, promotes the better one, logs comparison to MLflow

### Week 5 — Serving + CI/CD
- Write FastAPI `serve` component: `GET /predict?station_id=...&hours=6`
- Deploy to Cloud Run manually first, confirm it works
- Write GitHub Actions `ci.yml` and `deploy.yml`
- Set up Workload Identity Federation for GCP auth in Actions
- Push to main — confirm Actions builds and deploys automatically
- Deliverable: public Cloud Run URL returns forecasts; every push to main redeploys

### Week 6 — Monitoring + Closed Loop
- Write `dag_monitor.py` with Evidently integration
- Set up Prometheus pushgateway on the VM
- Build Grafana dashboard: RMSE per horizon, drift score, request latency
- Test the retrain trigger: manually corrupt the feature distribution, confirm dag_train fires
- Deliverable: closed feedback loop working end-to-end; Grafana shows live metrics

### Week 7 — Dashboard + Polish
- Build Streamlit dashboard: AQI map (Folium) with station markers coloured by current AQI category; click a station to see the 6-hour forecast with confidence bands
- Write README: architecture diagram, results table (XGB vs LSTM RMSE per horizon), GIF of dashboard, how to run locally, how to deploy to GCP
- Clean up repo: remove notebooks from main branch (move to `notebooks/` subdir), ensure all secrets are in `.env.example` (not committed), write `CONTRIBUTING.md`
- Deliverable: portfolio-ready GitHub repository

---

## 13. Repository Structure

```
aqi-mlops/
│
├── dags/
│   ├── dag_ingest.py
│   ├── dag_features.py
│   ├── dag_train.py
│   ├── dag_forecast.py
│   ├── dag_monitor.py
│   └── tests/
│       └── test_dag_imports.py      # confirms all DAGs parse without errors
│
├── components/
│   ├── ingest/
│   │   ├── Dockerfile
│   │   ├── requirements.txt
│   │   ├── src/
│   │   │   ├── main.py              # entrypoint
│   │   │   ├── openaq_client.py
│   │   │   ├── validator.py         # Pandera schemas
│   │   │   └── writer.py            # GCS + Postgres
│   │   └── tests/
│   │       └── test_validator.py
│   │
│   ├── features/
│   ├── train-xgb/
│   ├── train-lstm/
│   ├── forecast/
│   ├── monitor/
│   └── serve/
│       ├── Dockerfile
│       ├── requirements.txt
│       └── src/
│           ├── main.py              # FastAPI app
│           ├── model_loader.py      # loads from MLflow registry
│           └── schemas.py           # Pydantic request/response models
│
├── notebooks/
│   ├── 01_eda.ipynb
│   ├── 02_xgb_prototype.ipynb
│   └── 03_lstm_prototype.ipynb
│
├── infra/
│   ├── docker-compose.yml
│   ├── prometheus/
│   │   └── prometheus.yml
│   └── grafana/
│       └── dashboards/
│           └── aqi_mlops.json
│
├── .github/
│   └── workflows/
│       ├── ci.yml
│       └── deploy.yml
│
├── .env.example                     # template — never commit .env
├── requirements-dev.txt             # ruff, pytest, pre-commit
└── README.md
```

---

## 14. Evaluation Metrics and Reporting

### Model Performance Table (to be filled in README after Week 4)

| Model | Horizon | RMSE | MAE | Within 90% CI |
|---|---|---|---|---|
| XGBoost | t+1h | — | — | — |
| XGBoost | t+3h | — | — | — |
| XGBoost | t+6h | — | — | — |
| LSTM | t+1h | — | — | — |
| LSTM | t+3h | — | — | — |
| LSTM | t+6h | — | — | — |

Expected result: LSTM should outperform XGBoost on t+3h and t+6h (longer horizons where temporal patterns matter more); XGBoost may win on t+1h where the most recent lag is the dominant signal.

### Data Quality Metrics (logged per DAG run)
- Records ingested per city per hour
- Null fraction per pollutant parameter
- Station availability (% of expected stations reporting)

### System Metrics (Grafana)
- DAG success rate (last 7 days)
- Forecast API p95 latency
- Drift score trend (PSI over time)
- Retrain events (annotated on RMSE time series)

---

## 15. Interview Defence Points

These are the questions an interviewer will ask when reviewing this project. Answers should be internalised, not read off a page.

**"Why Airflow and not a simple cron job?"**
Cron has no dependency management (task B shouldn't run if task A failed), no built-in retry logic, no backfilling (re-running historical dates), no visibility into what ran and what didn't, and no way to trigger a DAG programmatically from another service. The drift monitor's ability to fire a retrain via the Airflow REST API is impossible with cron.

**"Why XGBoost and LSTM together? Why not just one?"**
They test different hypotheses about the data. XGBoost encodes our prior knowledge of the problem (explicit lag features). LSTM encodes the hypothesis that the raw sequence contains patterns our feature engineering misses. Running both and comparing on a held-out test set is rigorous model selection, not a gimmick. In production, only one model serves at a time.

**"How do you prevent data leakage in training?"**
The train/test split strictly respects temporal order — the last 7 days are the holdout, the preceding 90 days are training. No shuffling. Lag features at position t only use values from t-1 and earlier — computed at feature engineering time, not at training time. Open-Meteo forecast weather covariates (for t+1..t+6) simulate what would actually be available at inference time.

**"What happens when OpenAQ is down for an hour?"**
The `HttpSensor` in dag_ingest retries for up to 10 minutes. If the API is still down, the DAG fails cleanly and Airflow sends an alert. The downstream DAGs wait via `ExternalTaskSensor` and also fail. No partial data is inserted. The next hour's run will catch up normally (idempotent upsert). If the outage lasts multiple hours, you can manually trigger a backfill for the missed hours — Airflow handles this natively.

**"How does the drift trigger work exactly?"**
`dag_monitor` computes Population Stability Index (PSI) on the last 24 hours of input features against the training data baseline. PSI > 0.2 on any feature is the conventional threshold for significant drift. If triggered, `TriggerDagRunOperator` posts to `POST /api/v1/dags/dag_train/dagRuns` with a JSON body that includes `trigger_reason: drift_detected`. This is logged in MLflow so you can audit exactly which training runs were drift-triggered.

**"What would you add with more compute?"**
(1) Sentinel-5P satellite NO₂ and aerosol columns as additional spatial features — these require cloud-based preprocessing of GeoTIFF files. (2) A graph neural network over the station network (stations as nodes, wind-adjusted distance as edge weights) — captures spatial spillover between stations. (3) A probabilistic diffusion model for forecast uncertainty instead of MC-dropout. (4) Kafka for true streaming instead of hourly micro-batch.

---

*Document version: 1.0 — July 2026*
*Author: Tanmay Pawar, M.Tech AI, IIT Patna*
*Project: AQI-MLOps — Air Quality Forecasting Pipeline*
