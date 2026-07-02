-- Runs once on first Postgres init (mounted into /docker-entrypoint-initdb.d).
-- The default POSTGRES_DB (aqi) holds application data (aqi_readings, forecasts…).
-- Airflow metadata and MLflow backend store get their own databases.
CREATE DATABASE airflow;
CREATE DATABASE mlflow;
GRANT ALL PRIVILEGES ON DATABASE airflow TO aqi;
GRANT ALL PRIVILEGES ON DATABASE mlflow  TO aqi;
