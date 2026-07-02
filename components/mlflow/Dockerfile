# MLflow tracking server + registry, backed by Postgres, artifacts on a local volume.
FROM python:3.11-slim

RUN pip install --no-cache-dir \
      mlflow==2.16.2 \
      psycopg2-binary==2.9.9

EXPOSE 5000

# backend-store-uri and artifact root are supplied by docker-compose command.
ENTRYPOINT ["mlflow", "server", "--host", "0.0.0.0", "--port", "5000"]
