# Custom Airflow image: base 2.9 + the Python deps our DAGs import at parse/run time.
# Baking them in (vs. _PIP_ADDITIONAL_REQUIREMENTS) means faster, reproducible boots.
FROM apache/airflow:2.9.3-python3.11

COPY requirements.txt /requirements.txt
RUN pip install --no-cache-dir -r /requirements.txt
