FROM apache/airflow:2.9.2-python3.11

USER root
RUN apt-get update && apt-get install -y curl && rm -rf /var/lib/apt/lists/*

USER airflow
RUN pip install --no-cache-dir \
    apache-airflow-providers-postgres \
    apache-airflow-providers-docker \
    apache-airflow-providers-http
