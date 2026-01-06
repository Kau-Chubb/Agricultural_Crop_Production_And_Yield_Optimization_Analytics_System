FROM apache/airflow:2.8.1

USER root
RUN pip install --no-cache-dir \
    apache-airflow-providers-databricks \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.8.1/constraints-3.8.txt"

USER airflow
