"""
Airflow DAG to trigger Databricks Serverless Job
Agricultural Crop Production ETL: Bronze → Silver → Gold
"""

from airflow import DAG
from airflow.providers.databricks.operators.databricks import DatabricksRunNowOperator
from datetime import datetime, timedelta
import logging
import os
from logging.handlers import RotatingFileHandler

# -------------------------------------------------------------------
# LOGGING CONFIGURATION
# -------------------------------------------------------------------

LOG_DIR = "/opt/airflow/logs/agricultural_crop_etl"
os.makedirs(LOG_DIR, exist_ok=True)

LOG_FILE = os.path.join(LOG_DIR, "agricultural_crop_etl.log")

logger = logging.getLogger("agricultural_crop_etl")
logger.setLevel(logging.INFO)

# Prevent duplicate handlers on DAG reload
if not logger.handlers:
    file_handler = RotatingFileHandler(
        LOG_FILE,
        maxBytes=5 * 1024 * 1024,  # 5 MB
        backupCount=3
    )

    formatter = logging.Formatter(
        "%(asctime)s | %(levelname)s | %(name)s | %(message)s"
    )

    file_handler.setFormatter(formatter)
    logger.addHandler(file_handler)

# -------------------------------------------------------------------
# CALLBACKS FOR MONITORING
# -------------------------------------------------------------------

def on_task_success(context):
    logger.info(
        f"Task {context['task_instance'].task_id} completed successfully "
        f"for DAG {context['dag'].dag_id}"
    )

def on_task_failure(context):
    logger.error(
        f"Task {context['task_instance'].task_id} failed "
        f"for DAG {context['dag'].dag_id}",
        exc_info=True
    )

# -------------------------------------------------------------------
# DEFAULT ARGS
# -------------------------------------------------------------------

default_args = {
    "owner": "kaushik",
    "depends_on_past": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
    "on_success_callback": on_task_success,
    "on_failure_callback": on_task_failure,
}

# -------------------------------------------------------------------
# DAG DEFINITION
# -------------------------------------------------------------------

with DAG(
    dag_id="Agricultural_Crop_Production_databricks_etl",
    description="Trigger Databricks Serverless Job for Agricultural Crop Production ETL",
    default_args=default_args,
    start_date=datetime(2024, 1, 1),
    schedule_interval=None,   # manual trigger
    catchup=False,
    tags=["Agriculture", "ETL", "Databricks", "Serverless"],
) as dag:

    logger.info("Initializing Agricultural Crop Production Databricks ETL DAG")

    run_agriculture_etl_job = DatabricksRunNowOperator(
        task_id="run_agricultural_crop_production_serverless_job",
        databricks_conn_id="databricks_default",
        job_id=616190703118881  # <-- YOUR Databricks Job ID
    )

    logger.info(
        "DatabricksRunNowOperator configured with job_id=616190703118881"
    )

    run_agriculture_etl_job
