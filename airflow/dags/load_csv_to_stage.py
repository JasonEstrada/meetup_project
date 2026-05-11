from datetime import datetime
from airflow import DAG
from airflow.sdk import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.hooks.base import BaseHook
import snowflake.connector
import os
import glob
import logging
from callbacks import on_success, on_failure

logger = logging.getLogger(__name__)

# Configuration variables
STAGE_NAME = Variable.get("MEETUP_STAGE_NAME", default="meetup_stage")
DATA_PATH = Variable.get("MEETUP_DATA_PATH", default="/opt/airflow/data")

default_args = {
    "owner": "JasonEstrada",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "on_success_callback": on_success,
    "on_failure_callback": on_failure,
}


def load_csv():
    conn_airflow = BaseHook.get_connection("snowflake_default")

    conn = snowflake.connector.connect(
        user=conn_airflow.login,
        password=conn_airflow.password,
        account=conn_airflow.extra_dejson.get("account"),
        warehouse=conn_airflow.extra_dejson.get("warehouse"),
        database=conn_airflow.extra_dejson.get("database"),
        schema=conn_airflow.extra_dejson.get("schema"),
    )

    cursor = conn.cursor()
    cursor.execute(f"CREATE STAGE IF NOT EXISTS {STAGE_NAME}")
    logger.info(f"Stage '{STAGE_NAME}' ready")

    timestamp = datetime.now().strftime("%Y%m%d%H%M%S")
    files = glob.glob(f"{DATA_PATH}/*.csv")
    
    if not files:
        logger.error(f"No CSV files found in {DATA_PATH}")
        raise FileNotFoundError(f"No CSV files found in {DATA_PATH}")
    
    logger.info(f"Found {len(files)} CSV files to upload")

    uploaded_count = 0
    for filepath in files:
        try:
            name = os.path.splitext(os.path.basename(filepath))[0]
            destination = f"@{STAGE_NAME}/{name}/{name}_{timestamp}.csv"
            cursor.execute(f"PUT file://{filepath} {destination}")
            logger.info(f"Successfully uploaded {os.path.basename(filepath)} to {destination}")
            uploaded_count += 1
        except Exception as e:
            logger.error(f"Failed to upload {filepath}: {str(e)}")
            raise

    cursor.close()
    conn.close()
    logger.info(f"Upload completed: {uploaded_count} files uploaded")


dag = DAG(
    dag_id="load_csv_to_stage",
    default_args=default_args,
    description="Uploads CSV files to Snowflake internal stage, versioned by timestamp",
    schedule="*/15 * * * *",
    catchup=False,
)

load_task = PythonOperator(
    task_id="load_csv",
    python_callable=load_csv,
    dag=dag,
)

trigger_stg = TriggerDagRunOperator(
    task_id="trigger_stage_to_tables",
    trigger_dag_id="stage_to_stg_tables",
    wait_for_completion=False,
    dag=dag,
)

load_task >> trigger_stg