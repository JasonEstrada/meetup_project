from datetime import datetime
from airflow import DAG
from airflow.sdk import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
import logging
from callbacks import on_success, on_failure

logger = logging.getLogger(__name__)

default_args = {
    "owner": "JasonEstrada",
    "start_date": datetime(2024, 1, 1),
    "retries": 1,
    "on_success_callback": on_success,
    "on_failure_callback": on_failure,
}

# Configuration variables
SNOWFLAKE_CONN_ID = Variable.get("SNOWFLAKE_CONN_ID", default="snowflake_default")
S3_STAGE_NAME = Variable.get("MEETUP_S3_STAGE_NAME", default="s3_stage")

conn_airflow = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
DATABASE = Variable.get("SNOWFLAKE_DATABASE", default=conn_airflow.extra_dejson.get('database'))
SCHEMA = Variable.get("SNOWFLAKE_SCHEMA", default=conn_airflow.extra_dejson.get('schema'))
S3_STAGE = f"{DATABASE}.{SCHEMA}.{S3_STAGE_NAME}"

logger.info(f"Configuration loaded: S3_STAGE={S3_STAGE}")

TABLES = [
    "DIM_CATEGORIES",
    "DIM_CITIES",
    "FACT_EVENTS",
    "DIM_GROUPS",
    "BRIDGE_GROUPS_TOPICS",
    "DIM_MEMBERS",
    "BRIDGE_MEMBERS_TOPICS",
    "DIM_TOPICS",
    "DIM_VENUES",
]

logger.info(f"Configured {len(TABLES)} tables for export to S3")

with DAG(
    dag_id="export_to_s3",
    default_args=default_args,
    description="Export final tables from Snowflake to S3",
    schedule=None,
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")
    
    logger.info(f"Creating export tasks for {len(TABLES)} tables to S3")

    for table in TABLES:
        export = SQLExecuteQueryOperator(
            task_id=f"export_{table}",
            conn_id=SNOWFLAKE_CONN_ID,
            sql=f"""
                -- Exporting table {table} to S3 stage
                COPY INTO @{S3_STAGE}/{table}/
                FROM {DATABASE}.{SCHEMA}.{table}
                FILE_FORMAT = (
                    TYPE = CSV
                    COMPRESSION = NONE
                    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
                    NULL_IF = ()
                )
                HEADER = TRUE
                OVERWRITE = TRUE
                SINGLE = FALSE;
            """,
        )

        start >> export >> end