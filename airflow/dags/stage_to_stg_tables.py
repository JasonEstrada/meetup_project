from datetime import datetime
from airflow import DAG
from airflow.sdk import Variable
from airflow.providers.common.sql.operators.sql import SQLExecuteQueryOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.hooks.base import BaseHook
import snowflake.connector
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

FILES_TABLES = [
    ("categories",      "DIM_CATEGORIES"),
    ("cities",          "DIM_CITIES"),
    ("events",          "FACT_EVENTS"),
    ("groups",          "DIM_GROUPS"),
    ("groups_topics",   "BRIDGE_GROUPS_TOPICS"),
    ("members",         "DIM_MEMBERS"),
    ("members_topics",  "BRIDGE_MEMBERS_TOPICS"),
    ("topics",          "DIM_TOPICS"),
    ("venues",          "DIM_VENUES"),
]

# Configuration variables
SNOWFLAKE_CONN_ID = Variable.get("SNOWFLAKE_CONN_ID", default="snowflake_default")
STAGE_NAME = Variable.get("MEETUP_STAGE_NAME", default="meetup_stage")
FILE_FORMAT_NAME = Variable.get("MEETUP_FILE_FORMAT", default="csv_format")

conn_airflow = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
DATABASE = conn_airflow.extra_dejson.get("database")
SCHEMA = conn_airflow.extra_dejson.get("schema")
STAGE = f"{DATABASE}.{SCHEMA}.{STAGE_NAME}"
FILE_FORMAT = f"{DATABASE}.{SCHEMA}.{FILE_FORMAT_NAME}"

logger.info(f"Configuration loaded: STAGE={STAGE}, FILE_FORMAT={FILE_FORMAT}")


def get_snowflake_conn():
    return snowflake.connector.connect(
        user=conn_airflow.login,
        password=conn_airflow.password,
        account=conn_airflow.extra_dejson.get("account"),
        warehouse=conn_airflow.extra_dejson.get("warehouse"),
        database=DATABASE,
        schema=SCHEMA,
    )


def get_latest_file(file_name):
    conn = get_snowflake_conn()
    cursor = conn.cursor()
    try:
        logger.info(f"Searching for latest file: {file_name}")
        cursor.execute(f"LIST @{STAGE}/{file_name}/")
        rows = cursor.fetchall()
        if not rows:
            logger.error(f"No files found in @{STAGE}/{file_name}/")
            raise FileNotFoundError(f"No files found in @{STAGE}/{file_name}/")
        latest = sorted([row[0] for row in rows])[-1]
        relative_path = latest.split(f"{STAGE_NAME}/")[-1]
        logger.info(f"Latest file for {file_name}: {relative_path}")
        return relative_path
    except Exception as e:
        logger.error(f"Error getting latest file for {file_name}: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()


dag = DAG(
    dag_id="stage_to_stg_tables",
    default_args=default_args,
    description="Loads the most recent staged file into STG tables in Snowflake",
    schedule=None,
    catchup=False,
)

start = EmptyOperator(task_id="start", dag=dag)
end = EmptyOperator(task_id="end", dag=dag)

create_file_format = SQLExecuteQueryOperator(
    task_id="create_file_format",
    conn_id=SNOWFLAKE_CONN_ID,
    sql=f"""
        CREATE OR REPLACE FILE FORMAT {FILE_FORMAT}
        TYPE = CSV
        PARSE_HEADER = TRUE
        FIELD_OPTIONALLY_ENCLOSED_BY = '"'
        NULL_IF = ('NULL', 'null', '')
        ENCODING = 'ISO-8859-1';
    """,
    dag=dag,
)

start >> create_file_format

logger.info(f"Generating tasks for {len(FILES_TABLES)} tables")

for file_name, table in FILES_TABLES:
    stg_table = f"STG_{table}"
    latest_file = get_latest_file(file_name)
    logger.info(f"Creating tasks for {table} from {latest_file}")

    create_stg = SQLExecuteQueryOperator(
        task_id=f"create_{stg_table}",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            CREATE OR REPLACE TABLE {DATABASE}.{SCHEMA}.{stg_table}
            USING TEMPLATE (
                SELECT ARRAY_AGG(OBJECT_CONSTRUCT(*))
                FROM TABLE(
                    INFER_SCHEMA(
                        LOCATION => '@{STAGE}/{latest_file}',
                        FILE_FORMAT => '{FILE_FORMAT}'
                    )
                )
            );
        """,
        dag=dag,
    )

    load_stg = SQLExecuteQueryOperator(
        task_id=f"load_{stg_table}",
        conn_id=SNOWFLAKE_CONN_ID,
        sql=f"""
            COPY INTO {DATABASE}.{SCHEMA}.{stg_table}
            FROM @{STAGE}/{latest_file}
            FILE_FORMAT = (FORMAT_NAME = '{FILE_FORMAT}')
            MATCH_BY_COLUMN_NAME = CASE_INSENSITIVE;
        """,
        dag=dag,
    )

    create_file_format >> create_stg >> load_stg >> end

trigger_upsert = TriggerDagRunOperator(
    task_id="trigger_upsert_tables",
    trigger_dag_id="upsert_tables",
    wait_for_completion=False,
    dag=dag,
)

end >> trigger_upsert