from datetime import datetime
from airflow import DAG
from airflow.sdk import Variable
from airflow.operators.python import PythonOperator
from airflow.providers.standard.operators.empty import EmptyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
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

# Configuration variables
SNOWFLAKE_CONN_ID = Variable.get("SNOWFLAKE_CONN_ID", default="snowflake_default")

conn_airflow = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
DATABASE = Variable.get("SNOWFLAKE_DATABASE", default=conn_airflow.extra_dejson.get('database'))
SCHEMA = Variable.get("SNOWFLAKE_SCHEMA", default=conn_airflow.extra_dejson.get('schema'))

logger.info(f"Configuration loaded: DATABASE={DATABASE}, SCHEMA={SCHEMA}")

TABLES_CONFIG = [
    {
        "table": "DIM_CATEGORIES",
        "primary_key": ["category_id"],
        "columns": [
            ("category_id",     "category_id"),
            ("category_name",   "category_name"),
            ("shortname",       "shortname"),
            ("sort_name",       "sort_name"),
        ],
    },
    {
        "table": "DIM_CITIES",
        "primary_key": ["city_id"],
        "columns": [
            ("city_id",                  "city_id"),
            ("city",                     "city"),
            ("country",                  "country"),
            ("distance",                 "distance"),
            ("latitude",                 "latitude"),
            ("localized_country_name",   "localized_country_name"),
            ("longitude",                "longitude"),
            ("member_count",             "member_count"),
            ("ranking",                  "ranking"),
            ("state",                    "state"),
            ("zip",                      "zip"),
        ],
    },
    {
        "table": "FACT_EVENTS",
        "primary_key": ["event_id"],
        "columns": [
            ("event_id",                    "event_id"),
            ("created",                     "created"),
            ("description",                 "description"),
            ("duration",                    "duration"),
            ("event_url",                   "event_url"),
            ("fee.accepts",                 "fee_accepts"),
            ("fee.amount",                  "fee_amount"),
            ("fee.currency",                "fee_currency"),
            ("fee.description",             "fee_description"),
            ("fee.label",                   "fee_label"),
            ("fee.required",                "fee_required"),
            ("group.created",               "group_created"),
            ("group.group_lat",             "group_lat"),
            ("group.group_lon",             "group_lon"),
            ("group_id",                    "group_id"),
            ("group.join_mode",             "group_join_mode"),
            ("group.name",                  "group_name"),
            ("group.urlname",               "group_urlname"),
            ("group.who",                   "group_who"),
            ("headcount",                   "headcount"),
            ("how_to_find_us",              "how_to_find_us"),
            ("maybe_rsvp_count",            "maybe_rsvp_count"),
            ("event_name",                  "event_name"),
            ("photo_url",                   "photo_url"),
            ("rating.average",              "rating_average"),
            ("rating.count",                "rating_count"),
            ("rsvp_limit",                  "rsvp_limit"),
            ("event_status",                "event_status"),
            ("event_time",                  "event_time"),
            ("updated",                     "updated"),
            ("utc_offset",                  "utc_offset"),
            ("venue.address_1",             "venue_address_1"),
            ("venue.address_2",             "venue_address_2"),
            ("venue.city",                  "venue_city"),
            ("venue.country",               "venue_country"),
            ("venue_id",                    "venue_id"),
            ("venue.lat",                   "venue_lat"),
            ("venue.localized_country_name","venue_localized_country"),
            ("venue.lon",                   "venue_lon"),
            ("venue.name",                  "venue_name"),
            ("venue.phone",                 "venue_phone"),
            ("venue.repinned",              "venue_repinned"),
            ("venue.state",                 "venue_state"),
            ("venue.zip",                   "venue_zip"),
            ("visibility",                  "visibility"),
            ("waitlist_count",              "waitlist_count"),
            ("why",                         "why"),
            ("yes_rsvp_count",              "yes_rsvp_count"),
        ],
    },
    {
        "table": "DIM_GROUPS",
        "primary_key": ["group_id"],
        "columns": [
            ("group_id",                        "group_id"),
            ("category_id",                     "category_id"),
            ("category.name",                   "category_name"),
            ("category.shortname",              "category_shortname"),
            ("city_id",                         "city_id"),
            ("city",                            "city"),
            ("country",                         "country"),
            ("created",                         "created"),
            ("description",                     "description"),
            ("group_photo.base_url",            "group_photo_base_url"),
            ("group_photo.highres_link",        "group_photo_highres_link"),
            ("group_photo.photo_id",            "group_photo_photo_id"),
            ("group_photo.photo_link",          "group_photo_photo_link"),
            ("group_photo.thumb_link",          "group_photo_thumb_link"),
            ("group_photo.type",                "group_photo_type"),
            ("join_mode",                       "join_mode"),
            ("lat",                             "lat"),
            ("link",                            "link"),
            ("lon",                             "lon"),
            ("members",                         "members"),
            ("group_name",                      "group_name"),
            ("organizer.member_id",             "organizer_member_id"),
            ("organizer.name",                  "organizer_name"),
            ("organizer.photo.base_url",        "organizer_photo_base_url"),
            ("organizer.photo.highres_link",    "organizer_photo_highres_link"),
            ("organizer.photo.photo_id",        "organizer_photo_photo_id"),
            ("organizer.photo.photo_link",      "organizer_photo_photo_link"),
            ("organizer.photo.thumb_link",      "organizer_photo_thumb_link"),
            ("organizer.photo.type",            "organizer_photo_type"),
            ("rating",                          "rating"),
            ("state",                           "state"),
            ("timezone",                        "timezone"),
            ("urlname",                         "urlname"),
            ("utc_offset",                      "utc_offset"),
            ("visibility",                      "visibility"),
            ("who",                             "who"),
        ],
    },
    {
        "table": "BRIDGE_GROUPS_TOPICS",
        "primary_key": ["group_id", "topic_id"],
        "columns": [
            ("group_id",    "group_id"),
            ("topic_id",    "topic_id"),
            ("topic_key",   "topic_key"),
            ("topic_name",  "topic_name"),
        ],
    },
    {
        "table": "DIM_MEMBERS",
        "primary_key": ["member_id", "group_id"],
        "columns": [
            ("member_id",       "member_id"),
            ("group_id",        "group_id"),
            ("bio",             "bio"),
            ("city",            "city"),
            ("country",         "country"),
            ("hometown",        "hometown"),
            ("joined",          "joined"),
            ("lat",             "lat"),
            ("link",            "link"),
            ("lon",             "lon"),
            ("member_name",     "member_name"),
            ("state",           "state"),
            ("member_status",   "member_status"),
            ("visited",         "visited"),
        ],
    },
    {
        "table": "BRIDGE_MEMBERS_TOPICS",
        "primary_key": ["member_id", "topic_id"],
        "columns": [
            ("member_id",   "member_id"),
            ("topic_id",    "topic_id"),
            ("topic_key",   "topic_key"),
            ("topic_name",  "topic_name"),
        ],
    },
    {
        "table": "DIM_TOPICS",
        "primary_key": ["topic_id"],
        "columns": [
            ("topic_id",        "topic_id"),
            ("description",     "description"),
            ("link",            "link"),
            ("members",         "members"),
            ("topic_name",      "topic_name"),
            ("urlkey",          "urlkey"),
            ("main_topic_id",   "main_topic_id"),
        ],
    },
    {
        "table": "DIM_VENUES",
        "primary_key": ["venue_id"],
        "columns": [
            ("venue_id",                "venue_id"),
            ("venue_name",              "venue_name"),
            ("address_1",               "address_1"),
            ("city",                    "city"),
            ("country",                 "country"),
            ("distance",                "distance"),
            ("lat",                     "lat"),
            ("localized_country_name",  "localized_country_name"),
            ("lon",                     "lon"),
            ("rating",                  "rating"),
            ("rating_count",            "rating_count"),
            ("state",                   "state"),
            ("zip",                     "zip"),
            ("normalised_rating",       "normalised_rating"),
        ],
    },
]


def get_snowflake_conn():
    conn_airflow = BaseHook.get_connection(SNOWFLAKE_CONN_ID)
    return snowflake.connector.connect(
        user=conn_airflow.login,
        password=conn_airflow.password,
        account=conn_airflow.extra_dejson.get('account'),
        warehouse=conn_airflow.extra_dejson.get('warehouse'),
        database=conn_airflow.extra_dejson.get('database'),
        schema=conn_airflow.extra_dejson.get('schema'),
    )


def build_merge_sql(database, schema, table, primary_key, columns):
    table_full = f"{database}.{schema}.{table}"
    table_stg = f"{database}.{schema}.STG_{table}"

    join_condition = " AND ".join([
        f'target.{col_final} = source."{col_stg}"'
        for col_stg, col_final in columns if col_final in primary_key
    ])

    cols_no_pk = [(col_stg, col_final) for col_stg, col_final in columns if col_final not in primary_key]
    update_set = ",\n            ".join([
        f'target.{col_final} = source."{col_stg}"'
        for col_stg, col_final in cols_no_pk
    ])

    insert_cols = ", ".join([col_final for _, col_final in columns])
    insert_vals = ", ".join([f'source."{col_stg}"' for col_stg, _ in columns])

    return f"""
        MERGE INTO {table_full} AS target
        USING {table_stg} AS source
        ON {join_condition}
        WHEN MATCHED THEN UPDATE SET
            {update_set}
        WHEN NOT MATCHED THEN INSERT ({insert_cols})
            VALUES ({insert_vals});
    """

def upsert_table(table, primary_key, columns, **context):
    """
    Performs MERGE operation from STG table to final table.
    Includes validation and detailed logging.
    """
    stg_table = f"STG_{table}"
    conn = get_snowflake_conn()
    cursor = conn.cursor()

    try:
        logger.info(f"Starting UPSERT for {table}")
        
        # Validate STG table exists and has data
        try:
            cursor.execute(f"SELECT COUNT(*) as cnt FROM {DATABASE}.{SCHEMA}.{stg_table}")
            row_count = cursor.fetchone()
            if row_count and row_count[0] > 0:
                logger.info(f"[{table}] STG table '{stg_table}' has {row_count[0]} rows ready for merge")
            else:
                logger.warning(f"[{table}] STG table '{stg_table}' is empty")
        except Exception as e:
            logger.error(f"[{table}] Could not validate STG table: {str(e)}")
            raise
        
        # Build and execute MERGE
        sql = build_merge_sql(DATABASE, SCHEMA, table, primary_key, columns)
        logger.info(f"[{table}] Executing MERGE statement")
        logger.debug(f"[{table}] SQL:\n{sql}")

        cursor.execute(sql)
        logger.info(f"[{table}] MERGE completed successfully")
        
    except Exception as e:
        logger.error(f"[{table}] Error during UPSERT: {str(e)}")
        raise
    finally:
        cursor.close()
        conn.close()


with DAG(
    dag_id="upsert_tables",
    default_args=default_args,
    description="MERGE from STG tables to final tables in Snowflake",
    schedule=None, 
    catchup=False,
) as dag:

    start = EmptyOperator(task_id="start")
    end = EmptyOperator(task_id="end")

    for config in TABLES_CONFIG:
        task = PythonOperator(
            task_id=f"upsert_{config['table']}",
            python_callable=upsert_table,
            op_kwargs={
                "table": config["table"],
                "primary_key": config["primary_key"],
                "columns": config["columns"],
            },
        )

        start >> task >> end

trigger_export = TriggerDagRunOperator(
    task_id="trigger_export_to_s3",
    trigger_dag_id="export_to_s3",
    wait_for_completion=False,
    dag=dag,
)

end >> trigger_export