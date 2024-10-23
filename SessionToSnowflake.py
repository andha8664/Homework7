from airflow.decorators import task
from airflow import DAG
from airflow.models import Variable
from airflow.operators.python import get_current_context
from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

from datetime import datetime
from datetime import timedelta
import logging
import snowflake.connector

"""
This pipeline assumes that there are two other tables in your snowflake DB
 - dev.raw_data.user_session_channel
 - dev.raw_data.session_timestamp
"""

def return_snowflake_conn():

    # Initialize the SnowflakeHook
    hook = SnowflakeHook(snowflake_conn_id='snowflake_conn')

    # Execute the query and fetch results
    conn = hook.get_conn()
    return conn.cursor()


@task
def run_ctas(stage_sql, table_1_sql, table_2_sql):
    cur = return_snowflake_conn()

    try:
        cur.execute("BEGIN;")
        
        # Create stage and add url
        logging.info(stage_sql)
        cur.execute(f"{stage_sql}")

        # Copy into user_session_channel and session_timestamp tables
        logging.info(table_1_sql)
        cur.execute(f"{table_1_sql}")
        
        logging.info(table_2_sql)
        cur.execute(f"{table_2_sql}")
            
        cur.execute("COMMIT;")
    except Exception as e:
        cur.execute("ROLLBACK")
        logging.error('Failed to sql. Completed ROLLBACK!')
        raise


with DAG(
    dag_id = 'SessionToSnowflake',
    start_date = datetime(2024,10,2),
    catchup=False,
    tags=['ELT'],
    schedule = '45 2 * * *'
) as dag:
    
    stage_sql = """CREATE OR REPLACE STAGE dev.raw_data.blob_stage
    url = 's3://s3-geospatial/readonly/'
    file_format = (type = csv, skip_header = 1, field_optionally_enclosed_by = '"');
    """
    
    table_1_sql = """COPY INTO dev.raw_data.user_session_channel
    FROM @dev.raw_data.blob_stage/user_session_channel.csv; 
    """
    
    table_2_sql = """COPY INTO dev.raw_data.session_timestamp
    FROM @dev.raw_data.blob_stage/session_timestamp.csv; 
    """
    run_ctas(stage_sql, table_1_sql, table_2_sql)