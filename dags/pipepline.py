from airflow import DAG
from datetime import datetime
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
import time

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 13),
}

dag = DAG(
    dag_id='profiles_vault_pipeline',
    default_args=default_args,
    description='Profiles Vault Pipeline',
    schedule_interval='@daily',
    catchup=False
)

check_stage = SnowflakeOperator(
    task_id='list_stage_files',
    sql='LIST @my_s3_stage/customer_details;',
    snowflake_conn_id='admin',
    warehouse='COMPUTE_WH',
    database='PROFILES_VAULT',
    schema='BRONZE',
    dag=dag
)

refresh_pipeline = SnowflakeOperator(
    task_id='refresh_for_pipeline',
    sql='ALTER PIPE PROFILES_VAULT.BRONZE.profiles_pipe REFRESH;',
    snowflake_conn_id='admin',
    warehouse='COMPUTE_WH',
    database='PROFILES_VAULT',
    schema='BRONZE',
    dag=dag
)

wait_after_pipe = PythonOperator(
    task_id='wait_40_seconds',
    python_callable=lambda: time.sleep(90),
    dag=dag
)

trigger_medallion_profiles = TriggerDagRunOperator(
    task_id='trigger_medallion_profiles',
    trigger_dag_id='medallion_profile_dimensions',
    wait_for_completion=False,
    dag=dag
)

check_stage >> refresh_pipeline >> wait_after_pipe >> trigger_medallion_profiles
