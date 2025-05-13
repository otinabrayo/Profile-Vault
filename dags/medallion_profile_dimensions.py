from airflow import DAG
from airflow.utils.email import send_email
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import os

AWS_KEY = os.getenv('AWS_ACCESS_KEY_ID')
AWS_SECRET = os.getenv('AWS_SECRET_ACCESS_KEY')
BUCKET = os.getenv('S3_BUCKET_NAME')

default_args = {
    'owner': 'Brian',
    'start_date': datetime(2025, 4, 2, 12, 0)
}

def send_success_email(context):
    subject = 'New customer details data loaded to snowflake ✅'
    html_content = '''
        <h2>Profile Vault 🔑 Data Pipeline Success Report </h2>
        <p>New customers data successfully fetched from Kafka, loaded and transformed for analysis in Snowflake.</p>
        <p>Check the attached file below and snowflake table for the data.</p>
    '''
    send_email(to='marionkoki00@gmail.com', subject=subject, html_content=html_content)

def send_failure_email(context):
    subject = 'New customer details data ❌ failed to load to snowflake ❄'
    html_content = 'New customer details data failed to be fetched from Kafka / loaded into Snowflake.'
    send_email(to='marionkoki00@gmail.com', subject=subject, html_content=html_content)

dag = DAG(
    dag_id='medallion_profile_dimensions',
    default_args=default_args,
    catchup=False,
    schedule='@daily',
    on_success_callback=send_success_email,
    on_failure_callback=send_failure_email,
)

load_bronze_table = SnowflakeOperator(
    task_id='bronze_table_making',
    sql='./sqls/bronze.sql',
    snowflake_conn_id='admin',
    dag=dag
)

load_silver_table = SnowflakeOperator(
    task_id='silver_table_making',
    sql='./sqls/silver.sql',
    snowflake_conn_id='admin',
    dag=dag
)

load_gold_table = SnowflakeOperator(
    task_id='gold_table_making',
    sql='./sqls/gold.sql',
    snowflake_conn_id='admin',
    dag=dag
)

# load_corrupted_data = SnowflakeOperator(
#     task_id='crypto_corrupted_data_making',
#     sql='./sqls/corrupted_data.sql',
#     snowflake_conn_id='snowflake_conn_id',
#     dag=dag
# )

# trigger_final_table_to_mail = TriggerDagRunOperator(
#     task_id='trigger_final_table_dag',
#     trigger_dag_id='gold_crypto_layer',
#     wait_for_completion=False,
#     dag=dag
# )

load_bronze_table >> load_silver_table >> load_gold_table
