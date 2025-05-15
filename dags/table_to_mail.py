from airflow import DAG
from airflow.providers.snowflake.operators.snowflake import SnowflakeOperator
from airflow.operators.python import PythonOperator
from airflow.operators.email_operator import EmailOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from datetime import datetime
import boto3
import botocore.exceptions
import os
import gzip

default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 5, 13),
}

dag = DAG(
    dag_id='profiles_csv_to_mail',
    default_args=default_args,
    description='Profiles Gold table to mail',
    schedule_interval='@daily',
    catchup=False
)

unload_gold_data = """
    COPY INTO @my_s3_stage/gold_tables/final_customer_table.csv.gz
    FROM (
        SELECT * FROM PROFILES_VAULT.GOLD.customer_details ORDER BY customer_tracker
    )
    FILE_FORMAT = (
    TYPE = CSV
    ENCODING = 'UTF8'
    FIELD_DELIMITER = ','
    FIELD_OPTIONALLY_ENCLOSED_BY = '"'
    COMPRESSION = GZIP
)
HEADER = TRUE
SINGLE = FALSE
OVERWRITE = TRUE;
"""

def download_and_unzip_file(**kwargs):
    s3 = boto3.client('s3')
    bucket_name = 'data.engineering.projects'
    s3_key = 'gold_tables/final_customer_table.csv.gz_0_0_0.csv.gz'
    local_gz_path = '/tmp/final_customer_table.csv.gz_0_0_0.csv.gz'
    local_csv_path = '/tmp/final_customer_table.csv'

    # Download the gzipped file
    try:
        s3.download_file(bucket_name, s3_key, local_gz_path)
    except botocore.exceptions.ClientError as e:
        if e.response['Error']['Code'] == '404':
            raise FileNotFoundError(f"The file '{s3_key}' does not exist in bucket '{bucket_name}'.")
        else:
            raise

    # Re-read the file with surrogate escape to avoid errors
    with gzip.open(local_gz_path, 'rt', encoding='utf-8', errors='surrogateescape') as f_in:
        content = f_in.read()

    # Optional: Save it back with proper encoding
    with open(local_csv_path, 'w', encoding='utf-8', errors='surrogateescape') as f_out:
        f_out.write(content)

    # Optional: clean up .gz file
    os.remove(local_gz_path)

    # Push the path to next task via XCom
    kwargs['ti'].xcom_push(key='csv_file_path', value=local_csv_path)


def send_email_with_attachment(**kwargs):
    ti = kwargs['ti']
    csv_file_path = ti.xcom_pull(task_ids='download_csv', key='csv_file_path')

    send_csv_email = EmailOperator(
        task_id='send_csv_report',
        to='marionkoki00@gmail.com',
        subject='Gold Layer Customer Data ✅',
        html_content='Attached is the final gold layer customer CSV file exported from Snowflake.',
        files=[csv_file_path],
        dag=kwargs['dag']
    )

    send_csv_email.execute(kwargs)


export_gold_data_to_s3 = SnowflakeOperator(
    task_id='export_gold_data_to_s3',
    sql=unload_gold_data,
    snowflake_conn_id='admin',
    dag=dag
)

download_csv = PythonOperator(
    task_id='download_csv',
    python_callable=download_and_unzip_file,
    dag=dag
)

email_task = PythonOperator(
    task_id='send_final_csv_report',
    python_callable=send_email_with_attachment,
    provide_context=True,
    dag=dag
)

export_gold_data_to_s3 >> download_csv >> email_task