from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.email import send_email
from kafka import KafkaConsumer
from datetime import datetime
import json
import time
import logging
import boto3


def consumer_profile():
    consumer = KafkaConsumer(
        'profiles',
        bootstrap_servers='kafka:29092',
        auto_offset_reset='earliest',
        enable_auto_commit=True,
        group_id='profiles_group',
        consumer_timeout_ms=60000  # 60 second timeout
    )

    client = boto3.client('s3')
    try:
        message_count = 0
        for message in consumer:
            message_value = json.loads(message.value.decode('utf-8'))
            response = client.put_object(
                Bucket='data.engineering.projects',
                Key='customer_details/{}.json'.format(message_value['username']),
                Body=json.dumps(message_value)
            )
            print(response)
            message_count += 1
            time.sleep(2)

        if message_count == 0:
            print("No new users found in the topic")
        else:
            print(f"Successfully processed {message_count} users")
        return message_count
    except Exception as e:
        print(f"Error processing users: {str(e)}")
        raise e
    finally:
        consumer.close()

def send_success_email(context):
    message_count = context['task_instance'].xcom_pull(task_ids='feed_profiles_to_s3')
    subject = 'New customer details data loaded to s3 ✅'
    html_content = f'''
        <h2>Data Pipeline Success Report</h2>
        <p>New customer details data successfully fetched from Kafka and loaded to s3.</p>
        <p><strong>Total users processed:</strong> {message_count}</p>
    '''
    send_email(to='marionkoki00@gmail.com', subject=subject, html_content=html_content)

def send_failure_email(context):
    subject = 'New customer details data ❌ failed to load to s3'
    html_content = 'New customer details data failed to be fetched from Kafka / loaded into s3.'
    send_email(to='marionkoki00@gmail.com', subject=subject, html_content=html_content)


default_args = {
    'owner': 'Otina',
    'start_date': datetime(2025, 5, 10)
}

dag = DAG(
    'profiles_consumer',
    schedule_interval='@daily',
    default_args=default_args,
    on_success_callback=send_success_email,
    on_failure_callback=send_failure_email,
    catchup=False
)

consume_profiles = PythonOperator(
    task_id='feed_profiles_to_s3',
    python_callable=consumer_profile,
    dag=dag
)

trigger_pipeline = TriggerDagRunOperator(
    task_id='trigger_pipeline',
    trigger_dag_id='profiles_vault_pipeline',
    wait_for_completion=False,
    dag=dag
)

consume_profiles >> trigger_pipeline
