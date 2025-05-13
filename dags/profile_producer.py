from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from kafka import KafkaProducer
from datetime import datetime
import json
import time
import logging
import requests

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def extract_data():
    url = "https://randomuser.me/api/"
    response = requests.get(url)
    if response.status_code == 200:
        return response.json()['results'][0]
    else:
        logger.error("Failed to fetch data from API")
        return None

def format_data(data):
    if not data:
        logger.error("No data to format")
        return None
    return {
        'id': data['login']['uuid'],
        'first_name': data['name']['first'],
        'last_name': data['name']['last'],
        'gender': data['gender'],
        'country': f"{data['location']['country']}, {data['location']['state']}, {data['location']['city']}",
        'address': f"{str(data['location']['street']['number'])} {data['location']['street']['name']}",
        'post_code': data['location']['postcode'],
        'latitude': data['location']['coordinates']['latitude'],
        'longitude': data['location']['coordinates']['longitude'],
        'timezone': f"{data['location']['timezone']['offset']}, {data['location']['timezone']['description']}",
        'email': data['email'],
        'username': data['login']['username'],
        'id_name': data['id']['name'],
        'id_value': data['id']['value'],
        'dob': data['dob']['date'],
        'age': data['dob']['age'],
        'registered_date': data['registered']['date'],
        'phone': data['phone'],
        'picture': data['picture']['large'],
        'nationality': data['nat']
    }


def producer_profile():
    producer = KafkaProducer(
        bootstrap_servers='kafka:29092',
        value_serializer=lambda x: json.dumps(x).encode('utf-8')
    )
    start_time = time.time()
    try:
        while time.time() <= start_time + 100:
            raw_data = extract_data()
            if raw_data:
                formatted_profile = format_data(raw_data)
                producer.send('profiles', formatted_profile)
                logger.info(f"Sent profile: {formatted_profile}")
            time.sleep(3)
    except KeyboardInterrupt:
        logger.info("Stopping producer...")
    finally:
        producer.close()
        logger.info("Producer closed successfully")

if __name__ == "__main__":
    producer_profile()


default_args = {
    'owner': 'Otina',
    'start_date': datetime(2025, 5, 10)
}

dag = DAG(
    'profile_producer',
    schedule_interval='@daily',
    default_args=default_args,
    catchup=False
)

produce_profiles = PythonOperator(
    task_id='acquire_profiles',
    python_callable=producer_profile,
    dag=dag
)

trigger_profiles = TriggerDagRunOperator(
    task_id='trigger_profile_consumer',
    trigger_dag_id='profiles_consumer',
    wait_for_completion=False,
    dag=dag
)

produce_profiles >> trigger_profiles
