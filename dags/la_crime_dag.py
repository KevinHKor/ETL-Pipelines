from datetime import timedelta, datetime

from airflow import DAG
from airflow.decorators import task
from airflow.operators.python import PythonOperator
from etl.la_crime_extract import process_open_data, process_s3_mo_codes
from etl.la_crime_transform import transform_data
from etl.la_crime_load import load

# Create task for the DAG
@task
def extract_openData_task():
    process_open_data()

@task
def extract_s3_task():
    process_s3_mo_codes()

@task
def transform_task():
    transform_data()

@task
def load_task():
    load.load_final()
    load.load_to_dw()

# Default arguments for DAGs
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2023,6,1),
    'depends_on_past': False,
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5)
}

# Instantiate a DAG
with DAG(
    'tutorial',
    default_args=default_args,
    description='This is my first DAG example.',
    schedule=timedelta(days=1),
    catchup=False
) as dag:

    extract_openData = extract_openData_task()
    extract_s3 = extract_s3_task()
    transform = transform_task()
    load = load_task()

    extract_openData >> transform
    extract_s3 >> transform
    transform >> load