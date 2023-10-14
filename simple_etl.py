from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python_operator import PythonOperator

def extract():
    with open('source.txt', 'r') as file:
        data = file.read()
    return data

def transform(**kwargs):
    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='extract_task')
    return data[::-1]

def load(**kwargs):
    ti = kwargs['ti']
    transformed_data = ti.xcom_pull(task_ids='transform_task')
    with open('destination.txt', 'w') as file:
        file.write(transformed_data)

default_args = {
    'owner': 'you',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG(
    'simple_etl',
    default_args=default_args,
    description='A simple ETL DAG',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 10, 14),
    catchup=False
)

extract_task = PythonOperator(
    task_id='extract_task',
    python_callable=extract,
    dag=dag
)

transform_task = PythonOperator(
    task_id='transform_task',
    python_callable=transform,
    provide_context=True,
    dag=dag
)

load_task = PythonOperator(
    task_id='load_task',
    python_callable=load,
    provide_context=True,
    dag=dag
)

extract_task >> transform_task >> load_task
