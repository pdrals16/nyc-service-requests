from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from nyc.raw_ingest import raw_ingest
from nyc.bronze_ingest import bronze_ingest

default_args = {
    'owner': 'Pedro',
    'depends_on_past': False,
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 3,
    'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'service_requests',
    default_args=default_args,
    description='Service Request API',
    schedule_interval=timedelta(days=1),
    start_date=datetime(2023, 1, 1),
    catchup=False,
    tags=['nyc'],
)

task_raw_ingest = PythonOperator(
    task_id='raw_ingest',
    python_callable=raw_ingest,
    op_kwargs={
        "limit": 1000,
        "initial_offset": 0,
        "max_retries": 3
    },
    dag=dag
)

task_bronze_ingest = PythonOperator(
    task_id='bronze_ingest',
    python_callable=bronze_ingest,
    op_kwargs={
        "table_name": "service_requests"
    },
    dag=dag
)

task_raw_ingest >> task_bronze_ingest