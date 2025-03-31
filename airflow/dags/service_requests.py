import os 

from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from nyc.raw_ingest import raw_ingest
from nyc.bronze_ingest import bronze_ingest


DBT_PROFILE = 'service_requests'
DBT_PROFILES_DIR = os.environ.get("DBT_PROFILES_DIR")
DBT_TARGET = 'dev'
DBT_PROJECT_DIR = '/opt/airflow/dags/nyc/dbt/service_requests'

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
    start_date=datetime(2025, 3, 27),
    catchup=True,
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

task_dbt_deps = BashOperator(
    task_id='task_dbt_deps',
    bash_command=f'cd {DBT_PROJECT_DIR} && dbt deps --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}',
    dag=dag,
)

reference_date = "{{ ds }}"
task_dbt_run = BashOperator(
    task_id='task_dbt_run',
    bash_command=f"cd {DBT_PROJECT_DIR} && dbt run --vars '{{reference_date: {reference_date}}}' --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}",
    dag=dag, 
)

task_dbt_test = BashOperator(
    task_id='task_dbt_test',
    bash_command=f'cd {DBT_PROJECT_DIR} && dbt test --profiles-dir {DBT_PROFILES_DIR} --target {DBT_TARGET}',
    dag=dag,
)

task_raw_ingest >> task_bronze_ingest >> task_dbt_deps >> task_dbt_run >> task_dbt_test
