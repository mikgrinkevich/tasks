import subprocess
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime, timedelta

def call_load_csv():
    subprocess.run(["bash", "/opt/airflow/scripts/load_data.sh"], check=True)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2024, 7, 1)
}

dag = DAG(
    dag_id="load_csv_to_postgres",
    default_args=default_args,
    description="A simple DAG to load CSV data into PostgreSQL",
    schedule_interval="@daily",
)

load_csv_task = PythonOperator(
    task_id="load_data",
    python_callable=call_load_csv,
    dag=dag,
)