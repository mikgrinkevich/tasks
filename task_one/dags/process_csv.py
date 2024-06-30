from airflow import DAG
from airflow.operators.python import PythonOperator
import subprocess
from datetime import datetime


def call_load_csv_script():
    subprocess.run(["bash", "/opt/airflow/scripts/load_csv.sh"])

dag = DAG(
    dag_id="load_csv",
    start_date=datetime(2024, 6, 30),
    schedule_interval="@daily",
)

load_csv_task = PythonOperator(
    task_id="load_csv_bash",
    python_callable=call_load_csv_script,
    dag=dag,
)
