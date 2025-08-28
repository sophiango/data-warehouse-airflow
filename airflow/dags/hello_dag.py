from datetime import datetime
from airflow import DAG
from airflow.operators.python import PythonOperator

def say_hello():
    print("👋 Hello world!")

with DAG(
    dag_id="hello_dag",
    start_date=datetime(2025,8,28),
    schedule_interval=None,
    catchup=False,
    tags=["demo"]
) as dag:
    heelo_task=PythonOperator(
        task_id="say_hello",
        python_callable=say_hello
    )