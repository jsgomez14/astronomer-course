from airflow import DAG
from airflow.operators.dummy import DummyOperator
from datetime import datetime


with  DAG(
        dag_id = 'simple_dag',
        schedule_interval='*/10 * * * *', #datetime's timedelta object can be used to
        start_date=datetime(2022,1,1),
        catchup = True, #Backfilling dack runs
        max_active_runs=2, # Limit parallel dag runs
        ) as dag:
    task_1 = DummyOperator(
        task_id = 'task_1'
    )