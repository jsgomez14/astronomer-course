from airflow import DAG
from airflow.operators.dummy import DummyOperator
from airflow.operators.python import PythonOperator
from airflow.sensors.filesystem import FileSensor
from airflow.operators.bash import BashOperator
from airflow.models.baseoperator import chain,cross_downstream
from datetime import datetime,timedelta


default_args = {
        'retries' : 5,
        'retry_delay' : timedelta(minutes=5)
}

def _downloading_data(ds,ti,my_param):
    with open('/tmp/my_file.txt','w') as f:
        f.write('my_data')
    ti.xcom_push(key='my_key',value='Yanse')
    return my_param

def _checking_data(ti):
    my_xcom=ti.xcom_pull(key='return_value', task_ids=['downloading_data'])
    yanse_xcom=ti.xcom_pull(key='my_key', task_ids=['downloading_data'])
    print(my_xcom,yanse_xcom)

def _failure(context):
    print("On callback failure")
    print(context)

with  DAG(
        dag_id = 'simple_dag',
        schedule_interval='*/30 * * * *', #datetime's timedelta object can be used to
        start_date=datetime(2022,1,1),
        catchup = False, #Backfilling dack runs
        max_active_runs=2, # Limit parallel dag runs
        default_args=default_args 
        ) as dag:

    downloading_data = PythonOperator(
        task_id='downloading_data',
        python_callable=_downloading_data,
        op_kwargs={'my_param':42}
    )

    checking_data = PythonOperator(
        task_id='checking_data',
        python_callable=_checking_data,
    )

    #Sensor Operator
    waiting_for_data = FileSensor(
        task_id='waiting_for_data',
        fs_conn_id='fs_default',
        filepath='my_file.txt',
    )

    #Sensor Operator
    processing_data = BashOperator(
        task_id='processing_data',
        bash_command='exit 1',
        on_failure_callback=_failure
    )

    #Dummy

    task_1 = DummyOperator(
        task_id = 'task_1',
    )

    cross_downstream([downloading_data,task_1] , [checking_data,processing_data] )
    # task_2 = DummyOperator(
    #     task_id = 'task_2',
    # )

    # task_3 = DummyOperator(
    #     task_id = 'task_3',
    # )