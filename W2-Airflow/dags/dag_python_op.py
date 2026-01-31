from airflow import DAG
from datetime import datetime,timedelta
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator

default_args = {
'owner' : 'nont',
'retries' : 5,
'retry_delay' : timedelta(minutes=2)
}

def get_name(ti):
    ti.xcom_push(key='name',value='Vinci')

def greet(ti):
    print(f"Hello !! My name is {ti.xcom_pull(task_ids='get_name',key='name')}")

with DAG(
    dag_id='nont_python_dag_v2',
    default_args=default_args,
    description='first dag',
    start_date=datetime(2025,1,29,2),
    schedule='0 0 * * *'
) as dag:
    task1 = PythonOperator(
        task_id = 'greet',
        python_callable=greet
    )

    task2 = PythonOperator(
        task_id = 'get_name',
        python_callable=get_name
    )
    
    task3 = BashOperator(
        task_id = 'third_task',
        bash_command ='echo i am task 3'
    )

    task2>>task1