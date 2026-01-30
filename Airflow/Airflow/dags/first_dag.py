from airflow import DAG
from datetime import datetime,timedelta
from airflow.providers.standard.operators.bash import BashOperator

default_args = {
'owner' : 'nont',
'retries' : 5,
'retry_delay' : timedelta(minutes=2)

}


with DAG(
    dag_id='first_dagv3',
    default_args=default_args,
    description='first dag',
    start_date=datetime(2025,1,29,2),
    schedule='0 0 * * *'
) as dag:
    task1 = BashOperator(
        task_id = 'first_task',
        bash_command ='echo hello world,this is the first task!'

    )

    task2 = BashOperator(
        task_id = 'second_task',
        bash_command ='echo i am task 2'

    )
    
    task3 = BashOperator(
        task_id = 'third_task',
        bash_command ='echo i am task 3'

    )
    task1>>[task2,task3]