from airflow import DAG
from datetime import datetime,timedelta
from airflow.providers.standard.operators.bash import BashOperator
from airflow.providers.standard.operators.python import PythonOperator
from airflow.sdk import dag, task


default_args = {
'owner' : 'nont',
'retries' : 5,
'retry_delay' : timedelta(minutes=2)
}

@dag(dag_id='nont_python_taskflow_dag_v1',
    default_args=default_args,
    description='first dag',
    start_date=datetime(2025,1,29,2),
    schedule='0 0 * * *')
def taskflow_test():

    @task()
    def get_name():
        return "Versailles"
    
    @task
    def get_age():
        return 10

    @task()
    def greet(name,age):
        print(f"Hello !! My name is {name} and I'm {age} years old")

    name=get_name()
    age=get_age()
    greet(name=name,age=age)

greet_dag = taskflow_test()