from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import time
import logging
from airflow.decorators import dag, task

@dag(
    dag_id="dag_decorator",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    concurrency=1
)
def dag_process():

    @task
    def first_function():
        logging.info('Starging process')
        logging.warn("Don't steal my the code")


    bash = BashOperator(
        task_id = 'bash_task',
        bash_command='echo hello world'
    )

    first_function() >> bash
        
dag = dag_process()