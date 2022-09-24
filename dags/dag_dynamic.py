from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime
import time
import logging

def first_function():
    logging.info('Starging process')
    logging.warn("Don't steal my the code")

with DAG(
    dag_id="dag_dynamic",
    schedule_interval=None,
    start_date=datetime(2021, 1, 1),
    catchup=False,
    concurrency=1
) as dag:

    logger = PythonOperator(task_id="print_params",
                    python_callable=first_function,
                    provide_context=True,
                    dag=dag)

    for task in ['first','second','therd']:

        dynamic_task = PythonOperator(task_id=f"python_task_{task}",
                        dag=dag,
                        python_callable=lambda: time.sleep(30))


        logger >> dynamic_task