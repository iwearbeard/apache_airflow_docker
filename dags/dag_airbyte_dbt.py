from datetime import datetime
from airflow.decorators import dag
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'owner_name',
    'start_date': datetime(2021, 10, 14),
    'email': ['email'],
    # 'email_on_failure': True,
    # 'email_on_retry': True,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=1)
}

@dag(
    dag_id='challenge_airbyte_dbt',
    schedule_interval='0 7 * * 1-5',
    default_args=default_args, 
    description='',
    catchup=False
)

def etl_process():

    airbyte_sync = AirbyteTriggerSyncOperator(
        task_id='airbyte_sync',
        airbyte_conn_id='airbyte_local',
        connection_id='connection_id',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /dbt_project && dbt run -m model_name'
    )

    airbyte_sync >> dbt_run

execute = etl_process()