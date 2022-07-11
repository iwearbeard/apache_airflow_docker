from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator
from airflow.operators.bash_operator import BashOperator

default_args = {
    'owner': 'airbyte_dbt',
    'start_date': datetime(2021, 10, 14),
    'email': [''],
    # 'email_on_failure': True,
    # 'email_on_retry': True,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='challenge_airbyte_dbt',
    schedule_interval='0 7 * * 1-5',
    default_args=default_args, 
    description='',
    catchup=False
) as dag:

    airbyte_sync = AirbyteTriggerSyncOperator(
        task_id='airbyte_sync',
        airbyte_conn_id='airbyte_local',
        connection_id='b24870b0-9f44-48cf-b3dc-d3a9ff44f385',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    dbt_run = BashOperator(
        task_id='dbt_run',
        bash_command='cd /opt/airflow/dbt-tool/go_dbt && dbt run -m api_exchange'
    )

    airbyte_sync >> dbt_run