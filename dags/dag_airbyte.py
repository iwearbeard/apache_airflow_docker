from datetime import datetime
from airflow import DAG
from airflow.utils.dates import days_ago
from airflow.providers.airbyte.operators.airbyte import AirbyteTriggerSyncOperator

default_args = {
    'owner': 'airbyte',
    'start_date': datetime(2021, 10, 14),
    'email': ['lista de mails'],
    # 'email_on_failure': True,
    # 'email_on_retry': True,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=1)
}

with DAG(
    dag_id='airbyte_test',
    schedule_interval='0 7 * * 1-5',
    default_args=default_args, 
    description='execute airbyte sync',
    catchup=False
) as dag:

    money_to_json = AirbyteTriggerSyncOperator(
        task_id='airbyte_money_json',
        airbyte_conn_id='airbyte_local',
        connection_id='9a538305-e3ce-4f38-8f88-87792a33fe7c',
        asynchronous=False,
        timeout=3600,
        wait_seconds=3
    )

    money_to_json
