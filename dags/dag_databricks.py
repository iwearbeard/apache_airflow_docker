from datetime import datetime
from airflow.decorators import dag
from airflow.providers.databricks.operators.databricks import DatabricksSubmitRunOperator

default_args = {
    'owner': 'owner_name',
    'start_date': datetime(2021, 10, 14), #Ejemplo: datetime(2021, 10, 14)
    'email': ['email'],
    'email_on_failure': True,
    # 'email_on_retry': True,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=1)
}

@dag(
    dag_id='databricks',
    schedule_interval='0 7 * * 1-5', #Ejemplo:'0 7 * * 1-5' (sumar 3 horas dado que esta en UTF)
    default_args=default_args, 
    description='descripcion',
    catchup=False
)

def databricks_task():

    #Definimos parametros sobre nuestro entorno de trabajo.
    notebook_task_params = {
        'existing_cluster_id' : 'id_del_cluster',
        'notebook_task' : {
            'notebook_path': 'ruta_de_la_notebook',
        },
    }

    #Utilizamos operador de databricks para ejecutar nuestro script de manera remota.
    notebook_task = DatabricksSubmitRunOperator(
        task_id='nombre_de_la_tarea',
        databricks_conn_id='id_conexion',
        json=notebook_task_params)

    notebook_task

execute = databricks_task()