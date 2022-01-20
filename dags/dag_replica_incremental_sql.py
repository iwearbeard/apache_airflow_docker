from datetime import datetime, timedelta
from airflow.decorators import dag, task
from airflow.providers.microsoft.mssql.hooks.mssql import MsSqlHook
from airflow.models import Variable
import pandas as pd
import pymssql

from configuracion.common import config
from configuracion.conectors.azure_hook import Azure

config = config()

def _replica_tabla(pivot, nombreTabla, pathDatalake, query, tipoPivot, nombrePivot, nombreOrigen):

    """
    Funcion para replicar datos de una tabla en SQL Server y almacenarlos en Azure Datalake.

    Parametros:
    - pivot: campo que vamos a usar como pivot para realizar el replicado de datos
    - nombreTabla: nombre de la tabla a replicar
    - pathDatalake: ruta del datalake donde queremos guardar los datos
    - query: consulta a realizar
    - tipoPivot: tipo de dato de nuestro pivot
    - nombrePivot: nombre de la columna donde se aplica ese pivot
    - nombreOrigen: nombre de la conexion de origen de los datos, declarada en IU de Airflow

    Return:
    - Lista con new_pivot, fecha_inicio, fecha_fin y name_file
    """

    fecha_inicio = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    #Fecha y nombre del File
    today = datetime.now().strftime("%Y%m%d%H%M")
    name_file = f'replica_{nombreTabla}_{today}'

    #Conexion a BD
    hook = MsSqlHook(mssql_conn_id=nombreOrigen)
    with hook.get_conn() as conn:

        sign = '>' if str(pivot[1]) != 'replica_inexistente' else '>='
        pivot = str(pivot[0])

        find = query + f" WHERE {nombrePivot} {sign} '{pivot}'"
        print(find)
            
        cursor = conn.cursor()
        cursor.execute(find)
        data = cursor.fetchall()
        title = [i[0] for i in cursor.description]
        df = pd.DataFrame(data,columns=title)

    #Se pasa a parquet
    parquet = df.to_parquet()
    
    #Se inserta a LakeHouse
    azure = Azure()
    azure.to_datalake(parquet, name_file, f'{pathDatalake}/{nombreTabla}')

    #Tomamos el nuevo campo pivot
    if not df.empty:
        column = df[f"{nombrePivot}"]
        max_pivot = column.max()
        if tipoPivot == 'datetime':
            new_pivot = max_pivot.strftime('%Y-%m-%dT%H:%M:%S.%f')[:-3]
        elif tipoPivot == 'integer':
            new_pivot = str(int(max_pivot))
    else:
        new_pivot = pivot

    del df
    del parquet

    fecha_fin = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

    return [new_pivot, fecha_inicio, fecha_fin, name_file]


default_args = {
    'owner': '<<nombre_del_owner>>',
    'start_date': '<<fecha_inicio>>', #Ejemplo: datetime(2021, 10, 14)
    'email': ['<<lista_de_mails>>'],
    'email_on_failure': True,
    # 'email_on_retry': True,
    # 'retries': 1,
    # 'retry_delay': timedelta(minutes=1)
}

@dag(
    dag_id='<<nombre_del_dag>>',
    schedule_interval='<<cron>>', #Ejemplo:'0 7 * * 1-5' (sumar 3 horas dado que esta en UTF)
    default_args=default_args, 
    description='<<descripcion>>',
    catchup=False
)

def _function_dag():

    #Variables definidas
    origen = '<<origen>>'
    nombreOrigen = '<<nombreOrigen>>'
    nombreTabla = '<<nombreTabla>>'
    nombrePivot = '<<nombrePivot>>'
    tipoPivot = '<<tipoPivot>>'
    query = """
        <<query>>
    """
    pathDatalake = '<<pathDatalake>>'
    unidadNegocio = '<<unidadNegocio>>'

    @task
    def _pivot_task():

        """
        Funcion que se encarga de buscar el pivot necesario para realizar el replicado de datos.
        """

        azure_db = Variable.get("azure_secret", deserialize_json=True)
        conexion_origen = pymssql.connect(azure_db['address_server'],
                                    azure_db['user_name'],
                                    azure_db['password'],
                                    azure_db['name_db'])

        with conexion_origen.cursor() as cursor:

            find = f"""SELECT TOP(1) MAX(valorPivot), archivoDestino
                            FROM control_AirFlow
                            WHERE unidadNegocio = '{unidadNegocio}' AND nombreTabla = '{nombreTabla}'
                            GROUP BY archivoDestino, idEjecucion
                            ORDER BY idEjecucion desc
                    """
            cursor.execute(find)
            pivot = cursor.fetchall()

            return pivot[0]
        
    @task
    def _replica_task(pivot):
        """
        Funcion que recibe el pivot de 'pivot_task' por medio de XCOME y lo utiliza para ejecutar nuestra funcion principal.
        """
        return _replica_tabla(pivot, nombreTabla, pathDatalake, query, tipoPivot, nombrePivot, nombreOrigen)

    @task
    def _new_pivot_task(list_replica:list, **kwargs):

        """
        Funcion encargada de insertar el registro de auditoria de nuestra ejecucion a nivel local, como tambien de registrar
        el siguiente pivot a utilizar en la proxima replicacion de datos.

        Parametros:
        - list_replica: recibe una lista con new_pivot, fecha_inicio, fecha_fin y name_file provenientes de nuestra funcion principal
        """

        context = kwargs
        run_id = context['dag_run'].run_id
        now = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        new_pivot = list_replica[0]
        fecha_inicio = list_replica[1]
        fecha_fin = list_replica[2]
        nombre_file = list_replica[3]

        azure_db = Variable.get("azure_secret", deserialize_json=True)
        conexion_origen = pymssql.connect(azure_db['address_server'],
                                    azure_db['user_name'],
                                    azure_db['password'],
                                    azure_db['name_db'])

        with conexion_origen.cursor() as cursor:

            insert = f"""INSERT INTO control_AirFlow
                        VALUES ('{origen}',
                                '{nombreTabla}',
                                '1',
                                '{new_pivot}',
                                '1',
                                '{fecha_inicio}',
                                '{fecha_fin}',
                                '{nombre_file}.parquet',
                                '{now}',
                                '{run_id}',
                                '{unidadNegocio}'
                        )
                    """

            cursor.execute(insert)
            conexion_origen.commit()

    task_pivot = _pivot_task()
    list_response = _replica_task(task_pivot)
    _new_pivot_task(list_response)

etl_replica = _function_dag()