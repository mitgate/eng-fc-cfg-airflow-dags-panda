# Nome da DAG: dag_alarms_aggregation_spatial_5M
# Owner / responsável: CoE
# Descrição do objetivo da DAG: 
# 1. Busca en Postgres Prd los thesholds con granularidad = 5 minutos y agregación temporal = None 
# 2. Por cada registro de configuración de Threshold, se lanza una petición a druid adapter con el fin de obtener la consulta SQL que será ejecutada por Druid para la generación de la métrica deseada.
# 3. Se lanza la ejecución en Druid de cada consulta SQL generada en el paso anterior.
# 4. Envia cada fila del resultado obtenido kafka
# Nota: 
# Este dag es una nueva versión que consulta y consolida en un json anidado las métricas históricas de las alarmas configuradas.
# Las funciones usadas están guardadas en el archivo functions_v2.py
# Usa Druid?: Si
# Principais tabelas / consultas Druid acessadas: snmp-enriched-metrics fastoss-pm-enriched-metrics
# Frequência de execução (schedule): Cada 5 minutos
# Dag Activo?: Si
# Autor: CoE
# Data de modificação: 2025-05-26
from airflow import DAG
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.utils.state import State

import json
import functions_v2 as functions

#VARIABLES
varMinutes = 5
varSeconds = 300  
varDurationIso = "PT300S"
varGranularity = "PT5M"

default_args = {
    'owner': 'CoE',
    'start_date': datetime(2024, 5, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'dagrun_timeout': timedelta(seconds=299),
    'email_on_failure': True,
    'email': ['mbenitep@emeal.nttdata.com', 'danielgerardo.escamillamontoya@nttdata.com']
}

# Definición del DAG
@dag(
    dag_id="dag_alarms_aggregation_spatial_5M",  
    default_args={"owner": "CoE"},
    #schedule_interval=None, 
    schedule_interval='4-59/5 * * * *',
    start_date=days_ago(1), 
    catchup=False, 
    tags=["alarms", "aggregation"],
    max_active_tasks = 15
)

def dag_alarms_aggregation_spatial_5M():  
    @task
    def query_select_datataframe(start_date):
        result_json = functions.postgresData("dataAggregationTimeGranularity", varGranularity)
        df_dict = json.loads(result_json)  
        return [{"row": row, "start_date": start_date} for row in df_dict]  
    
    #@task(retries=1, retry_delay=timedelta(minutes=1), execution_timeout=timedelta(seconds=299))
    @task(execution_timeout=timedelta(seconds=299))
    def process_task(data, ti=None):
        queued_time = ti.queued_dttm  # Fecha con zona horaria
        now = datetime.utcnow().replace(tzinfo=timezone.utc)  # Convertir a offset-aware

        time_in_queue = (now - queued_time).total_seconds() / 60  # Convierte a minutos    
        print("queued_time:", queued_time)
        print("now:", now)
        print("time_in_queue:", time_in_queue)
        
        if time_in_queue > 5:
            print(f"Tarea cancelada: estuvo {time_in_queue:.2f} minutos en cola.")
            return None  # No ejecuta nada si el tiempo en cola es mayor a 5 minutos
        
        # Código normal de la tarea si pasó la validación
        print("Ejecutando tarea normalmente...")
        
        row, start_date = data["row"], data["start_date"]
        var_datetime = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S.%f%z")
        json_data = json.dumps([row]) 
        
        algorithm = row["algorithm"]  
        print("algorithm:", algorithm)

        #algorithmcomparisontype = row["algorithmcomparisontype"]
        
        #functions.prepared_data_load(json_data, var_datetime, varGranularity, varMinutes)

        if algorithm == 'AVERAGE_HISTORICAL_VALUE':    
            json_list = []
            algorithmcomparisonvalue = float(row['algorithmcomparisonvalue'])  
            print("algorithmcomparisonvalue:", algorithmcomparisonvalue)

            intervaldays = int(row["intervaldays"])  
            print("intervaldays:", intervaldays)

            times = int(row["times"])  
            print("times:", times)       

            for i in range(0, max(1, times + 1)):
                print(f"Iteración {i} - {var_datetime}")

                sql_queries = functions.druid_adapter_query(json_data, aggregationTime="", minutos=varMinutes, var_datetime=var_datetime)
                results = functions.execute_druid_query(sql_queries)
                nested_json = functions.create_nested_json(results, varSeconds, varDurationIso) 
                json_list.append(nested_json)  

                if i == 0:
                    var_datetime = var_datetime - timedelta(minutes = int(algorithmcomparisonvalue)*varMinutes)                           
                elif i > 0: 
                    var_datetime = var_datetime - timedelta(days = intervaldays) 
        else:
            sql_queries = functions.druid_adapter_query(json_data, aggregationTime="", minutos=varMinutes, var_datetime=var_datetime)
            json_list = functions.execute_druid_query(sql_queries)
        
        functions.produce_message_to_topic_kafka(json_list, varSeconds, varDurationIso, algorithm)

        
    # Cada fila se procesa en su flujo completo
    start_date = "{{ dag_run.start_date }}"
    df_rows = query_select_datataframe(start_date)
    process_task.expand(data=df_rows)


# Creando la instancia del DAG
dag_instance = dag_alarms_aggregation_spatial_5M()