# # Nome da DAG: dag_alarms_aggregation_spatial_5M_test
# # Owner / responsável: CoE
# # Descrição do objetivo da DAG: 
# # 1. Busca en Postgres Dev los thesholds con granularidad = 5 minutos y agregación temporal = None 
# # 2. Por cada registro de configuración de Threshold, se lanza una petición a druid adapter con el fin de obtener la consulta SQL que será ejecutada por Druid para la generación de la métrica deseada.
# # 3. Se lanza la ejecución en Druid de cada consulta SQL generada en el paso anterior.
# # 4. Envia cada fila del resultado obtenido kafka
# # Nota: 
# # Este dag es una nueva versión que consulta y consolida en un json anidado las métricas históricas de las alarmas configuradas.
# # Las funciones usadas están guardadas en el archivo functions_dev.py
# # Usa Druid?: Si
# # Principais tabelas / consultas Druid acessadas: snmp-enriched-metrics fastoss-pm-enriched-metrics
# # Frequência de execução (schedule): Cada 5 minutos
# # Dag Activo?: No
# # Autor: CoE
# # Data de modificação: 2025-05-26
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

import pandas as pd
import json

import functions_dev as functions

# Variables
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
    dag_id="dag_alarms_aggregation_spatial_5M_test",  # Aquí se especifica el nombre
    default_args={"owner": "CoE"},
    schedule_interval=None, 
    #schedule_interval='4-59/5 * * * *',  
    start_date=days_ago(1), 
    catchup=False, 
    tags=["alarms", "aggregation"]
)


def dag_alarms_aggregation_spatial_5M_test():  # La función DAG con el mismo nombre
    @task
    def query_select_dataframe(start_date):
        result_json = functions.postgresData("dataAggregationTimeGranularity", varGranularity)
        df_dict = json.loads(result_json)  

        var_time = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S.%f%z")
        elapsed_time = (datetime.utcnow() - var_time.replace(tzinfo=None)).total_seconds() / 60

        if elapsed_time > 5:
            print(f"Cancelando ejecución: el DAG excedió 5 minutos ({elapsed_time:.2f} min).")
            return None  # Detener ejecución

        return [{"row": row, "start_date": start_date} for row in df_dict]  
    
    
    def process_tasks_group(start_date):
        with TaskGroup(group_id="alarm_processing_group") as group:
            @task(execution_timeout=timedelta(minutes=5))
            def process_task(data):
                row, start_date = data["row"], data["start_date"]
                var_datetime = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S.%f%z")
                json_data = json.dumps([row]) 

                var_time = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S.%f%z")
                elapsed_time = (datetime.utcnow() - var_time.replace(tzinfo=None)).total_seconds() / 60  
                print("time_in_queue:", elapsed_time)
                
                if elapsed_time > 5:
                    print(f"Tarea cancelada: estuvo {elapsed_time:.2f} minutos en cola.")
                    return None  # No ejecuta nada si el tiempo en cola es mayor a 5 minutos
                
                # Código normal de la tarea si pasó la validación
                print("Ejecutando tarea normalmente...")

                algorithm = row["algorithm"]  
                print("algorithm:", algorithm)

                #algorithmcomparisontype = row["algorithmcomparisontype"]
                
                sw = functions.prepared_data_load(json_data, var_datetime, "PT5M",  5)
                print("Prepared Data Load:", sw)

                if sw:
                    if algorithm in ('AVERAGE_HISTORICAL_VALUE', 'HISTORICAL_VALUE', 'SLOPE_COMPARATION'):    
                        json_list = []
                        algorithmcomparisonvalue = float(row['algorithmcomparisonvalue'])  
                        print("algorithmcomparisonvalue:", algorithmcomparisonvalue)

                        intervaldays = int(row["intervaldays"])  
                        print("intervaldays:", intervaldays)

                        if algorithm == 'AVERAGE_HISTORICAL_VALUE':
                            times = int(row["times"])  
                        else:
                            times = 1
                        print("times:", times)       

                        for i in range(0, max(1, times + 1)):
                            print(f"Iteración {i} - {var_datetime}")

                            sql_queries = functions.druid_adapter_query(json_data, aggregationTime="", minutos=5, var_datetime=var_datetime)
                            results = functions.execute_druid_query(sql_queries)
                            nested_json = functions.create_nested_json(results, 300, "PT300S") 
                            json_list.append(nested_json)  

                            if i == 0:
                                var_datetime = var_datetime - timedelta(minutes = int(algorithmcomparisonvalue)*5)                           
                            elif i > 0: 
                                var_datetime = var_datetime - timedelta(days = intervaldays) 
                    else:
                        sql_queries = functions.druid_adapter_query(json_data, aggregationTime="", minutos=5, var_datetime=var_datetime)
                        json_list = functions.execute_druid_query(sql_queries)
                    
                    functions.produce_message_to_topic_kafka(json_list, 300, "PT300S", algorithm)
            
            df_rows = query_select_dataframe(start_date)
            process_task.expand(data=df_rows)

        return group 

    # Flujo del DAG con grupo de tareas
    start_date = "{{ dag_run.start_date }}"
    process_tasks_group(start_date)

# Creando la instancia del DAG
dag_instance = dag_alarms_aggregation_spatial_5M_test()