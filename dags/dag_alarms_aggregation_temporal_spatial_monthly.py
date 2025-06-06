# Nome da DAG: dag_alarms_aggregation_temporal_spatial_monthly
# Owner / responsável: CoE
# Descrição do objetivo da DAG: 
# 1. Busca en Postgres Prd los thesholds con granularidad = mes
# 2. Por cada registro de configuración de Threshold, se lanza una petición a druid adapter con el fin de obtener la consulta SQL que será ejecutada por Druid para la generación de la métrica deseada.
# 3. Se lanza la ejecución en Druid de cada consulta SQL generada en el paso anterior.
# 4. Envia cada fila del resultado obtenido kafka
# 
# Nota: 
# Las funciones usadas están guardadas en el archivo functions.py
# Usa Druid?: Si
# Principais tabelas / consultas Druid acessadas: snmp-enriched-metrics fastoss-pm-enriched-metrics
# Frequência de execução (schedule): Cada mes
# Dag Activo?: Si
# Autor: CoE
# Data de modificação: 2025-05-26
from airflow import DAG
from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator

import functions


default_args = {
    'owner': 'CoE',
    'start_date': datetime(2024, 4, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}


# Función para el control de carga
def data_load_control_kpi(**kwargs):    
    param_start_date = kwargs['start_date']
    var_datetime = datetime.strptime(param_start_date, "%Y-%m-%d %H:%M:%S.%f%z")
    result=functions.postgresData("dataAggregationTime","MONTH")
    functions.prepared_data_load(result,var_datetime,"MONTH",0)


# Función para realizar la consulta del dataframe
def query_select_datataframe(**kwargs):
    resut_json=functions.postgresData("dataAggregationTime","MONTH")
    return resut_json


# Función para ejecutar la mutación Druid Adapter
def druid_adapter_query(param_df_druid, param_start_date, **kwargs): 
    var_datetime = datetime.strptime(param_start_date, "%Y-%m-%d %H:%M:%S.%f%z")
    print(var_datetime)
             
    sql_queries_json=functions.druid_adapter_query(param_df_druid=param_df_druid,aggregationTime="MONTH",minutos=0,var_datetime=var_datetime)
    return sql_queries_json  
      
def execute_druid_adapter_query(**kwargs):
    df_druid = kwargs['param_df']
    start_date = kwargs['start_date']
    sql_queries_json = druid_adapter_query(df_druid, start_date)
    return(sql_queries_json)
  

# Función para ejecutar la consulta a Druid
def execute_druid_query(**kwargs):
    sql_queries_str = kwargs['param_sql_queries']    
    results=functions.execute_druid_query(sql_queries_str)    
    kwargs['ti'].xcom_push(key='response_texts', value=results)
         

def produce_message_to_topic_kafka(**kwargs):
    response_texts = kwargs['ti'].xcom_pull(task_ids='execute_druid_query_task', key='response_texts')
    functions.produce_message_to_topic_kafka(response_texts,2592000,"PT2592000S")
    

#Definición del Dag
dag = DAG(
    'dag_alarms_aggregation_temporal_spatial_monthly',  # Nombre de tu DAG
    default_args=default_args,
    schedule_interval='30 0 1 * *',  # Se ejecuta todos los 1 de cada mes a las 12:30 am
    catchup=False,
    tags=["alarms", "aggregation"],
)

# Definición de las tareas
start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

query_select_datataframe_task = PythonOperator(
    task_id='query_select_datataframe_task',
    python_callable=query_select_datataframe,
    dag=dag,
)

execute_druid_adapter_query_task = PythonOperator(
    task_id='execute_druid_adapter_query_task',
    python_callable=execute_druid_adapter_query,
    op_kwargs={'param_df': "{{ ti.xcom_pull(task_ids='query_select_datataframe_task')}}", 'start_date':"{{ dag_run.start_date }}"},
    provide_context=True,
    dag=dag,
)

execute_druid_query_task = PythonOperator(
    task_id='execute_druid_query_task',
    python_callable=execute_druid_query,
    op_kwargs={'param_sql_queries': "{{ ti.xcom_pull(task_ids='execute_druid_adapter_query_task')}}"},
    provide_context=True,
    dag=dag,
)

produce_messages_to_kafka_task = PythonOperator(
    task_id='produce_messages_to_kafka_task',
    python_callable=produce_message_to_topic_kafka,
    provide_context=True,
    dag=dag,
)

data_load_control_kpi_task = PythonOperator(
    task_id='data_load_control_kpi_task',
    python_callable=data_load_control_kpi,    
    op_kwargs={'start_date':"{{ dag_run.start_date }}"},
    provide_context=True,
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

# Definir el orden de las tareas
start_task >> data_load_control_kpi_task 
data_load_control_kpi_task >> query_select_datataframe_task 
query_select_datataframe_task >> execute_druid_adapter_query_task
execute_druid_adapter_query_task >> execute_druid_query_task
execute_druid_query_task >> produce_messages_to_kafka_task 
produce_messages_to_kafka_task >> end_task