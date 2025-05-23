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
    'retry_delay': timedelta(minutes=2),
}

# Función para el control de carga
def data_load_control_kpi(**kwargs):    
    param_start_date = kwargs['start_date']
    var_datetime = datetime.strptime(param_start_date, "%Y-%m-%d %H:%M:%S.%f%z")
    result=functions.postgresData("dataAggregationTimeGranularity","PT30M")
    functions.prepared_data_load(result,var_datetime,"PT30M", 30)


# Función para realizar la consulta del dataframe
def query_select_datataframe(**kwargs):
    resut_json=functions.postgresData("dataAggregationTimeGranularity","PT30M")  
    return resut_json


# Función para ejecutar la mutación Druid Adapter
def druid_adapter_query(param_df_druid, param_start_date, **kwargs): 
    var_datetime = datetime.strptime(param_start_date, "%Y-%m-%d %H:%M:%S.%f%z")
    print(var_datetime)  

    sql_queries_json=functions.druid_adapter_query(param_df_druid=param_df_druid,aggregationTime="",minutos=30,var_datetime=var_datetime)
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
    functions.produce_message_to_topic_kafka(response_texts,1800,"PT1800S")
    

#Definición del Dag
dag = DAG(
    'dag_alarms_aggregation_spatial_30M',  # Nombre de tu DAG
    default_args=default_args,
    schedule_interval = '10-59/30 * * * *',  # Se ejecuta cada 30 min desde el min 10
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