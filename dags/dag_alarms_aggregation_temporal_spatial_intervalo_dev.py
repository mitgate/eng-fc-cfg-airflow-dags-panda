from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable


import functions_dev as functions

# Definir variables para periodicidad HORA
AGGREGATION_TIME = "HOUR" 
INTERVAL_SECONDS = 3600  
DURATION_ISO = "PT3600S" 

# Definir variables para periodicidad DIA
# AGGREGATION_TIME = "DAY" 
# INTERVAL_SECONDS = 86400  
# DURATION_ISO = "PT86400S" 

# Definir variables para periodicidad WEEK
# AGGREGATION_TIME = "WEEK" 
# INTERVAL_SECONDS = 604800  
# DURATION_ISO = "PT604800S" 

# Definir variables para periodicidad MONTH
# AGGREGATION_TIME = "MONTH" 
# INTERVAL_SECONDS = 2592000  
# DURATION_ISO = "PT2592000S" 

#alarms_email_to = Variable.get("alarms_email_to")

default_args = {
    'owner': 'CoE',
    'start_date': datetime(2025, 1, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    #'email':  alarms_email_to,  'email_on_failure': True
}

def process_time_task(execution_time, aggregation_time, interval_seconds, duration_iso, **kwargs):
    formatted_time = execution_time.strftime("%Y-%m-%d %H:%M:%S.%f%z")
    print(f"Processing time: {formatted_time}")
    
    df_druid = functions.postgresData("dataAggregationTime", aggregation_time)
    sql_queries_json = functions.druid_adapter_query(
        param_df_druid=df_druid, aggregationTime=aggregation_time, minutos=0, var_datetime=execution_time
    )
    results = functions.execute_druid_query(sql_queries_json)
    functions.produce_message_to_topic_kafka(results, interval_seconds, duration_iso)


dag = DAG(
    'dag_alarms_aggregation_dev_intervalo_temporal_spatial',
    default_args=default_args,
    schedule_interval=None,  # Se ejecuta manualmente con parámetros
    catchup=False,
    tags=["alarms", "aggregation", "dev"]
)

start_task = DummyOperator(
    task_id='start',
    dag=dag,
)

end_task = DummyOperator(
    task_id='end',
    dag=dag,
)

start_time = datetime(2025, 2, 25, 4, 10)
end_time = datetime(2025, 2, 26, 3, 10)

previous_task = start_task
current_time = start_time

while current_time <= end_time:
    task = PythonOperator(
        task_id=f'process_time_task_{current_time.strftime("%Y%m%d_%H%M%S")}',
        python_callable=process_time_task,
        op_kwargs={
            'execution_time': current_time,
            'aggregation_time': AGGREGATION_TIME,
            'interval_seconds': INTERVAL_SECONDS,
            'duration_iso': DURATION_ISO,
        },
        provide_context=True,
        dag=dag,
    )
    previous_task >> task  
    previous_task = task 
    current_time += timedelta(seconds=INTERVAL_SECONDS)

previous_task >> end_task