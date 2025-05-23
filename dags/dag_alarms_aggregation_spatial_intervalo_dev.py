from airflow import DAG
from datetime import datetime, timedelta
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.models import Variable


import functions_dev as functions

# Definir variables para periodicidad 5MIN
AGGREGATION_TIME = "PT5M"
INTERVAL_MINUTE = 5  
DURATION_ISO = "PT300S"

# Definir variables para periodicidad 15MIN
# AGGREGATION_TIME = "PT15M"
# INTERVAL_SECONDS = 15
# DURATION_ISO = "PT900S"

# Definir variables para periodicidad 30MIN
# AGGREGATION_TIME = "PT30M"
# INTERVAL_MINUTE = 30  
# DURATION_ISO = "PT1800S"

# Definir variables para periodicidad 1H
# AGGREGATION_TIME = "PT1H"
# # INTERVAL_MINUTE = 60
# DURATION_ISO = "PT3600S" 

#alarms_email_to = Variable.get("alarms_email_to")

default_args = {
    'owner': 'CoE',
    'start_date': datetime(2025, 1, 30),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    #'email':  alarms_email_to,  'email_on_failure': True
}

def process_time_task(execution_time, aggregation_time, interval_minute, duration_iso, **kwargs):
    formatted_time = execution_time.strftime("%Y-%m-%d %H:%M:%S.%f%z")
    print(f"Processing time: {formatted_time}")
    
    df_druid = functions.postgresData("dataAggregationTimeGranularity", aggregation_time)

    sql_queries_json = functions.druid_adapter_query(
        param_df_druid=df_druid, aggregationTime="", minutos=interval_minute, var_datetime=execution_time
    )
    results = functions.execute_druid_query(sql_queries_json)
    functions.produce_message_to_topic_kafka(results, interval_minute*60, duration_iso)


dag = DAG(
    'dag_alarms_aggregation_dev_intervalo_spatial',
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
start_time = datetime(2025, 5, 7, 6, 6)
end_time = datetime(2025, 5, 7, 15, 36)

previous_task = start_task
current_time = start_time

while current_time <= end_time:
    task = PythonOperator(
        task_id=f'process_time_task_{current_time.strftime("%Y%m%d_%H%M%S")}',
        python_callable=process_time_task,
        op_kwargs={
            'execution_time': current_time,
            'aggregation_time': AGGREGATION_TIME,
            'interval_minute': INTERVAL_MINUTE,
            'duration_iso': DURATION_ISO,
        },
        provide_context=True,
        dag=dag,
    )
    previous_task >> task  
    previous_task = task 
    current_time += timedelta(seconds = INTERVAL_MINUTE * 60)

previous_task >> end_task