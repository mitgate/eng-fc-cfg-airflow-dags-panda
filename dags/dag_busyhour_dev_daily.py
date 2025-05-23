from airflow import DAG
import pandas as pd
import functionsBH
import json
import os

from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.decorators import task
from airflow.utils.task_group import TaskGroup
from airflow.utils.trigger_rule import TriggerRule
from airflow.models.taskinstance import TaskInstance
from airflow.models.dagrun import DagRun


default_args = {
    'owner': 'CoE',
    'start_date': datetime(2024, 5, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

periodicity = 'DAILY_BUSY_HOUR'
#periodicity = 'WEEKLY_BUSY_HOUR'
#periodicity = 'MONTHLY_BUSY_HOUR'

dag = DAG('dag_busyHour_dev_daily',
        default_args=default_args,
        #start_date=datetime(2024, 5, 3),
        catchup = False,
        #schedule_interval='0 1 * * *',  # Ejecutar a la 1:00 AM cada día
        schedule_interval = None,
        tags = ["busyHour"],
        max_active_tasks = 4  # Limitar a 4 tareas concurrentes
    )

with dag:
    start_task = DummyOperator(task_id='start')

    @task 
    def setting_variables(param_start_date):
        var_execution_date = datetime.strptime(param_start_date, "%Y-%m-%d %H:%M:%S.%f%z")
        #var_execution_date = datetime.strptime("2024-11-08 00:00:00", "%Y-%m-%d %H:%M:%S")  # Fecha de inicio
        #var_execution_date = datetime.strftime(param_start_date, "%Y-%m-%d %H:%M:%S")
        return var_execution_date

    @task
    def get_busyHour_configuration():
        results = functionsBH.get_busyHour_configuration((periodicity))
        df_dict = json.loads(results)
        return df_dict
    
    @task
    def create_query_groups(param_df, param_execution_date):
        results = functionsBH.create_query_groups(param_df, param_execution_date, periodicity)
        df_dict = json.loads(results)
        return [(item, index) for index, item in enumerate(df_dict)]

    @task
    def calculate_busyHour_row(row): 
        item, index = row  
        functionsBH.calculate_busyhour(item, index)

    @task
    def consolidate_output():
        functionsBH.consolidate_output_files(periodicity)
 
    @task
    def write_to_datalake():
        functionsBH.save_combined_files_to_datalake(periodicity)

    @task
    def send_to_druid():
        functionsBH.send_to_kafka(periodicity)

    setting_variables_task = setting_variables(param_start_date="{{ dag_run.start_date }}")

    get_busyHour_configuration_task  = get_busyHour_configuration()

    create_query_groups_task = create_query_groups(param_df=get_busyHour_configuration_task, param_execution_date=setting_variables_task)

    with TaskGroup("calculate_busyHour_tasks", prefix_group_id=False) as calculate_busyHour_tasks:
        calculate_busyHour_task = calculate_busyHour_row.expand(row = create_query_groups_task)

    consolidate_output_task = consolidate_output()
    #consolidate_output_task.trigger_rule = TriggerRule.ALL_DONE  # Ejecuta independientemente de fallas previas

    write_to_datalake_task = write_to_datalake()

    send_to_druid_task = send_to_druid()
    
    end_task = DummyOperator(task_id='end')

    # Definir las dependencias de las tareas
    start_task >> [setting_variables_task, get_busyHour_configuration_task]
    get_busyHour_configuration_task >> create_query_groups_task >> calculate_busyHour_tasks 
    calculate_busyHour_tasks >> consolidate_output_task 
    consolidate_output_task >> send_to_druid_task    
    send_to_druid_task >> write_to_datalake_task
    write_to_datalake_task >> end_task  
   