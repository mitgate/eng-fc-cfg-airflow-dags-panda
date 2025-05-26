# Nome da DAG: dag_summarization_dev_daily
# Owner / responsável: CoE
# Descrição do objetivo da DAG: Ejecuta una tarea index_parallel en Druid para leer datos desde snmp-enriched-metrics-temporal-hourly y agregarlos en snmp-enriched-metrics-temporal-daily-3, consolidando métricas diarias en el ambiente de desarrollo (dev)
#
# Nota: 
# Las funciones usadas están guardadas en el archivo functions_summarization_dev.py
# Usa Druid?: Si
# Principais tabelas / consultas Druid acessadas: snmp-enriched-metrics-temporal-hourly
# Frequência de execução (schedule): Cada día
# Dag Activo?: No
# Autor: CoE
# Data de modificação: 2025-05-26
from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator

import functions_summarization_dev as functions

default_args = {
    'owner': 'CoE',
    'start_date': datetime(2024, 5, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('dag_summarization_dev_daily',
        default_args=default_args,
        #start_date=datetime(2024, 5, 3),
        catchup = False,
        #schedule_interval='0 1 * * *',  # Ejecutar a la 1:00 AM cada día
        schedule_interval = None,
        tags = ["summarization"],
    )

with dag:
    start_task = DummyOperator(task_id='start')
    end_task = DummyOperator(task_id='end')
    
    @task 
    def setting_variables(param_start_date):
        var_execution_date = datetime.strptime(param_start_date, "%Y-%m-%d %H:%M:%S.%f%z")    
        #var_execution_date = datetime.strftime(param_start_date, "%Y-%m-%d %H:%M:%S")
        return var_execution_date
    
    @task
    def calculate_summarization(var_execution_date): 
            functions.druid_create_task_summarization(var_execution_date,"DAILY","temporal","snmp","load")
            #functions.druid_create_task_summarization(var_execution_date,"DAILY","temporal","others","load")
    
    param_start_date=Variable.get("summarization_date")  
    setting_variables_task = setting_variables(param_start_date)
    calculate_summarization_task = calculate_summarization(var_execution_date = setting_variables_task)
   
    start_task >> setting_variables_task
    setting_variables_task >> calculate_summarization_task 
    calculate_summarization_task>>  end_task