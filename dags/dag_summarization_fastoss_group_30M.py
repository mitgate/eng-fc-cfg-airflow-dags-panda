# Nome da DAG: dag_summarization_fastoss_group_30M
# Owner / responsável: CoE
# Descrição do objetivo da DAG: Ejecuta una tarea index_parallel en Druid para leer datos desde fastoss-pm-enriched-metrics y agregarlos en fastoss-pm-enriched-metrics-group-30m cada 30 minutos, procesando métricas con granularidad de 15 minutos en el ambiente de producción (prd)
# 
# Nota: 
# Las funciones usadas están guardadas en el archivo functions_summarization.py
# Usa Druid?: Si
# Principais tabelas / consultas Druid acessadas: fastoss-pm-enriched-metrics
# Frequência de execução (schedule): Cada 30 minutos
# Dag Activo?: Si
# Autor: CoE
# Data de modificação: 2025-05-26
from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator

import functions_summarization as functions

default_args = {
    'owner': 'CoE',
    'start_date': datetime(2024, 5, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('dag_summarization_fastoss_group_30M',
        default_args=default_args,
        #start_date=datetime(2024, 5, 3),
        catchup = False,
        schedule_interval='5,35 * * * *',  
        tags = ["summarization", "group"],
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
            functions.druid_create_task_summarization(var_execution_date, "30M", "spatial", "fastoss", "load")
            
    #param_start_date = Variable.get("summarization_date")  
    param_start_date = "{{ dag_run.start_date }}"
    setting_variables_task = setting_variables(param_start_date)
    calculate_summarization_task = calculate_summarization(var_execution_date = setting_variables_task)
   
    start_task >> setting_variables_task
    setting_variables_task >> calculate_summarization_task 
    calculate_summarization_task>>  end_task