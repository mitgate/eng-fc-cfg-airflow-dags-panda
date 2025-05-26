# Nome da DAG: dag_summarization_snmp_interval_group_daily
# Owner / responsável: CoE
# Descrição do objetivo da DAG: Reprocesa las agregaciones por grupo de Día para un periodo de tiempo estipulado
# 
# Nota: 
# Las funciones usadas están guardadas en el archivo functions_summarization.py
# Usa Druid?: Si
# Principais tabelas / consultas Druid acessadas: snmp-enriched-metrics-group-hourly
# Frequência de execução (schedule): A demanda
# Dag Activo?: No
# Autor: CoE
# Data de modificação: 2025-05-26
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import time
from airflow.models import Variable

import functions_summarization as functions

def generar_tareas_druid(fecha, **kwargs):   
    functions.druid_create_task_summarization(fecha, "DAILY", "spatial", "snmp", "interval")    

def esperar():
    time.sleep(7 * 60)  # Espera 

default_args = {
    'owner': 'CoE',
    "depends_on_past": False,
    "email_on_failure": False,
    "email_on_retry": False,
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

with DAG(
    "dag_summarization_snmp_interval_group_daily",
    default_args=default_args,
    schedule_interval=None, 
    start_date=datetime(2024, 12, 1),
    catchup=False,
    tags = ["summarization"],
) as dag:
    
    with TaskGroup("grupo_subtareas") as grupo:
        start_date = Variable.get("summarization_snmp_group_date_start")  
        end_date = Variable.get("summarization_snmp_group_date_end")           
        intervals=functions.date_creator(start_date, end_date, timedelta(days=1)) 
        num_subtareas = len(intervals)
        
        # Crear las tareas y configurarlas en grupos de 3
        previous_wait_task = None  # Para rastrear la última tarea de espera
        batch_size = 2
        num_batches = (len(intervals) + batch_size - 1) // batch_size  # Calcular el número de lotes
        
        for batch_index in range(num_batches):
            batch_intervals = intervals[batch_index * batch_size : (batch_index + 1) * batch_size]
            batch_tasks = []
            for interval in batch_intervals:
                fecha = interval.strftime("%Y%m%d_%H%M%S")
                tarea = PythonOperator(
                    task_id=f"druid_task_{batch_index}_{fecha}",
                    python_callable=generar_tareas_druid,
                    op_args=[interval],
                    provide_context=True,
                    dag=dag,
                )
                batch_tasks.append(tarea)

            if previous_wait_task:
                previous_wait_task >> batch_tasks[0]

            for i in range(1, len(batch_tasks)):
                batch_tasks[i - 1] >> batch_tasks[i]

            # Crear la tarea de espera solo si no es el último lote
            if batch_index < num_batches - 1:
                wait_task = PythonOperator(
                    task_id=f"wait_task_batch_{batch_index}",
                    python_callable=esperar,
                    dag=dag,
                )
                batch_tasks[-1] >> wait_task
                previous_wait_task = wait_task