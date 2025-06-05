# Nome da DAG: druid_delete_segments_outlier
# Owner / responsável: Leandro
# Descrição do objetivo da DAG: ti = kwargs['ti']      data = ti.xcom_pull(task_ids='get_dates_from_druid', key='datas_do_druid')
# Usa Druid?: Sim
# Principais tabelas / consultas Druid acessadas: sys.segments
# Frequência de execução (schedule): 0 5 * * *
# Dag Activo?: 
# Autor: Leandro
# Data de modificação: 2025-05-26
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
import requests
import re
import json
import os
from operators.druid_segments_aux import SegmentsAuxDruid

DRUID_URL = Variable.get("druid_url")
DEV = False

"""
 ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='get_dates_from_druid', key='datas_do_druid')
"""
aux_segments = SegmentsAuxDruid(druid_url=DRUID_URL)

# Função pegar as taks em execução
def get_segments_outlier(**kwargs):
    ti = kwargs['ti']

    segments = aux_segments.send_quey("""
    SELECT
        "segment_id",
        "datasource",
        "start",
        "end",
        CURRENT_TIMESTAMP
    FROM sys.segments
    WHERE 
        (CAST(REPLACE("start",'T',' ') AS TIMESTAMP) >= CURRENT_TIMESTAMP + INTERVAL '1' DAY 
        AND CAST(REPLACE("end",'T',' ') AS TIMESTAMP) >= CURRENT_TIMESTAMP + INTERVAL '1' DAY) OR
        (CAST(REPLACE("start",'T',' ') AS TIMESTAMP) <= '2022-01-01T00:00:00.000Z' AND CAST(REPLACE("end",'T',' ') AS TIMESTAMP) <= '2022-01-01T00:00:00.000Z')
    """)
    kwargs['ti'].xcom_push(key='druid_segments_outlier', value=segments)
def delete_segments_outlier(**kwargs):
    ti  = kwargs['ti']
    segments = ti.xcom_pull(task_ids='get_segments_outlier', key='druid_segments_outlier')
    if len(segments) > 0:
        for segment in segments:
            segment_id = segment['segment_id']
            datasource = segment['datasource']
            aux_segments.delete(segment_id=segment_id,datasource=datasource)
    else:
        raise AirflowSkipException('Estapa pulada por não ser necessária a intervenção automática.')
default_args = {
    'owner': 'Leandro',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5) 
}

# Definindo a DAG
with DAG(
    'druid_delete_segments_outlier',
    default_args=default_args,
    description="""
    
    Executa a limpeza nos segmentos do futuro e passado, antes de 2022-01-01 e após a data atual. 
    
    """,
    schedule_interval='0 5 * * *',
    tags=["druid"],
    max_active_runs=1
) as dag:
    start_tasks = DummyOperator(task_id='start_tasks', dag=dag)

    get_segments_outlier_result = PythonOperator(
        task_id='get_segments_outlier',
        python_callable=get_segments_outlier,
        provide_context=True
    )
 
    delete_segments_outlier_result = PythonOperator(
        task_id='delete_segments_outlier',
        python_callable=delete_segments_outlier,
        provide_context=True
    )
 
    end_tasks = DummyOperator(task_id='end_tasks', dag=dag)

    # Definindo a ordem das tarefas
    start_tasks >> get_segments_outlier_result >>  delete_segments_outlier_result >> end_tasks