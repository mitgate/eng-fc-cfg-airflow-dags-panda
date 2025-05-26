# Nome da DAG: dag_summarization_snmp_daily
# Owner / responsável: CoE
# Descrição do objetivo da DAG: Ejecuta una tarea index_parallel en Druid para leer datos desde snmp-enriched-metrics-temporal-hourly y agregarlos en snmp-enriched-metrics-temporal-daily-3, consolidando métricas diarias en el ambiente de producción (prd)
#
# Nota: 
# Las funciones usadas están guardadas en el archivo functions_summarization.py
# Usa Druid?: Si
# Principais tabelas / consultas Druid acessadas: snmp-enriched-metrics-temporal-hourly
# Frequência de execução (schedule): Cada día
# Dag Activo?: Si
# Autor: CoE
# Data de modificação: 2025-05-26
from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.decorators import task
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.exceptions import AirflowFailException

import functions_summarization as functions
import logging

default_args = {
    'owner': 'CoE',
    'start_date': datetime(2024, 5, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': True,
    'email': ['mbenitep@emeal.nttdata.com']
}

dag = DAG('dag_summarization_snmp_daily',
        default_args=default_args,
        #start_date=datetime(2024, 5, 3),
        catchup = False,
        #schedule_interval='40 10 * * *',  
        schedule_interval = None,
        tags = ["summarization"],
    )

with dag:
    start_task = DummyOperator(task_id='start')

    end_task = DummyOperator(
        task_id='end',
        trigger_rule="none_failed_min_one_success"
        )

    @task 
    def setting_variables(param_start_date):
        var_execution_date = datetime.strptime(param_start_date, "%Y-%m-%d %H:%M:%S.%f%z")    
        #var_execution_date = datetime.strftime(param_start_date, "%Y-%m-%d %H:%M:%S")
        return var_execution_date
    
    @task(multiple_outputs=True)
    def calculate_summarization(var_execution_date): 
        response = functions.druid_create_task_summarization(var_execution_date, "DAILY", "temporal", "snmp", "load")
        
        return {
            'task_druid_id': response['task'],
            'execution_date': var_execution_date.strftime("%Y-%m-%d %H:%M:%S")
        }
    
    def verify_and_retry_druid_task(**context):
        ti = context['ti']
        result = ti.xcom_pull(task_ids='calculate_summarization')
        task_druid_id = result['task_druid_id']
        var_execution_date = result['execution_date']

        max_intentos = 4
        intervalo_minutos = 60
        try:
            val, status = functions.verify_druid_task(task_druid_id, max_intentos, intervalo_minutos)
            print("Validación:", val, "/ Estado:", status)

            if val:
                # Tarea verificada con éxito
                context['ti'].xcom_push(key='task_status', value='success')
                return True
            
            # Si falla el primer intento, iniciar recuperación
            if status == 'FAILED':
                logging.warning("Primer intento de verificación fallido. Iniciando recuperación.")
                
                # Intento de recuperación
                recovery_response = functions.druid_create_task_summarization(var_execution_date, "DAILY", "temporal", "snmp", "load")
                val_recovery, status_recovery = functions.verify_druid_task(recovery_response['task'], max_intentos, intervalo_minutos)
                
                if val_recovery:
                    context['ti'].xcom_push(key='task_status', value='success')
                    return True
                else:
                    # Si la recuperación falla
                    raise AirflowFailException(f"Tarea de recuperación de Druid fallida. Estado: {status_recovery}")
    
            # Si la tarea original fue cancelada
            if status in ['CANCELLED', 'CANCEL_FAILED', 'CANCEL_EXCEPTION']:
                # Falla la tarea de Airflow
                raise AirflowFailException(f"Tarea de Druid cancelada o no se pudo cancelar. Estado: {status}")   
        
        except Exception as e:
            logging.error(f"Error verificando tarea Druid: {str(e)}")
            context['ti'].xcom_push(key='task_status', value='failed')
            raise AirflowFailException(f"Error en verificación de tarea: {str(e)}")
        
    verify_druid_task = PythonOperator(
        task_id='verify_druid_task',
        python_callable=verify_and_retry_druid_task,
        provide_context=True
    )
    
    #param_start_date = Variable.get("summarization_date")  
    param_start_date = "{{ dag_run.start_date }}"
    setting_variables_task = setting_variables(param_start_date)
    calculate_summarization_task = calculate_summarization(var_execution_date = setting_variables_task)
   
    # Flujo principal
    start_task >> setting_variables_task >> calculate_summarization_task >> verify_druid_task >> end_task