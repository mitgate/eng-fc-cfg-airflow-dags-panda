# Nome da DAG: dag_summarization_fastoss_hourly
# Owner / responsável: CoE
# Descrição do objetivo da DAG: Ejecuta una tarea index_parallel en Druid para leer datos desde fastoss-pm-enriched-metrics-temporal-30m y agregarlos en fastoss-pm-enriched-metrics-temporal-hourly, consolidando métricas cada hora en el ambiente de producción (prd)
#
# Nota: 
# Las funciones usadas están guardadas en el archivo functions_summarization.py
# Usa Druid?: Si
# Principais tabelas / consultas Druid acessadas: fastoss-pm-enriched-metrics-temporal-30m
# Frequência de execução (schedule): Cada hora
# Dag Activo?: Si
# Autor: CoE
# Data de modificação: 2025-05-26
from airflow import DAG
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.decorators import task

from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.operators.python import BranchPythonOperator, PythonOperator
from airflow.exceptions import AirflowFailException

import functions_summarization as functions
import logging


default_args = {
    'owner': 'CoE',
    'start_date': datetime(2024, 5, 7),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
}

dag = DAG('dag_summarization_fastoss_hourly',
        default_args=default_args,
        #start_date=datetime(2024, 5, 3),
        catchup = False,
        schedule_interval = None, #'15 * * * *',  
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
        response = functions.druid_create_task_summarization(var_execution_date, "HOURLY", "temporal", "fastoss", "load")
        
        var_delta_date = var_execution_date - timedelta(hours=0)
        is_last_interval = (var_delta_date.hour == 3)
        print("var_delta_date", var_delta_date)
        print("is_last_interval:", is_last_interval)

        return {
            'task_druid_id': response['task'],
            'is_last_interval': is_last_interval,
            'execution_date': var_execution_date
        }
    
    def verify_and_retry_druid_task(**context):
        ti = context['ti']
        result = ti.xcom_pull(task_ids='calculate_summarization')
        task_druid_id = result['task_druid_id']
        var_execution_date = result['execution_date']

        print("Tarea:", task_druid_id)
        max_intentos = 8
        intervalo_minutos = 5
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
                recovery_response = functions.druid_create_task_summarization(var_execution_date, "HOURLY", "temporal", "fastoss", "load")
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
        
    def determine_trigger_dag(**context):
        ti = context['ti']
        calc_result = ti.xcom_pull(task_ids='calculate_summarization')
        task_status = ti.xcom_pull(key='task_status')
        
        # Verificar si la tarea tuvo éxito y es el último intervalo
        if task_status == 'success' and calc_result['is_last_interval']:
            return 'trigger_daily_dag'
        else:
            return 'end'
        
    verify_druid_task = PythonOperator(
        task_id='verify_druid_task',
        python_callable=verify_and_retry_druid_task,
        provide_context=True
    )
    
    determine_trigger = BranchPythonOperator(
        task_id='determine_trigger',
        python_callable=determine_trigger_dag,
        provide_context=True
    )
        
    trigger_daily_dag = TriggerDagRunOperator(
        task_id='trigger_daily_dag',
        trigger_dag_id='dag_summarization_fastoss_daily', 
        conf={
            'interval': '1D',
            'execution_date': '{{ ti.xcom_pull(task_ids="calculate_summarization")["execution_date"] }}'
        },
        wait_for_completion = False
    )
            
    #param_start_date = Variable.get("summarization_date")  
    param_start_date = "{{ dag_run.start_date }}"
    setting_variables_task = setting_variables(param_start_date)
    calculate_summarization_task = calculate_summarization(var_execution_date = setting_variables_task)
   
    # Flujo principal
    start_task >> setting_variables_task >> calculate_summarization_task >> verify_druid_task >> determine_trigger
    
    # Rutas de flujo
    determine_trigger >> trigger_daily_dag >> end_task
    determine_trigger >> end_task