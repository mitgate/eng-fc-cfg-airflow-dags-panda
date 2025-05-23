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
    'email_on_failure': True,
    'email': ['mbenitep@emeal.nttdata.com']
}

dag = DAG('dag_summarization_snmp_15M',
        default_args=default_args,
        #start_date=datetime(2025, 3, 3),
        catchup = False,
        schedule_interval  ='1,16,31,46 * * * *',  # Se ejecuta cada 15 min
        #schedule_interval = None,
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
        response = functions.druid_create_task_summarization(var_execution_date ,"15M", "temporal", "snmp", "load")
        
        minute_block = (var_execution_date.minute // 15) * 15
        is_last_interval = (minute_block == 0)
        
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

        max_intentos = 8
        intervalo_minutos = 3
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
                recovery_response = functions.druid_create_task_summarization(var_execution_date, "15M", "temporal", "snmp", "load")
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
            return 'trigger_hourly_dag'
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
        
    trigger_hourly_dag = TriggerDagRunOperator(
        task_id='trigger_hourly_dag',
        trigger_dag_id='dag_summarization_snmp_hourly', 
        conf={
            'interval': '1H',
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
    determine_trigger >> trigger_hourly_dag >> end_task
    determine_trigger >> end_task