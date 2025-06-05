# Nome da DAG: dag_alarms_aggregation_spatial_5M_monitor
# Owner / responsável: CoE
# Descrição do objetivo da DAG: 
# 1. Busca en Postgres Prd los thesholds con granularidad = 5 minutos y agregación temporal = None 
# 2. Por cada registro de configuración de Threshold, se lanza una petición a druid adapter con el fin de obtener la consulta SQL que será ejecutada por Druid para la generación de la métrica deseada.
# 3. Se lanza la ejecución en Druid de cada consulta SQL generada en el paso anterior.
# 4. Envia cada fila del resultado obtenido kafka
# Nota: 
# Este dag es una nueva versión que consulta y consolida en un json anidado las métricas históricas de las alarmas configuradas.
# Las funciones usadas están guardadas en el archivo functions_v2.py
# Usa Druid?: Si
# Principais tabelas / consultas Druid acessadas: snmp-enriched-metrics / fastoss-pm-enriched-metrics
# Frequência de execução (schedule): Cada 5 minutos
# Dag Activo?: Si
# Autor: CoE
# Data de modificação: 2025-05-30
from airflow import DAG
from datetime import datetime, timedelta, timezone
from sqlalchemy import create_engine
from sqlalchemy.types import VARCHAR
from airflow.models import Variable, DagRun, TaskInstance
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.sensors.base import BaseSensorOperator
from airflow.utils.context import Context

from airflow.decorators import dag, task
from airflow.utils.task_group import TaskGroup
from airflow.utils.dates import days_ago
from airflow.utils.state import State, TaskInstanceState
from airflow.utils.db import provide_session
from airflow.exceptions import AirflowException

import json
import functions_v2 as functions
import threading
import time
import signal
import os
from sqlalchemy.orm import Session

# VARIABLES
varMinutes = 5
varSeconds = 300  
varDurationIso = "PT300S"
varGranularity = "PT5M"
DAG_TIMEOUT_SECONDS = 295

default_args = {
    'owner': 'CoE',
    'start_date': datetime(2024, 5, 26),
    'retries': 1,
    'retry_delay': timedelta(minutes=1),
    'email_on_failure': True,
    'email': ['mbenitep@emeal.nttdata.com', 'danielgerardo.escamillamontoya@nttdata.com'],
    'execution_timeout': timedelta(seconds=290)  # Timeout individual por tarea ligeramente menor
}

# Variable global para controlar el timeout
dag_start_times = {}
timeout_threads = {}

def kill_dag_execution(dag_id, run_id):
    """Función para matar la ejecución del DAG después del timeout"""
    from airflow.models import DagRun, TaskInstance
    from airflow.utils.db import provide_session
    from airflow.utils.state import TaskInstanceState, State
    
    @provide_session
    def _kill_tasks(session=None):
        try:
            print(f"TIMEOUT REACHED: Killing DAG {dag_id} run {run_id} after {DAG_TIMEOUT_SECONDS} seconds")
            
            # Obtener todas las task instances activas
            active_states = [TaskInstanceState.RUNNING, TaskInstanceState.QUEUED, TaskInstanceState.SCHEDULED]
            
            task_instances = session.query(TaskInstance).filter(
                TaskInstance.dag_id == dag_id,
                TaskInstance.run_id == run_id,
                TaskInstance.state.in_(active_states)
            ).all()
            
            print(f"Found {len(task_instances)} active tasks to kill")
            
            # Matar todas las tareas activas
            for ti in task_instances:
                print(f"Killing task: {ti.task_id} (state: {ti.state})")
                ti.set_state(TaskInstanceState.FAILED, session=session)
                
                # Intentar matar el proceso si está corriendo
                if ti.pid:
                    try:
                        os.kill(ti.pid, signal.SIGTERM)
                        time.sleep(2)
                        os.kill(ti.pid, signal.SIGKILL)
                    except (OSError, ProcessLookupError):
                        pass
            
            # Marcar el DAG run como failed
            dag_run = session.query(DagRun).filter(
                DagRun.dag_id == dag_id,
                DagRun.run_id == run_id
            ).first()
            
            if dag_run:
                dag_run.set_state(State.FAILED, session=session)
            
            session.commit()
            print(f"DAG {dag_id} execution killed due to timeout")
            
        except Exception as e:
            print(f"Error killing DAG execution: {str(e)}")
            if session:
                session.rollback()
    
    _kill_tasks()

def start_timeout_monitor(dag_id, run_id):
    """Inicia el monitor de timeout para el DAG"""
    def timeout_worker():
        time.sleep(DAG_TIMEOUT_SECONDS)
        kill_dag_execution(dag_id, run_id)
        
        # Limpiar registros
        if dag_id in dag_start_times:
            del dag_start_times[dag_id]
        if dag_id in timeout_threads:
            del timeout_threads[dag_id]
    
    # Registrar tiempo de inicio
    dag_start_times[dag_id] = time.time()
    
    # Crear y iniciar hilo de timeout
    timeout_thread = threading.Thread(target=timeout_worker)
    timeout_thread.daemon = True
    timeout_thread.start()
    
    timeout_threads[dag_id] = timeout_thread
    print(f"Timeout monitor started for DAG {dag_id} - will kill after {DAG_TIMEOUT_SECONDS} seconds")

def stop_timeout_monitor(dag_id):
    """Detiene el monitor de timeout si el DAG termina normalmente"""
    if dag_id in dag_start_times:
        elapsed = time.time() - dag_start_times[dag_id]
        print(f"DAG {dag_id} completed normally in {elapsed:.2f} seconds")
        del dag_start_times[dag_id]
    
    if dag_id in timeout_threads:
        del timeout_threads[dag_id]

# Definición del DAG
@dag(
    dag_id="dag_alarms_aggregation_spatial_5M_monitor",  
    default_args={"owner": "CoE"},
    schedule_interval='4-59/5 * * * *',
    start_date=days_ago(1), 
    catchup=False, 
    tags=["alarms", "aggregation"],
    max_active_tasks=64,
    dagrun_timeout=timedelta(seconds=300)  # Timeout de respaldo
)

def dag_alarms_aggregation_spatial_5M_monitor():     
    @task
    def init_timeout():
        """Inicializa el monitor de timeout"""
        from airflow.models import DagRun
        import airflow.utils.db
        
        # Obtener información del DAG run actual desde la base de datos
        @provide_session
        def get_current_run_info(session=None):
            # Buscar el DAG run más reciente para este DAG
            dag_run = session.query(DagRun).filter(
                DagRun.dag_id == "dag_alarms_aggregation_spatial_5M_monitor"
            ).order_by(DagRun.execution_date.desc()).first()
            
            if dag_run:
                return dag_run.dag_id, dag_run.run_id
            return None, None
        
        dag_id, run_id = get_current_run_info()
        if dag_id and run_id:
            start_timeout_monitor(dag_id, run_id)
            return f"timeout_started_{dag_id}_{run_id}"
        else:
            print("Could not find DAG run information")
            return "timeout_start_failed"
    
    @task
    def query_select_datataframe(start_date):
        """Consulta los datos y retorna lista para procesamiento dinámico"""
        try:
            result_json = functions.postgresData("dataAggregationTimeGranularity", varGranularity)
            df_dict = json.loads(result_json)  
            return [{"row": row, "start_date": start_date} for row in df_dict]
        except Exception as e:
            print(f"Error en query_select_datataframe: {str(e)}")
            raise
    
    @task
    def process_task(data):
        """Procesa cada fila de datos"""
        try:
            row, start_date = data["row"], data["start_date"]
            var_datetime = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S.%f%z")
            json_data = json.dumps([row]) 
            
            algorithm = row["algorithm"]  
            print("algorithm:", algorithm)

            if algorithm == 'AVERAGE_HISTORICAL_VALUE':    
                json_list = []
                algorithmcomparisonvalue = float(row['algorithmcomparisonvalue'])  
                print("algorithmcomparisonvalue:", algorithmcomparisonvalue)

                intervaldays = int(row["intervaldays"])  
                print("intervaldays:", intervaldays)

                times = int(row["times"])  
                print("times:", times)       

                for i in range(0, max(1, times + 1)):
                    print(f"Iteración {i} - {var_datetime}")

                    sql_queries = functions.druid_adapter_query(json_data, aggregationTime="", minutos=varMinutes, var_datetime=var_datetime)
                    results = functions.execute_druid_query(sql_queries)
                    nested_json = functions.create_nested_json(results, varSeconds, varDurationIso) 
                    json_list.append(nested_json)  

                    if i == 0:
                        var_datetime = var_datetime - timedelta(minutes = int(algorithmcomparisonvalue)*varMinutes)                           
                    elif i > 0: 
                        var_datetime = var_datetime - timedelta(days = intervaldays) 
            else:
                sql_queries = functions.druid_adapter_query(json_data, aggregationTime="", minutos=varMinutes, var_datetime=var_datetime)
                json_list = functions.execute_druid_query(sql_queries)
            
            functions.produce_message_to_topic_kafka(json_list, varSeconds, varDurationIso, algorithm)
            
        except Exception as e:
            print(f"Error en process_task: {str(e)}")
            raise

    @task
    def finalize_timeout():
        """Finaliza el monitor de timeout al completar normalmente"""
        stop_timeout_monitor("dag_alarms_aggregation_spatial_5M_monitor")
        return "timeout_stopped"

    # Flujo del DAG
    start_date = "{{ dag_run.start_date }}"
    
    # Secuencia de ejecución
    timeout_init = init_timeout()
    df_rows = query_select_datataframe(start_date)
    process_tasks = process_task.expand(data=df_rows)
    timeout_end = finalize_timeout()
    
    # Dependencias
    timeout_init >> df_rows >> process_tasks >> timeout_end

# Creando la instancia del DAG
dag_instance = dag_alarms_aggregation_spatial_5M_monitor()
