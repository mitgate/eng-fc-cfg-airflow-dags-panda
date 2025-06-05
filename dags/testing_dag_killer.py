from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.bash import BashOperator
from airflow.utils.task_group import TaskGroup
from airflow.exceptions import AirflowException
from airflow.models import TaskInstance
from airflow.utils.state import TaskInstanceState
from airflow.utils.context import Context
from airflow.configuration import conf
import time
import logging
import signal
import os
import psutil
import threading
from concurrent.futures import ThreadPoolExecutor

# Configuración por defecto del DAG
default_args = {
    'owner': 'data_team',
    'depends_on_past': False,
    'start_date': datetime(2024, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,  # Sin reintentos para evitar prolongar la ejecución
    'retry_delay': timedelta(minutes=1),
}

# Variable global para controlar la terminación
DAG_KILLER_ACTIVE = False
DAG_START_TIME = None

def inicializar_temporizador_dag(**context):
    """Inicializa el temporizador global del DAG"""
    global DAG_START_TIME, DAG_KILLER_ACTIVE
    DAG_START_TIME = datetime.now()
    DAG_KILLER_ACTIVE = True
    
    logging.info(f"DAG iniciado a las: {DAG_START_TIME}")
    logging.info("Temporizador de 295 segundos activado")
    
    # Iniciar el hilo del killer en background
    killer_thread = threading.Thread(target=dag_killer_monitor, args=(context,))
    killer_thread.daemon = True
    killer_thread.start()
    
    return "temporizador_iniciado"

def dag_killer_monitor(context):
    """Monitor que mata el DAG después de 295 segundos"""
    global DAG_KILLER_ACTIVE
    
    tiempo_limite = 295  # segundos
    tiempo_verificacion = 5  # verificar cada 5 segundos
    
    while DAG_KILLER_ACTIVE:
        tiempo_transcurrido = (datetime.now() - DAG_START_TIME).total_seconds()
        
        if tiempo_transcurrido >= tiempo_limite:
            logging.critical(f"¡TIMEOUT! DAG ha excedido {tiempo_limite} segundos. Iniciando terminación forzada.")
            terminar_dag_forzadamente(context)
            break
        
        logging.info(f"Tiempo transcurrido: {tiempo_transcurrido:.1f}s / {tiempo_limite}s")
        time.sleep(tiempo_verificacion)

def terminar_dag_forzadamente(context):
    """Termina forzadamente todas las tareas en ejecución del DAG"""
    global DAG_KILLER_ACTIVE
    DAG_KILLER_ACTIVE = False
    
    try:
        dag_run = context['dag_run']
        session = context['session'] if 'session' in context else None
        
        logging.critical("=== INICIANDO TERMINACIÓN FORZADA DEL DAG ===")
        
        # Obtener todas las task instances del DAG run actual
        task_instances = dag_run.get_task_instances()
        
        tareas_terminadas = []
        for ti in task_instances:
            if ti.state in [TaskInstanceState.RUNNING, TaskInstanceState.QUEUED]:
                logging.critical(f"Terminando tarea forzadamente: {ti.task_id} (Estado: {ti.state})")
                
                # Intentar kill del proceso si está corriendo
                if ti.pid:
                    kill_process_tree(ti.pid)
                
                # Marcar la tarea como fallida
                ti.set_state(TaskInstanceState.FAILED)
                tareas_terminadas.append(ti.task_id)
        
        logging.critical(f"Tareas terminadas forzadamente: {tareas_terminadas}")
        
        # Intentar kill de procesos por nombre (fallback)
        kill_airflow_tasks_by_name()
        
        logging.critical("=== TERMINACIÓN FORZADA COMPLETADA ===")
        
    except Exception as e:
        logging.error(f"Error durante terminación forzada: {str(e)}")

def kill_process_tree(pid):
    """Mata un proceso y todos sus hijos"""
    try:
        parent = psutil.Process(pid)
        children = parent.children(recursive=True)
        
        # Terminar procesos hijos primero
        for child in children:
            try:
                logging.warning(f"Terminando proceso hijo: {child.pid}")
                child.kill()
            except psutil.NoSuchProcess:
                pass
        
        # Terminar proceso padre
        try:
            logging.warning(f"Terminando proceso padre: {pid}")
            parent.kill()
        except psutil.NoSuchProcess:
            pass
            
    except psutil.NoSuchProcess:
        logging.warning(f"Proceso {pid} ya no existe")
    except Exception as e:
        logging.error(f"Error matando proceso {pid}: {str(e)}")

def kill_airflow_tasks_by_name():
    """Mata procesos de Airflow por nombre (fallback)"""
    try:
        for proc in psutil.process_iter(['pid', 'name', 'cmdline']):
            try:
                cmdline = ' '.join(proc.info['cmdline'] or [])
                if 'dag_con_taskgroup_timeout_killer' in cmdline and 'airflow' in cmdline:
                    logging.warning(f"Matando proceso de Airflow: {proc.info['pid']}")
                    proc.kill()
            except (psutil.NoSuchProcess, psutil.AccessDenied):
                pass
    except Exception as e:
        logging.error(f"Error en kill by name: {str(e)}")

def tarea_rapida_con_monitor(**context):
    """Tarea rápida con monitoreo de timeout"""
    logging.info("Ejecutando tarea rápida...")
    
    # Simular trabajo con verificaciones periódicas de timeout
    duracion_total = 30  # 30 segundos
    intervalos = 6  # verificar cada 5 segundos
    tiempo_por_intervalo = duracion_total / intervalos
    
    for i in range(intervalos):
        if not DAG_KILLER_ACTIVE:
            logging.warning("Tarea rápida interrumpida por timeout global")
            raise AirflowException("Tarea interrumpida por timeout del DAG")
        
        logging.info(f"Tarea rápida - progreso: {((i+1)/intervalos)*100:.1f}%")
        time.sleep(tiempo_por_intervalo)
    
    logging.info("Tarea rápida completada exitosamente")
    return "tarea_rapida_completada"

def tarea_lenta_con_monitor(**context):
    """Tarea lenta con monitoreo de timeout"""
    logging.info("Ejecutando tarea lenta...")
    
    # Simular trabajo pesado con verificaciones de timeout
    duracion_total = 240  # 4 minutos (para probar que funciona dentro del límite)
    # Para probar timeout, cambiar a duracion_total = 350 (5+ minutos)
    intervalos = 24  # verificar cada 10 segundos
    tiempo_por_intervalo = duracion_total / intervalos
    
    for i in range(intervalos):
        if not DAG_KILLER_ACTIVE:
            logging.warning("Tarea lenta interrumpida por timeout global")
            raise AirflowException("Tarea interrumpida por timeout del DAG")
        
        tiempo_transcurrido_dag = (datetime.now() - DAG_START_TIME).total_seconds()
        logging.info(f"Tarea lenta - progreso: {((i+1)/intervalos)*100:.1f}% | Tiempo DAG: {tiempo_transcurrido_dag:.1f}s")
        time.sleep(tiempo_por_intervalo)
    
    logging.info("Tarea lenta completada exitosamente")
    return "tarea_lenta_completada"

def verificar_estado_final(**context):
    """Verifica el estado final del DAG y reporta estadísticas"""
    global DAG_KILLER_ACTIVE
    DAG_KILLER_ACTIVE = False  # Desactivar el killer
    
    tiempo_total = (datetime.now() - DAG_START_TIME).total_seconds()
    logging.info(f"=== RESUMEN DE EJECUCIÓN DEL DAG ===")
    logging.info(f"Tiempo total de ejecución: {tiempo_total:.1f} segundos")
    logging.info(f"Límite configurado: 295 segundos")
    
    if tiempo_total > 295:
        logging.critical(f"DAG excedió el tiempo límite por {tiempo_total - 295:.1f} segundos")
        return "dag_timeout_excedido"
    else:
        logging.info(f"DAG completado dentro del tiempo límite (restaban {295 - tiempo_total:.1f} segundos)")
        return "dag_completado_a_tiempo"

def cleanup_recursos(**context):
    """Limpia recursos y procesos residuales"""
    logging.info("Ejecutando limpieza de recursos...")
    
    # Limpiar variables globales
    global DAG_KILLER_ACTIVE
    DAG_KILLER_ACTIVE = False
    
    # Verificar procesos residuales
    try:
        kill_airflow_tasks_by_name()
    except Exception as e:
        logging.warning(f"Error en limpieza: {str(e)}")
    
    logging.info("Limpieza de recursos completada")
    return "cleanup_completado"

# Crear el DAG
dag = DAG(
    'dag_con_taskgroup_timeout_killer',
    default_args=default_args,
    description='DAG con terminación forzada a los 295 segundos',
    schedule_interval=timedelta(days=1),
    catchup=False,
    max_active_runs=1,  # Solo una ejecución activa
    tags=['timeout', 'killer', 'taskgroup'],
)

# Tarea de inicialización del temporizador
inicializador = PythonOperator(
    task_id='inicializar_temporizador',
    python_callable=inicializar_temporizador_dag,
    dag=dag,
)

# TaskGroup con las tareas críticas
with TaskGroup(
    group_id='grupo_tareas_criticas',
    tooltip='Grupo de tareas con kill automático a los 295s',
    dag=dag
) as taskgroup:
    
    tarea_1 = PythonOperator(
        task_id='tarea_rapida',
        python_callable=tarea_rapida_con_monitor,
        execution_timeout=timedelta(seconds=300),  # Timeout individual de seguridad
        dag=dag,
    )
    
    tarea_2 = PythonOperator(
        task_id='tarea_lenta',
        python_callable=tarea_lenta_con_monitor,
        execution_timeout=timedelta(seconds=300),  # Timeout individual de seguridad
        dag=dag,
    )
    
    # Secuencia dentro del TaskGroup
    tarea_1 >> tarea_2

# Verificador de estado final
verificador_final = PythonOperator(
    task_id='verificar_estado_final',
    python_callable=verificar_estado_final,
    trigger_rule='all_done',  # Se ejecuta siempre
    dag=dag,
)

# Limpieza final
limpieza_final = PythonOperator(
    task_id='cleanup_final',
    python_callable=cleanup_recursos,
    trigger_rule='all_done',  # Se ejecuta siempre
    dag=dag,
)

# Definir las dependencias del DAG
inicializador >> taskgroup >> verificador_final >> limpieza_final
