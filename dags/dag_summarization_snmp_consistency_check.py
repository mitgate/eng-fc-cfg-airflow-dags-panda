# Nome da DAG: dag_summarization_snmp_consistency_check
# Owner / responsável: CoE
# Descrição do objetivo da DAG: Valida para una fecha dada la consistencia de número de registros entre la tabla Raw y la data de las tablas agregadas y reprocesa los tiempos donde se encuentra inconsistencia
# 
# Nota: 
# Las funciones usadas están guardadas en el archivo functions_summarization.py
# Usa Druid?: Si
# Principais tabelas / consultas Druid acessadas: snmp-enriched-metrics
# Frequência de execução (schedule): A demanda
# Dag Activo?: No
# Autor: CoE
# Data de modificação: 2025-05-28
import time
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from datetime import datetime, timedelta
from airflow.decorators import task

import functions_summarization as functions


# Configuración del DAG
default_args = {
    'owner': 'CoE',
    'start_date': datetime(2025, 4, 5),
    #'retries': 1,
    #'retry_delay': timedelta(minutes=5),
}

dag = DAG(
    'dag_summarization_snmp_consistency_check',
    default_args=default_args,
    schedule_interval=None,
    catchup=False,
    tags = ["summarization"],
)

with dag:
    start_task = DummyOperator(task_id='start')

    end_task = DummyOperator(
        task_id='end',
        trigger_rule="none_failed_min_one_success"
        )

    def setting_variables(param_start_date, periodicity):
        var_execution_date = datetime.strptime(param_start_date, "%Y-%m-%d %H:%M:%S.%f%z")    
        interval = functions.get_time_values (var_execution_date, "fastoss", "interval", 'DAILY', 1)
        fechas = interval.split('/')
        fecha_inicio_iso = fechas[0]
        fecha_fin_iso = fechas[1]

        fecha_inicio = datetime.fromisoformat(fecha_inicio_iso.replace('Z', '+00:00')) - timedelta(hours=3)
        fecha_fin = datetime.fromisoformat(fecha_fin_iso.replace('Z', '+00:00')) - timedelta(hours=3)

        # Formatear las fechas como cadenas
        fecha_inicio_str = fecha_inicio.strftime('%Y-%m-%d %H:%M:%S')
        fecha_fin_str = fecha_fin.strftime('%Y-%m-%d %H:%M:%S')

        return {
            'fecha_inicio_str': fecha_inicio_str,
            'fecha_fin_str': fecha_fin_str
            }
    
    @task
    def process_query(param_start_date, periodicity, temporal_table, wait_time):
        filter_mapping = {"15M": 'PT15M', "HOURLY": 'PT1H'}
        filter = filter_mapping.get(periodicity, None)
        
        fecha_variables = setting_variables(param_start_date, periodicity)
        fecha_inicio_str = fecha_variables["fecha_inicio_str"]
        fecha_fin_str = fecha_variables["fecha_fin_str"]
    
        if periodicity in ["15M", "HOURLY"]:
            query = f"""
                WITH fasts AS (
                    SELECT TIME_FLOOR("__time", '{filter}') AS "Time", COUNT(*) AS fasts_count
                    FROM "snmp-enriched-metrics"
                    WHERE "__time" >= TIMESTAMP '{fecha_inicio_str}' AND "__time" < TIMESTAMP '{fecha_fin_str}'
                        AND "reportInterval" = 300 
                        AND (CASE WHEN "isRate" = true THEN "denominator" != 0 ELSE true END)
                    GROUP BY 1
                ), hourly AS (
                    SELECT TIME_FLOOR("__time", '{filter}') AS "Time", Sum("count") AS hourly_count
                    FROM "{temporal_table}"
                    WHERE "__time" >= TIMESTAMP '{fecha_inicio_str}' AND "__time" < TIMESTAMP '{fecha_fin_str}'
                    GROUP BY 1
                )
                SELECT f."Time", f.fasts_count, h.hourly_count,
                    ABS(COALESCE(f.fasts_count, 0) - COALESCE(h.hourly_count, 0)) AS difference
                FROM fasts f
                FULL OUTER JOIN hourly h ON f."Time" = h."Time"
                WHERE COALESCE(f.fasts_count, 0) != COALESCE(h.hourly_count, 0)
            """
            print(query)
            results = functions.execute_query_druid(query)
            print(f"Tamaño de results: {len(results)} filas")

            if periodicity == "HOURLY":
                time.sleep(wait_time*60)

            if not results.empty:
                for index, row in results.iterrows():
                    print(f"Procesando fila {index + 1} / {len(results)}") 
                    fecha = datetime.strptime(row["Time"], "%Y-%m-%d %H:%M:%S")                    
                    functions.druid_create_task_summarization(fecha, periodicity, "temporal", "snmp", "interval")
                    time.sleep(wait_time*60)
        else:
            time.sleep(wait_time*60)
            fecha = datetime.strptime(fecha_inicio_str, "%Y-%m-%d %H:%M:%S")    
            functions.druid_create_task_summarization(fecha, "DAILY", "temporal", "snmp", "interval")  
    
    
    param_start_date = '2025-05-18 00:00:00.000Z'
    #param_start_date = "{{ dag_run.start_date }}"

    task_15min = process_query.override(task_id="process_15min_data")(param_start_date, '15M', 'snmp-enriched-metrics-temporal-15m', 10)
    task_hourly = process_query.override(task_id="process_hourly_data")(param_start_date, 'HOURLY', 'snmp-enriched-metrics-temporal-hourly', 10)
    task_daily = process_query.override(task_id="process_daily_data")(param_start_date, 'DAILY', 'snmp-enriched-metrics-temporal-daily-3', 20)
   
    # Flujo principal
    start_task >> task_15min >> task_hourly >> task_daily >> end_task


