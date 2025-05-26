# Nome da DAG: functions_summarization
# Owner / responsável: CoE
# Descrição do objetivo da DAG: Módulo de funciones en Python para el procesamiento de DAGs de Sumarización en el ambiente desarrollo
# Usa Druid?: Si
# Principais tabelas / consultas Druid acessadas: 
# Frequência de execução (schedule): 
# Dag Activo?: 
# Autor: CoE
# Data de modificação: 2025-05-26

#from sqlalchemy import text
#from sqlalchemy import create_engine
#from confluent_kafka import Producer

from airflow.models import Variable
from datetime import datetime, timedelta
#from airflow.providers.http.hooks.http import HttpHook

import requests
import json
import time
import jaydebeapi 
import pandas as pd


def verify_druid_task(task_id, max_intentos, intervalo_minutos):
    print("task_druid_id: ", task_id)
    intervalo_segundos = intervalo_minutos * 60
    url_druid = Variable.get("url_druid_task")    

    time.sleep(intervalo_segundos)

    for intento in range(max_intentos):
        try:
            response = requests.get(f"{url_druid}/{task_id}/status")
            if response.status_code == 200:
                status = response.json().get('status', {}).get('status')
                print("Intento: ", intento + 1)
                print("Task Id Status: ", status)                
                if status == 'SUCCESS':
                    return True, status
                elif status == 'FAILED':
                    print(f"La tarea en Druid falló: {task_id}")
                    return False, status
                elif status in ['WAITING', 'RUNNING', 'PENDING']:
                    time.sleep(intervalo_segundos)
                else:
                    print(f"Estado desconocido: {status}")
                    return False, 'UNKNOWN'
            else:
                print(f"Error al consultar estado: {response.status_code}")
                return False, 'ERROR'
        except Exception as e:
            print(f"Excepción al verificar estado en Druid: {e}")
            return False
    
    # Si se agota el tiempo, intentar cancelar la tarea
    print(f"Se agotó el tiempo de espera para la tarea: {task_id}")    
    try:
        # Intentar cancelar la tarea
        cancel_url = f"{url_druid}/{task_id}/cancel"
        cancel_response = requests.post(cancel_url)
        
        if cancel_response.status_code == 200:
            print(f"Tarea {task_id} cancelada exitosamente")
            return False, 'CANCELLED'
        else:
            print(f"No se pudo cancelar la tarea {task_id}. Código de estado: {cancel_response.status_code}")
            return False, 'CANCEL_FAILED'
    
    except Exception as e:
        print(f"Error al intentar cancelar la tarea {task_id}: {str(e)}")
        return False, 'CANCEL_EXCEPTION'


def druid_create_task_summarization(date, periodicity, agregationType, source, process):
    #url_druid = Variable.get("url_druid_task_dev")    
    url_druid = Variable.get("url_druid_task")       
    if (agregationType == "spatial") and (periodicity in ["15M", "30M"]):   
        url_druid = "https://druid.apps.ocp-01.tdigital-vivo.com.br/druid/v2/sql/statements"

    headers = {"Content-Type": "application/json"} 

    intervalo = [0]  
    if (process == "load") and (source != "snmp"):
        intervalo = [0, 1, 3] 

    for i in intervalo:  
        if agregationType == "temporal": 
            data = druid_temporal_summarization(date, periodicity, source, process, i)
        else:
            data = druid_spatial_summarization(date, periodicity, source, process, i)
        
        print("json: ", data) 
        
        response = requests.post(url_druid, headers=headers, data=data)
        if response.status_code != 200:
            raise Exception(f"Error en la ingesta: {response.content}")
                        
        print("respuesta: ", response.json())         
    
    return response.json()
        

def create_json (periodicity, dataSource_d, queryGranularity, segmentGranularity, reportInterval, granularityInterval, dataSource_o, interval):
    # Mapeo para definir el filtro según la periodicidad
    filter_mapping = {"15M": 300, "30M": 900}
    filter = filter_mapping.get(periodicity, None)
    
    # Dimensiones y exclusiones comunes
    common_dimensions = [
        {"type": "string", "name": "managedObjectId", "multiValueHandling": "SORTED_ARRAY","createBitmapIndex": True},
        {"type": "string", "name": "additionalDn", "multiValueHandling": "SORTED_ARRAY", "createBitmapIndex": True},
        {"type": "string", "name": "dn", "multiValueHandling": "SORTED_ARRAY", "createBitmapIndex": True},
        {"type": "string", "name": "vendorId", "multiValueHandling": "SORTED_ARRAY", "createBitmapIndex": True},
        {"type": "string", "name": "reportInterval", "multiValueHandling": "SORTED_ARRAY", "createBitmapIndex": True},
        {"type": "string", "name": "equipmentTypeId", "multiValueHandling": "SORTED_ARRAY", "createBitmapIndex": True},
        {"type": "string", "name": "equipmentId", "multiValueHandling": "SORTED_ARRAY", "createBitmapIndex": True},
        {"type": "string", "name": "sourceSystem", "multiValueHandling": "SORTED_ARRAY", "createBitmapIndex": True},
        {"type": "string", "name": "functionId", "multiValueHandling": "SORTED_ARRAY", "createBitmapIndex": True},
        {"type": "string", "name": "locationId", "multiValueHandling": "SORTED_ARRAY", "createBitmapIndex": True},
        {"type": "json", "name": "enrichment", "formatVersion": 5},
        {"type": "string", "name": "networkGroups", "multiValueHandling": "SORTED_ARRAY", "createBitmapIndex": True},
        {"type": "string", "name": "pollingRuleId", "multiValueHandling": "SORTED_ARRAY", "createBitmapIndex": True},
        {"type": "string", "name": "formulaId", "multiValueHandling": "SORTED_ARRAY", "createBitmapIndex": True},
        {"type": "string", "name": "metricId", "multiValueHandling": "SORTED_ARRAY", "createBitmapIndex": True},
        {"type": "string", "name": "isRate", "multiValueHandling": "SORTED_ARRAY", "createBitmapIndex": True},
        {"type": "string", "name": "granularityInterval", "multiValueHandling": "SORTED_ARRAY", "createBitmapIndex": True},
    ]
    
    if periodicity in ["15M", "30M"]:
        dimension_exclusions = [
            "__time",
            "count",
            "value",
            "numerator",
            "denominator",
            "sum_value",
            "max_value",
            "min_value",            
            "sum_numerator",
            "max_numerator",
            "min_numerator",
            "sum_denominator",               
            "max_denominator",
            "min_denominator",      
            "values_reservoir" 
        ]

        metrics_spec = [
            {"type": "count", "name": "count"},
            {"type": "doubleSum", "name": "sum_value", "fieldName": "value"},
            {"type": "doubleMax", "name": "max_value", "fieldName": "value"},
            {"type": "doubleMin", "name": "min_value", "fieldName": "value"}, 
            {"type": "doubleSum", "name": "sum_numerator", "fieldName": "numerator"},
            {"type": "doubleMax", "name": "max_numerator", "fieldName": "numerator"},
            {"type": "doubleMin", "name": "min_numerator", "fieldName": "numerator"}, 
            {"type": "doubleSum", "name": "sum_denominator", "fieldName": "denominator"},
            {"type": "doubleMax", "name": "max_denominator", "fieldName": "denominator"},
            {"type": "doubleMin", "name": "min_denominator", "fieldName": "denominator"}, 
            {"type": "doublesReservoir", "name": "values_reservoir", "fieldName": "value", "maxReservoirSize": 1024}
        ]
        
        inputSource = {
                "type": "druid",
                "dataSource": dataSource_o,
                "interval": interval,
                "filter": {"type": "selector", "dimension": "reportInterval", "value": filter}
            }

        maxRowsPerSegment = 8000000
        maxTotalRows= 8000000
        bitmap = {"type": "roaring"}

        inputFormat= {
                    "type": "json",
                    "keepNullColumns": False,
                    "assumeNewlineDelimited": False,
                    "useJsonNodeReader": False
                }
        
        partitionsSpec = None 
        stringDictionaryEncoding = {"type": "frontCoded", "bucketSize": 8, "formatVersion": 1}        
        force_guaranteed_rollup = False

        #inputFormat = None
        #stringDictionaryEncoding = {"type": "utf8"}
        #force_guaranteed_rollup = True

        maxInputSegmentBytesPerTask =  "150Mi"
        maxNumSegments = 20

        transform_spec = [
            {"type": "expression", "name": "reportInterval", "expression": reportInterval},
            {"type": "expression", "name": "granularityInterval", "expression": granularityInterval},
            {"type": "expression", "name": "value", "expression": "if (isRate == 'true' && denominator != 0, numerator/denominator, value)"}
         ]

        filter_spec = {
          "type": "not",
          "field": {
            "type": "selector",
            "dimension": "value",
            "value": "null"
            }
        }
    else:
        dimension_exclusions = [
            "__time", 
            "count", 
            "sum_value", 
            "max_value", 
            "min_value",
            "sum_numerator", 
            "max_numerator",
            "min_numerator",
            "sum_denominator",
            "max_denominator",
            "min_denominator",  
            "values_reservoir"  
        ]

        metrics_spec = [
            {"type": "doubleSum", "name": "count", "fieldName": "count" },
            {"type": "doubleSum", "name": "sum_value", "fieldName": "sum_value"},
            {"type": "doubleMax", "name": "max_value", "fieldName": "max_value"},
            {"type": "doubleMin", "name": "min_value", "fieldName": "min_value"},
            {"type": "doubleSum", "name": "sum_numerator", "fieldName": "sum_numerator"},
            {"type": "doubleMax", "name": "max_numerator", "fieldName": "max_numerator"},
            {"type": "doubleMin", "name": "min_numerator", "fieldName": "min_numerator"},
            {"type": "doubleSum", "name": "sum_denominator", "fieldName": "sum_denominator"},
            {"type": "doubleMax", "name": "max_denominator", "fieldName": "max_denominator"},
            {"type": "doubleMin", "name": "min_denominator", "fieldName": "min_denominator"}, 
            {"type": "doublesReservoir", "name": "values_reservoir", "fieldName": "values_reservoir", "maxReservoirSize": 1024}
        ]

        inputSource = {
                    "type": "druid",
                    "dataSource": dataSource_o,
                    "interval": interval
                }

        maxInputSegmentBytesPerTask =  "250Mi"
        maxNumSegments = 10

        inputFormat = None
        maxRowsPerSegment = 5000000
        maxTotalRows = None

        # partitionsSpec = {
        #             "type": "hashed",
        #             "numShards": None,
        #             "partitionDimensions": [],
        #             "partitionFunction": "murmur3_32_abs",
        #             "maxRowsPerSegment": 5000000
        #         }
       
        partitionsSpec = {
            "type": "range",
            "partitionDimensions": [
                "managedObjectId",
                "additionalDn",
                "dn"
            ],
            "assumeGrouped": False,
            "targetRowsPerSegment": 8000000
        }   
        
        bitmap = {"type": "frontCoded", "bucketSize": 8, "formatVersion": 1}
        stringDictionaryEncoding = {"type": "utf8"}
        force_guaranteed_rollup = True

        if periodicity == 'HOURLY':
            transform_spec = [
                {"type": "expression", "name": "reportInterval", "expression": reportInterval},
                {"type": "expression", "name": "granularityInterval", "expression": granularityInterval}
            ]
        elif periodicity=='DAILY': 
             transform_spec = [
                {"type": "expression", "name": "reportInterval", "expression": reportInterval},
                {"type": "expression", "name": "granularityInterval", "expression": granularityInterval},
                {"type": "expression", "name": "__time", "expression": "timestamp_floor(__time, 'P1D', timestamp_shift(__time, 'PT-3H', 1, 'UTC'), 'UTC')"}
            ]

        filter_spec = None

    # Estructura general
    druid_task_json = {
        "type": "index_parallel",        
        "spec": {
            "dataSchema": {
                "dataSource": dataSource_d,
                "timestampSpec": {
                    "column": "__time",
                    "format": "millis",
                    "missingValue": None
                },
                "dimensionsSpec": {
                    "dimensions": common_dimensions,
                    "dimensionExclusions": dimension_exclusions,
                    "includeAllDimensions": False,
                    "useSchemaDiscovery": False
                },
                "metricsSpec": metrics_spec,
                "granularitySpec": {
                    "type": "uniform",
                    "segmentGranularity": segmentGranularity,
                    "queryGranularity": queryGranularity,
                    "rollup": True,
                    "intervals": []
                },
                "transformSpec": {
                    "transforms": transform_spec,
                    "filter": filter_spec
                }                
            },
            "ioConfig": {
                "type": "index_parallel",
                "inputSource": inputSource,
                "appendToExisting": False,
                "dropExisting": False
            },
            "tuningConfig": {
                "type": "index_parallel",
                "maxRowsInMemory": 1000000,
                "maxBytesInMemory": 691972778,
                "forceGuaranteedRollup": True,
                "maxNumConcurrentSubTasks": 17,
                "totalNumMergeTasks": 11,
                "splitHintSpec": {
                    "type": "segments",
                    "maxInputSegmentBytesPerTask": maxInputSegmentBytesPerTask,
                    "maxNumSegments": maxNumSegments
                },
                "segmentWriteOutMediumFactory": {
                    "type": "offHeapMemory" 
                },
                #"skipBytesInMemoryOverheadCheck": False,
                #"maxTotalRows": maxTotalRows,
                #"numShards": None,
                "partitionsSpec": {
                    "type": "range",
                    "partitionDimensions": [
                        "managedObjectId",
                        "additionalDn",
                        "dn"
                    ],
                    "targetRowsPerSegment": 4000000,
                    "assumeGrouped": False
                },
                "indexSpec": {
                    "bitmap": {"type": "roaring"},                    
                    "stringDictionaryEncoding": {
                        "type": "frontCoded",
                        "bucketSize": 8,
                        "formatVersion": 1
                    },
                    "longEncoding": "longs",
                    "dimensionCompression": "lz4",
                    "metricCompression": "lz4",
                    "complexMetricCompression": "lz4" 
                },
                "indexSpecForIntermediatePersists": {
                    "bitmap": {"type": "roaring"},                    
                    "stringDictionaryEncoding": {"type": "utf8"},
                    "longEncoding": "longs",                   
                    "dimensionCompression": "uncompressed",
                    "metricCompression": "none",
                    "complexMetricCompression": "uncompressed"
                },
                "maxPendingPersists": 0,
                "reportParseExceptions": False,
                "pushTimeout": 0,
                "maxRetry": 3,
                "taskStatusCheckPeriodMs": 1000,
                "chatHandlerTimeout": "PT10S",
                "chatHandlerNumRetries": 5,
                "maxNumSegmentsToMerge": 100,
                "logParseExceptions": False,
                "maxParseExceptions": 2147483647,
                "maxSavedParseExceptions": 0,
                "maxColumnsToMerge": -1,
                "awaitSegmentAvailabilityTimeoutMillis": 0,
                "maxAllowedLockCount": -1,
                "partitionDimensions": []
            }
        },
        "context": {
            "forceTimeChunkLock": True,
            "useLineageBasedSegmentAllocation": True,
            "lookupLoadingMode": "NONE"
        },
        "dataSource": dataSource_d
    }
    return druid_task_json


def get_time_values (date, source, process, periodicity, previous):
    intervals = {
        '15M': timedelta(minutes=15),
        '30M': timedelta(minutes=30),
        'HOURLY': timedelta(hours=1),
        'DAILY': timedelta(days=1)
    }
    
    print("Date:", date)
    if process in ["interval", "reprocess"]:
        start_date = date
        e_date = date + intervals[periodicity]
    elif process == "load":
        current_date = date - timedelta(days=previous)
        if source == "snmp":
            current_date -= timedelta(hours=3)
            print("Date (delta 3H):", current_date)

        if periodicity == '15M':
            minute_block = (current_date.minute // 15) * 15
            e_date = current_date.replace(minute=minute_block, second=0, microsecond=0)
            start_date = e_date - intervals['15M']
        elif periodicity == '30M':
            minute_block = (current_date.minute // 30) * 30
            e_date = current_date.replace(minute=minute_block, second=0, microsecond=0)
            start_date = e_date - intervals['30M']
        elif periodicity == 'HOURLY':
            e_date = current_date.replace(minute=0, second=0, microsecond=0)
            start_date = e_date - intervals['HOURLY']
        elif periodicity == 'DAILY':
            e_date = current_date.replace(hour=0, minute=0, second=0, microsecond=0)
            start_date = e_date - intervals['DAILY']

    if periodicity == 'DAILY':
        start_date += timedelta(hours=3)
        e_date += timedelta(hours=3)

    startdate = start_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4] + "Z"
    enddate = e_date.strftime("%Y-%m-%dT%H:%M:%S.%fZ")[:-4] + "Z"
    interval = startdate + "/" + enddate  
    return interval


def druid_temporal_summarization(date, periodicity, source, process, previous):
    interval = get_time_values (date, source, process, periodicity, previous)
    print ("interval:", interval)
    
    if periodicity =='15M':        
        dataSource_d = "snmp-enriched-metrics-temporal-15m"
        dataSource_o = "snmp-enriched-metrics"
        reportInterval = 900
        granularityInterval =  "'PT900S'"  
        queryGranularity = "FIFTEEN_MINUTE"
        segmentGranularity = "FIFTEEN_MINUTE"
    elif periodicity == '30M':
        reportInterval = 1800
        granularityInterval = "'PT1800S'"  
        dataSource_d = "fastoss-pm-enriched-metrics-temporal-30m"
        dataSource_o = "fastoss-pm-enriched-metrics"
        queryGranularity = "THIRTY_MINUTE"   
        segmentGranularity = "THIRTY_MINUTE" 
    elif periodicity == 'HOURLY':
        reportInterval = 3600
        granularityInterval = "'PT3600S'"
        queryGranularity = "HOUR"  
        segmentGranularity = "HOUR"   
        if source == "snmp": 
            dataSource_d = "snmp-enriched-metrics-temporal-hourly"
            dataSource_o = "snmp-enriched-metrics-temporal-15m"
        else:
            dataSource_d = "fastoss-pm-enriched-metrics-temporal-hourly"
            dataSource_o = "fastoss-pm-enriched-metrics-temporal-30m"          
    elif periodicity=='DAILY':
        reportInterval = 86400
        granularityInterval = "'PT86400S'"
        queryGranularity = "DAY" 
        segmentGranularity = "DAY"   
        if source == "snmp": 
            dataSource_d = "snmp-enriched-metrics-temporal-daily-3"
            dataSource_o = "snmp-enriched-metrics-temporal-hourly"
        else:
            dataSource_d = "fastoss-pm-enriched-metrics-temporal-daily-3"
            dataSource_o = "fastoss-pm-enriched-metrics-temporal-hourly"
    
    druid_task_json = create_json(periodicity, dataSource_d, queryGranularity, segmentGranularity, reportInterval, granularityInterval, dataSource_o, interval)
    data=json.dumps(druid_task_json)
    return data


def date_creator(start_date, end_date, intervalo):
    inicio = datetime.strptime(start_date, "%Y-%m-%d %H:%M:%S")
    fin = datetime.strptime(end_date, "%Y-%m-%d %H:%M:%S")
    fechas = []
    actual = inicio
    while actual < fin:
        fechas.append(actual)
        actual += intervalo    
    
    return fechas


def druid_spatial_summarization(date, periodicity, source, process, previous):
    interval = get_time_values (date, source, process, periodicity, previous)
    print ("interval:", interval)        
   
    if periodicity =='15M':        
        dataSource_d = "snmp-enriched-metrics-group-15m"
        dataSource_o = "snmp-enriched-metrics"
        reportInterval = 900
        granularityInterval =  "'PT900S'"  
        queryGranularity = "FIFTEEN_MINUTE"
        segmentGranularity = "FIFTEEN_MINUTE"
    elif periodicity == '30M':
        reportInterval = 1800
        granularityInterval = "'PT1800S'"  
        dataSource_d = "fastoss-pm-enriched-metrics-group-30m"
        dataSource_o = "fastoss-pm-enriched-metrics"
        queryGranularity = "THIRTY_MINUTE"   
        segmentGranularity = "THIRTY_MINUTE" 
    elif periodicity == 'HOURLY':
        reportInterval = 3600
        granularityInterval = "'PT3600S'"
        queryGranularity = "HOUR"  
        segmentGranularity = "HOUR"   
        if source == "snmp": 
            dataSource_d = "snmp-enriched-metrics-group-hourly"
            dataSource_o = "snmp-enriched-metrics-group-15m"
        else:
            dataSource_d = "fastoss-pm-enriched-metrics-group-hourly"
            dataSource_o = "fastoss-pm-enriched-metrics-group-30m"          
    elif periodicity=='DAILY':
        reportInterval = 86400
        granularityInterval = "'PT86400S'"
        queryGranularity = "DAY" 
        segmentGranularity = "DAY"   
        if source == "snmp": 
            dataSource_d = "snmp-enriched-metrics-group-daily-3"
            dataSource_o = "snmp-enriched-metrics-group-hourly"
        else:
            dataSource_d = "fastoss-pm-enriched-metrics-group-daily-3"
            dataSource_o = "fastoss-pm-enriched-metrics-group-hourly"
    
    druid_task_json = create_spatial_json(periodicity, dataSource_d, queryGranularity, segmentGranularity, reportInterval, granularityInterval, dataSource_o, interval)
    data=json.dumps(druid_task_json)
    return data


def create_spatial_json(periodicity, dataSource_d, queryGranularity, segmentGranularity, reportInterval, granularityInterval, dataSource_o, interval):
    #intervalo = "2025-03-16T00:00:00.000Z/2025-03-16T00:15:00.000Z"
    fechas = interval.split('/')
    fecha_inicio_iso = fechas[0]
    fecha_fin_iso = fechas[1]

    fecha_inicio = datetime.fromisoformat(fecha_inicio_iso.replace('Z', '+00:00'))
    fecha_fin = datetime.fromisoformat(fecha_fin_iso.replace('Z', '+00:00'))

    # Formatear las fechas como cadenas
    fecha_inicio_str = fecha_inicio.strftime('%Y-%m-%d %H:%M:%S')
    fecha_fin_str = fecha_fin.strftime('%Y-%m-%d %H:%M:%S')

    if periodicity =='15M': 
        query_reportInterval = "300"
        time_floor = "PT15M"
    elif periodicity =='30M': 
        query_reportInterval = "900"
        time_floor = "PT30M"
        
    if periodicity in ["15M", "30M"]:  
        sqlQuery = f"""
            REPLACE INTO "{dataSource_d}"
            OVERWRITE WHERE "__time" >= TIMESTAMP'{fecha_inicio_str}' AND "__time" < TIMESTAMP'{fecha_fin_str}'
            WITH datos_agg AS (
                SELECT  "__time", reportInterval, formulaId, metricId, isRate, granularityInterval, "networkGroup",
                    COUNT(*) as "count",
                    SUM(value_calculado) AS sum_value, MAX(value_calculado) AS max_value, MIN(value_calculado) as min_value, AVG(value_calculado) as avg_value,
                    SUM("numerator") AS sum_numerator, MAX("numerator") AS max_numerator, MIN("numerator") as min_numerator,
                    SUM("denominator") AS sum_denominator, MAX("denominator") AS max_denominator, MIN("denominator") as min_denominator,
                    COUNT (DISTINCT "dn") as count_dn, COUNT (DISTINCT "additionalDn") as count_additionalDn
                FROM (
                    SELECT "__time", reportInterval, formulaId, metricId, isRate, granularityInterval, un.g AS "networkGroup",
                        numerator, denominator, "dn", "additionalDn",
                        CASE
                            WHEN isRate = 'true' AND denominator != 0 THEN numerator / denominator
                            ELSE "value"
                        END AS value_calculado
                    FROM "{dataSource_o}" CROSS JOIN UNNEST(MV_TO_ARRAY(networkGroups)) AS un(g)
                    WHERE "__time" >= TIMESTAMP'{fecha_inicio_str}' AND "__time" < TIMESTAMP'{fecha_fin_str}'
                        AND "reportInterval" = '{query_reportInterval}'
                        AND "networkGroups" IS NOT NULL
                    )
                WHERE value_calculado IS NOT NULL
                GROUP BY 1, 2, 3, 4, 5, 6, 7
            )

            SELECT 
                TIME_FLOOR("__time", '{time_floor}') as "__time",
                '{reportInterval}' as reportInterval,
                formulaId, 
                metricId, 
                isRate, 
                {granularityInterval} as granularityInterval,
                "networkGroup",
                SUM("count") as "count",
                SUM(sum_value) AS sum_value,
                MAX(max_value) AS max_value,
                MIN(min_value) AS min_value,
                SUM(sum_value) / SUM("count") AS avg_value,
                SUM(sum_numerator) AS sum_numerator,
                MAX(max_numerator) AS max_numerator,
                MIN(min_numerator) AS min_numerator,
                SUM(sum_denominator) AS sum_denominator,
                MAX(max_denominator) AS max_denominator,
                MIN(min_denominator) AS min_denominator,                
                MAX(count_dn) AS max_count_dn,
                MIN(count_dn) AS min_count_dn,
                MAX(count_additionalDn) AS max_count_additionalDn,
                MIN(count_additionalDn) AS min_count_additionalDn,
                DR_PERCENTILE_AGG("sum_value", 1024) as reservoir_sum_value,
                DR_PERCENTILE_AGG("max_value", 1024) as reservoir_max_value,
                DR_PERCENTILE_AGG("min_value", 1024) as reservoir_min_value,
                DR_PERCENTILE_AGG("avg_value", 1024) as reservoir_avg_value
            FROM datos_agg
            GROUP BY 1, 2, 3, 4, 5, 6, 7
            PARTITIONED BY {segmentGranularity}
            CLUSTERED BY networkGroup, metricId   
        """
        print(sqlQuery)
                
        druid_task_json = {
            "query": sqlQuery,
            "resultFormat": "array",
            "header": True,
            "typesHeader": True,
            "sqlTypesHeader": True,
            "context": {
                "useApproximateCountDistinct": False,
                "durableShuffleStorage": True,
                "selectDestination": "durableStorage",
                "maxNumTasks": 11,
                "removeNullBytes": True,
                "executionMode": "async"
            }
        }
    else:
        common_dimensions = [
            {"type": "string", "name": "reportInterval", "multiValueHandling": "SORTED_ARRAY", "createBitmapIndex": True},
            {"type": "string", "name": "networkGroup", "multiValueHandling": "SORTED_ARRAY", "createBitmapIndex": True},
            {"type": "string", "name": "formulaId", "multiValueHandling": "SORTED_ARRAY", "createBitmapIndex": True},
            {"type": "string", "name": "metricId", "multiValueHandling": "SORTED_ARRAY", "createBitmapIndex": True},
            {"type": "string", "name": "isRate", "multiValueHandling": "SORTED_ARRAY", "createBitmapIndex": True},
            {"type": "string", "name": "granularityInterval", "multiValueHandling": "SORTED_ARRAY", "createBitmapIndex": True},
        ]
  
        dimension_exclusions = [
            "__time", 
            "count", 
            "sum_value", 
            "max_value", 
            "min_value",
            "avg_value",
            "sum_numerator", 
            "max_numerator",
            "min_numerator",
            "sum_denominator",
            "max_denominator",
            "min_denominator",  
            "max_count_dn",
            "min_count_dn",
            "max_count_additionalDn",
            "min_count_additionalDn",            
            "reservoir_sum_value",
            "reservoir_max_value",
            "reservoir_min_value",
            "reservoir_avg_value"
        ]

        metrics_spec = [
            {"type": "doubleSum", "name": "count", "fieldName": "count" },
            {"type": "doubleSum", "name": "sum_value", "fieldName": "sum_value"},
            {"type": "doubleMax", "name": "max_value", "fieldName": "max_value"},
            {"type": "doubleMin", "name": "min_value", "fieldName": "min_value"},
            #{"type": "doubleSum", "name": "avg_value", "fieldName": "avg_value"},
            {"type": "doubleSum", "name": "sum_numerator", "fieldName": "sum_numerator"},
            {"type": "doubleMax", "name": "max_numerator", "fieldName": "max_numerator"},
            {"type": "doubleMin", "name": "min_numerator", "fieldName": "min_numerator"},
            {"type": "doubleSum", "name": "sum_denominator", "fieldName": "sum_denominator"},
            {"type": "doubleMax", "name": "max_denominator", "fieldName": "max_denominator"},
            {"type": "doubleMin", "name": "min_denominator", "fieldName": "min_denominator"}, 
            {"type": "doubleMax", "name": "max_count_dn", "fieldName": "max_count_dn"},
            {"type": "doubleMin", "name": "min_count_dn", "fieldName": "min_count_dn"}, 
            {"type": "doubleMax", "name": "max_count_additionalDn", "fieldName": "max_count_additionalDn"},
            {"type": "doubleMin", "name": "min_count_additionalDn", "fieldName": "min_count_additionalDn"}, 
            {"type": "doublesReservoir", "name": "reservoir_sum_value", "fieldName": "reservoir_sum_value", "maxReservoirSize": 1024},
            {"type": "doublesReservoir", "name": "reservoir_max_value", "fieldName": "reservoir_max_value", "maxReservoirSize": 1024},
            {"type": "doublesReservoir", "name": "reservoir_min_value", "fieldName": "reservoir_min_value", "maxReservoirSize": 1024},
            {"type": "doublesReservoir", "name": "reservoir_avg_value", "fieldName": "reservoir_avg_value", "maxReservoirSize": 1024}
        ]

        inputSource = {
                    "type": "druid",
                    "dataSource": dataSource_o,
                    "interval": interval
                }

        maxInputSegmentBytesPerTask =  "250Mi"
        maxNumSegments = 10

        if periodicity == 'HOURLY':
            transform_spec = [
                {"type": "expression", "name": "reportInterval", "expression": reportInterval},
                {"type": "expression", "name": "granularityInterval", "expression": granularityInterval}
            ]
        elif periodicity=='DAILY': 
            transform_spec = [
                {"type": "expression", "name": "reportInterval", "expression": reportInterval},
                {"type": "expression", "name": "granularityInterval", "expression": granularityInterval},
                {"type": "expression", "name": "__time", "expression": "timestamp_floor(__time, 'P1D', timestamp_shift(__time, 'PT-3H', 1, 'UTC'), 'UTC')"}
            ]

        filter_spec = None

        # Estructura general
        druid_task_json = {
            "type": "index_parallel",        
            "spec": {
                "dataSchema": {
                    "dataSource": dataSource_d,
                    "timestampSpec": {
                        "column": "__time",
                        "format": "millis",
                        "missingValue": None
                    },
                    "dimensionsSpec": {
                        "dimensions": common_dimensions,
                        "dimensionExclusions": dimension_exclusions,
                        "includeAllDimensions": False,
                        "useSchemaDiscovery": False
                    },
                    "metricsSpec": metrics_spec,
                    "granularitySpec": {
                        "type": "uniform",
                        "segmentGranularity": segmentGranularity,
                        "queryGranularity": queryGranularity,
                        "rollup": True,
                        "intervals": []
                    },
                    "transformSpec": {
                        "transforms": transform_spec,
                        "filter": filter_spec
                    }                
                },
                "ioConfig": {
                    "type": "index_parallel",
                    "inputSource": inputSource,
                    "appendToExisting": False,
                    "dropExisting": False
                },
                "tuningConfig": {
                    "type": "index_parallel",
                    "maxRowsInMemory": 1000000,
                    "maxBytesInMemory": 691972778,
                    "forceGuaranteedRollup": True,
                    "maxNumConcurrentSubTasks": 17,
                    "totalNumMergeTasks": 17,
                    "splitHintSpec": {
                        "type": "segments",
                        "maxInputSegmentBytesPerTask": maxInputSegmentBytesPerTask,
                        "maxNumSegments": maxNumSegments
                    },
                    "segmentWriteOutMediumFactory": {
                        "type": "offHeapMemory" 
                    },
                    #"skipBytesInMemoryOverheadCheck": False,
                    #"maxTotalRows": maxTotalRows,
                    #"numShards": None,
                    "partitionsSpec": {
                        "type": "range",
                        "partitionDimensions": [
                            "managedObjectId",
                            "additionalDn",
                            "dn"
                        ],
                        "targetRowsPerSegment": 4000000,
                        "assumeGrouped": False
                    },
                    "indexSpec": {
                        "bitmap": {"type": "roaring"},                    
                        "stringDictionaryEncoding": {
                            "type": "frontCoded",
                            "bucketSize": 8,
                            "formatVersion": 1
                        },
                        "longEncoding": "longs",
                        "dimensionCompression": "lz4",
                        "metricCompression": "lz4",
                        "complexMetricCompression": "lz4" 
                    },
                    "indexSpecForIntermediatePersists": {
                        "bitmap": {"type": "roaring"},                    
                        "stringDictionaryEncoding": {"type": "utf8"},
                        "longEncoding": "longs",                   
                        "dimensionCompression": "uncompressed",
                        "metricCompression": "none",
                        "complexMetricCompression": "uncompressed"
                    },
                    "maxPendingPersists": 0,
                    "reportParseExceptions": False,
                    "pushTimeout": 0,
                    "maxRetry": 3,
                    "taskStatusCheckPeriodMs": 1000,
                    "chatHandlerTimeout": "PT10S",
                    "chatHandlerNumRetries": 5,
                    "maxNumSegmentsToMerge": 100,
                    "logParseExceptions": False,
                    "maxParseExceptions": 2147483647,
                    "maxSavedParseExceptions": 0,
                    "maxColumnsToMerge": -1,
                    "awaitSegmentAvailabilityTimeoutMillis": 0,
                    "maxAllowedLockCount": -1,
                    "partitionDimensions": []
                }
            },
            "context": {
                "forceTimeChunkLock": True,
                "useLineageBasedSegmentAllocation": True,
                "lookupLoadingMode": "NONE"
            },
            "dataSource": dataSource_d
        }

    return druid_task_json

def execute_query_druid(sql):
    results=pd.DataFrame()
    url_druid = Variable.get("url-druidDb")    
    jdbc_url = "jdbc:avatica:remote:url=" + url_druid + "/avatica/"
       
    jar_files = [        
        "/opt/airflow/dags/repo/dags/libs/avatica-core-1.25.0.jar",
        "/opt/airflow/dags/repo/dags/libs/slf4j-api-1.7.32.jar",
        "/opt/airflow/dags/repo/dags/libs/httpcore5-5.0.3.jar",
        "/opt/airflow/dags/repo/dags/libs/httpclient5-5.0.3.jar",
        "/opt/airflow/dags/repo/dags/libs/jackson-databind-2.12.3.jar",
        "/opt/airflow/dags/repo/dags/libs/jackson-core-2.12.3.jar",
        "/opt/airflow/dags/repo/dags/libs/jackson-annotations-2.12.3.jar",
        "/opt/airflow/dags/repo/dags/libs/protobuf-java-3.19.0.jar",
        "/opt/airflow/dags/repo/dags/libs/protobuf-java-util-3.19.0.jar"
    ]

    driver_class = "org.apache.calcite.avatica.remote.Driver"

    try:
        # Establecer la conexión
        if 'connection' not in locals() or connection is None or connection.closed:
            # Establecer la conexión si no existe o está cerrada
            connection = jaydebeapi.connect(driver_class, jdbc_url, [], jar_files)
            print("Conexión con druid establecida.")       

        cursor = connection.cursor()
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        results = pd.DataFrame(rows, columns=columns)
    except Exception as e:
        print(f"Se produjo un error: {e}")
        raise e
    finally:
        # Cerrar la conexión
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()
        
    #print("Resultados:", results)
    return results