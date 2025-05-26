# Nome da DAG: functions_summarization_dev
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


def druid_create_task_summarization(date, periodicity, agregationType, source, process):
    intervalo = 1
    if agregationType=="temporal":
        if source != "snmp":
            intervalo = 3

        for i in range(intervalo): 
            data = druid_temporal_summarization(date, periodicity, source, process, i)
            print("json: ", data) 

            url_druid = Variable.get("url_druid_task_dev")  
            #url_druid = Variable.get("url_druid_task")         
            headers = {"Content-Type": "application/json"}     
            # response = requests.post(url_druid, headers=headers, data=data)
            # if response.status_code != 200:
            #     raise Exception(f"Error en la ingesta: {response.content}")
                        
            #print("respuesta: ", response.json())           
    else:
        data = druid_temporal_summarization(date,periodicity,source)      
        print("json: ", data) 


def create_json (periodicity, dataSource_d, queryGranularity, segmentGranularity, reportInterval, granularityInterval, dataSource_o, interval):
    # Mapeo para definir el filtro según la periodicidad
    filter_mapping = {"15M": 300, "30M": 900}
    #filter_mapping = {"15M": 300, "30M": 900, "HOURLY": 300}
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
    #if periodicity in ["15M", "30M", "HOURLY"]:
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
            "assumeGrouped": True, # False,
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
                "inputFormat": inputFormat,
                "appendToExisting": False,
                "dropExisting": False
            },
            "tuningConfig": {
                "type": "index_parallel",
                "maxRowsPerSegment": maxRowsPerSegment,
                "appendableIndexSpec": {
                    "type": "onheap",
                    "preserveExistingMetrics": False
                },
                "maxRowsInMemory": 1000000,
                "maxBytesInMemory": 0,
                "skipBytesInMemoryOverheadCheck": False,
                "maxTotalRows": maxTotalRows,
                "numShards": None,
                "splitHintSpec": None,
                "partitionsSpec": partitionsSpec,
                "indexSpec": {
                    "bitmap": {"type": "roaring"},
                    "dimensionCompression": "lz4",
                    "stringDictionaryEncoding": {"type": "utf8"},
                    "metricCompression": "lz4",
                    "longEncoding": "longs",
                    "complexMetricCompression": "lz4" #zstd
                },
                "indexSpecForIntermediatePersists": {
                    "bitmap": bitmap,
                    "dimensionCompression": "lz4",
                    "stringDictionaryEncoding": stringDictionaryEncoding,
                    "metricCompression": "lz4",
                    "longEncoding": "longs"
                },
                "maxPendingPersists": 0,
                "forceGuaranteedRollup": force_guaranteed_rollup,
                "reportParseExceptions": False,
                "pushTimeout": 0,
                "segmentWriteOutMediumFactory": None,
                "maxNumConcurrentSubTasks": 20,
                "maxRetry": 3,
                "taskStatusCheckPeriodMs": 1000,
                "chatHandlerTimeout": "PT10S",
                "chatHandlerNumRetries": 5,
                "maxNumSegmentsToMerge": 100,
                "totalNumMergeTasks": 10,
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
            "useLineageBasedSegmentAllocation": True
        },
        "dataSource": dataSource_d
    }
    return druid_task_json


def get_time_values(date, process, periodicity, previous):
    intervals = {
        '15M': timedelta(minutes=15),
        '30M': timedelta(minutes=30),
        'HOURLY': timedelta(hours=1),
        'DAILY': timedelta(days=1)
    }
    
    if process in ["interval", "reprocess"]:
        start_date = date
        e_date = date + intervals[periodicity]
    elif process == "load":
        current_date = date - timedelta(days=previous)
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
    interval = get_time_values (date, process, periodicity, previous)
    print ("interval:", interval)
    
    if periodicity =='15M':        
        dataSource_d = "summarization-snmp-15m"
        dataSource_o = "snmp-enriched-metrics"
        reportInterval = 900
        granularityInterval =  "'PT900S'"  
        queryGranularity = "FIFTEEN_MINUTE"
        segmentGranularity = "FIFTEEN_MINUTE"
    elif periodicity == '30M':
        reportInterval = 1800
        granularityInterval = "'PT1800S'"  
        dataSource_d = "core_agg_30m"
        dataSource_o = "fastoss-pm-enriched-metrics"
        queryGranularity = "THIRTY_MINUTE"   
        segmentGranularity = "HOUR" 
    elif periodicity == 'HOURLY':
        reportInterval = 3600
        granularityInterval = "'PT3600S'"
        queryGranularity = "HOUR"  
        segmentGranularity = "HOUR"   
        if source == "snmp": 
            dataSource_d = "summarization-snmp-hourly"
            dataSource_o = "summarization-snmp-15m"
        else:
            dataSource_d = "core_agg_hourly"
            dataSource_o = "core_agg_30m"          
    elif periodicity=='DAILY':
        reportInterval = 86400
        granularityInterval = "'PT86400S'"
        queryGranularity = "DAY" 
        segmentGranularity = "DAY"   
        if source == "snmp": 
            dataSource_d = "summarization-snmp-daily"
            dataSource_o = "summarization-snmp-hourly"
        else:
            dataSource_d = "core_agg_daily"
            dataSource_o = "core_agg_hourly"
    
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