# Nome da DAG: dag_alarms_aggregation_controller_dev
# Owner / responsável: CoE
# Descrição do objetivo da DAG: Busca las configuracion de los thresholds en Symphony Dev y las guarda en la base de datos Postgres de airflow
# Usa Druid?: No
# Principais tabelas / consultas Druid acessadas: 
# Frequência de execução (schedule): Demanda
# Dag Activo?: No
# Autor: CoE
# Data de modificação: 2025-05-26
from airflow import DAG
import pandas as pd
#import numpy as np
import json
import asyncio
import pytz
import functions as functions_2

from datetime import datetime, timedelta
from sqlalchemy import create_engine
from sqlalchemy import text
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from airflow.operators.dummy_operator import DummyOperator
from airflow.hooks.base_hook import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.operators.trigger_dagrun import TriggerDagRunOperator
from airflow.utils.task_group import TaskGroup

from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.exceptions import TransportServerError
from gql.transport.aiohttp import log as aiohttp_logger

connection = BaseHook.get_connection('postgres_prd')
engine = create_engine(f"postgresql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}")

default_args = {
    'owner': 'CoE',
    'start_date': datetime(2024, 4, 25),
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Función para ejecutar la mutación GraphQL
async def execute_graphql_query():
    mutation = '''
        query Thresholds {
            thresholds {
                edges {
                    node {
                        id
                        name
                        status
                        dataAggregationTime
                        dataAggregationTimeGranularity
                        dataAggregationFunction       
                        dataAggregationPercentileValue
                        timeZone                      
                        managedObject {
                            id
                            name
                            model {
                                tableName
                            }
                        }
                        expression {
                            id
                            terms {
                                kpi {
                                    id
                                    name
                                    defaultTimeGranularity
                                    isRate
                                    unit
                                    hourly
                                    daily
                                    weekly
                                    monthly
                                    yearly   
                                    fifteenDays
                                    quarterly
                                    semesterly
                                    hourlyPercentile
                                    dailyPercentile
                                    weeklyPercentile
                                    monthlyPercentile
                                    yearlyPercentile
                                    fifteenDaysPercentile      
                                    quarterlyPercentile
                                    semesterlyPercentile
                                }
                            }
                        }
                        spatialAggregation
                        spatialAggregationFunction
                        spatialAggregationPercentileValue
                        spatialLocationTypeAggregation {
                            id
                            name
                        }
                        spatialManagedObjectAggregation {
                            id
                            name
                        }
                        equipmentAssociation {
                            ngExclusion {
                                id
                                name
                            }
                            ngInclusion {
                                id
                                name
                                typeAssociation
                            }
                            equipmentExclusion {
                                id
                                dn
                            }
                            equipmentInclusion {
                                id
                                dn
                            }
                        }
                        algorithm
                        algorithmComparisonValue
                        algorithmComparisonType
                        intervalDays
                        times
                        effectiveDays
                        days {
                            optionsDays
                        }
                        effectiveHours {
                            optionsEffectiveHours
                        }
                    }
                }
            }
        }'''

    url_apollo = Variable.get("url-symphony")   
    url_base64 = Variable.get("base64_string")
    
    try:
      transport = AIOHTTPTransport(url=url_apollo, headers={'Authorization': f'Basic {url_base64}'}, timeout=600)
      async with Client(transport=transport, fetch_schema_from_transport=False, execute_timeout=600) as client:
            query = gql(mutation)            
            result = await client.execute(query)            
            return result            
    except Exception as e:
      print(repr(e))
      raise e
    

async def execute_graphql_query_supergroups(ids):
    mutation = '''
        query NG($filterBy: [NetworkGroupFilterInput!]) { 
            networkGroups(filterBy: $filterBy) { 
                totalCount 
                edges { 
                    node { 
                        id 
                        name 
                        targetGroups 
                    } 
                }
            }
        }
    ''' 
    variables = {
        "filterBy": [
            {
                "filterType": "PARENT_GROUP_ID",
                "operator": "IS_ONE_OF",
                "idSet": ids, 
                "boolValue": True
            },
            { 
                "filterType": "TARGET_GROUPS", 
                "operator": "IS", 
                "stringValue": "ALARMS" 
            }
        ]
    }
       
    url_apollo = Variable.get("url-symphony")   
    url_base64 = Variable.get("base64_string")
    
    try:
      transport = AIOHTTPTransport(url=url_apollo, headers={'Authorization': f'Basic {url_base64}'}, timeout=600)
      async with Client(transport=transport, fetch_schema_from_transport=False, execute_timeout=600) as client:
            query = gql(mutation)            
            result = await client.execute(query, variable_values=variables)             
            return result            
    except Exception as e:
      print(repr(e))
      raise e
    

def process_graphql_result(result):   
    thresholds_edges = result['thresholds']['edges']
    
    thresholds_edges = [
        edge for edge in thresholds_edges
        if edge['node']['dataAggregationTime'] != "NONE" or edge['node']['spatialAggregation'] != "NONE"
      ]

    rows = []
    for edge in thresholds_edges:
        node = edge['node']        
        node_status = node.get('status')
        if node_status is True:
            managedObject = node['managedObject']
            managedObject_name = None
            managedObject_id = None
            dataTableName = None
            timeZone=node['timeZone']
            if managedObject is not None:
                managedObject_id = managedObject.get('id')
                managedObject_name = managedObject.get('name')
                if managedObject['model'] is not None:
                    dataTableName = managedObject['model'].get('tableName')
            
            id_spatialLocationTypeAggregation = ""
            spatialLocationTypeAggregation = ""
            id_spatialManagedObjectAggregation = ""
            spatialManagedObjectAggregation = ""
            if node['spatialLocationTypeAggregation'] is not None:
                id_spatialLocationTypeAggregation = node['spatialLocationTypeAggregation'].get('id')
                spatialLocationTypeAggregation = node['spatialLocationTypeAggregation'].get('name')
            if node['spatialManagedObjectAggregation'] is not None:
                id_spatialManagedObjectAggregation = node['spatialManagedObjectAggregation'].get('id')
                spatialManagedObjectAggregation = node['spatialManagedObjectAggregation'].get('name')

            ngExclusion = []
            ngInclusion = []
            equipmentExclusion = []
            equipmentInclusion = []

            id_ngExclusion = []
            id_ngInclusion = []
            id_equipmentExclusion = []
            id_equipmentInclusion = []

            id_ngInclusion_SG = []
            typeAssociation = []
            if 'equipmentAssociation' in node:
                for association in node['equipmentAssociation']:
                    if 'ngExclusion' in association:
                        for item in association['ngExclusion']:
                            id_ngExclusion.append(item.get('id'))
                            ngExclusion.append(item.get('name'))

                    if 'ngInclusion' in association:
                        for item in association['ngInclusion']:
                            id_ngInclusion.append(item.get('id'))  
                            ngInclusion.append(item.get('name'))
                            typeAssociation.append(item.get('typeAssociation'))

                            if id_ngInclusion and all(assoc == 'SUPERGROUP' for assoc in typeAssociation):
                                resultSG = execute_graphql_query_supergroups_sync (id_ngInclusion)
                                id_ngInclusion_SG = [edge['node']['id'] for edge in resultSG['networkGroups']['edges']]

                    if 'equipmentExclusion' in association:
                        for item in association['equipmentExclusion']:
                            id_equipmentExclusion.append(item.get('id'))
                            equipmentExclusion.append(item.get('dn'))

                    if 'equipmentInclusion' in association:
                        for item in association['equipmentInclusion']:
                            id_equipmentInclusion.append(item.get('id')) 
                            equipmentInclusion.append(item.get('dn')) 

            days = node['days']
            optionsDays = None
            if days:
                optionsDays = [d.get('optionsDays') for d in days]
            
            effectiveHours = node['effectiveHours']
            if effectiveHours is not None:
                effective_hours = [hour['optionsEffectiveHours'] for hour in node['effectiveHours']]
                formatted_hours = [':'.join(hora.split('_')[1:]) if hora.startswith('_') else hora for hora in effective_hours]
                formatted_numbers = [hora.split(':')[0] for hora in formatted_hours]
                
            kpi_terms = node['expression']['terms']
            for term in kpi_terms:
                kpi_data = term.get('kpi')        
                row_data = {
                    'node_id': node.get('id'),
                    'node_name': node.get('name'),
                    'node_status':node_status,
                    'dataAggregationTime': node.get('dataAggregationTime'),
                    'dataAggregationTimeGranularity': node.get('dataAggregationTimeGranularity'),
                    'dataAggregationFunction': node.get('dataAggregationFunction'),
                    'dataAggregationPercentileValue': node.get('dataAggregationPercentileValue'),
                    'spatialAggregation': node.get('spatialAggregation'),                    
                    'spatialAggregationFunction': node.get('spatialAggregationFunction'),
                    'spatialAggregationPercentileValue': node.get('spatialAggregationPercentileValue'),
                    'spatialLocationTypeAggregation': spatialLocationTypeAggregation,
                    'id_spatialLocationTypeAggregation': id_spatialLocationTypeAggregation,
                    'spatialManagedObjectAggregation': spatialManagedObjectAggregation,
                    'id_spatialManagedObjectAggregation': id_spatialManagedObjectAggregation,
                    'managedObject_id': managedObject_id,
                    'managedObject_name': managedObject_name,
                    'dataTableName': dataTableName,
                    'expression_id': node['expression'].get('id'),
                    'ngExclusion': ngExclusion,
                    'ngInclusion': ngInclusion,
                    'equipmentExclusion': equipmentExclusion,
                    'equipmentInclusion': equipmentInclusion,
                    'id_ngExclusion': id_ngExclusion,
                    'id_ngInclusion': id_ngInclusion,
                    'id_ngInclusion_SG': id_ngInclusion_SG,
                    'id_equipmentExclusion': id_equipmentExclusion,
                    'id_equipmentInclusion': id_equipmentInclusion,
                    'typeAssociation': typeAssociation,
                    'algorithm': node.get('algorithm'),
                    'algorithmComparisonValue': node.get('algorithmComparisonValue'),
                    'algorithmComparisonType': node.get('algorithmComparisonType'),
                    'intervalDays': node.get('intervalDays'),
                    'times': node.get('times'),
                    'effectiveDays': node.get('effectiveDays'),
                    'optionsDays': optionsDays,
                    'effectiveHours': formatted_numbers,
                    'timeZone': timeZone
                }

                if kpi_data:
                    row_data.update({
                        'kpi_id': kpi_data.get('id'),
                        'kpi_name': kpi_data.get('name'),
                        'kpi_defaultTimeGranularity': kpi_data.get('defaultTimeGranularity'),
                        'kpi_isRate': kpi_data.get('isRate'),
                        'kpi_unit': kpi_data.get('unit'),
                        'kpi_hourly': kpi_data.get('hourly'),
                        'kpi_daily': kpi_data.get('daily'),
                        'kpi_weekly': kpi_data.get('weekly'),
                        'kpi_monthly': kpi_data.get('monthly'),
                        'kpi_yearly': kpi_data.get('yearly'),
                        'kpi_fifteendays': kpi_data.get('fifteenDays'),
                        'kpi_quarterly': kpi_data.get('quarterly'),
                        'kpi_semesterly': kpi_data.get('semesterly'),
                        'hourlyPercentile': kpi_data.get('hourlyPercentile'),
                        'dailyPercentile': kpi_data.get('dailyPercentile'),
                        'weeklyPercentile': kpi_data.get('weeklyPercentile'),
                        'monthlyPercentile': kpi_data.get('monthlyPercentile'),
                        'yearlyPercentile': kpi_data.get('yearlyPercentile'),
                        'fifteenDaysPercentile': kpi_data.get('fifteenDaysPercentile'),      
                        'quarterlyPercentile': kpi_data.get('quarterlyPercentile'),
                        'semesterlyPercentile': kpi_data.get('semesterlyPercentile')
                    })            

                rows.append(row_data)

    df = pd.DataFrame(rows) 

    columns_to_process = [
        'ngExclusion', 'ngInclusion', 'equipmentExclusion', 'equipmentInclusion',
        'id_ngExclusion', 'id_ngInclusion', 'id_ngInclusion_SG',
        'id_equipmentExclusion', 'id_equipmentInclusion', 'typeAssociation',
        'optionsDays', 'effectiveHours'
    ]

    for column in columns_to_process:
        df[column] = df[column].apply(lambda x: json.dumps([str(item) for item in x]))
     
    #df = df[df['node_status'] == True]
    df_filtered = df[df['kpi_id'].notnull() & df['kpi_id'].notna()].copy()
    df_filtered['dataAggregationTime'] = df_filtered['dataAggregationTime'].str.upper()
    
    execution_date_utc = datetime.utcnow()
    tz = pytz.timezone('America/Argentina/Buenos_Aires')  # Zona horaria UTC-3
    execution_date_utc3 = execution_date_utc.astimezone(tz)
    formatted_execution_date_utc3 = execution_date_utc3.strftime("%Y-%m-%d %H:%M:%S")  # Formato personalizado
    df_filtered['execution_date'] = formatted_execution_date_utc3

    json_str = df_filtered.to_json(orient='records')
    return json_str


def execute_graphql_query_sync(*args, **kwargs):
    loop = asyncio.get_event_loop()
    if loop.is_closed():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    result = loop.run_until_complete(execute_graphql_query())
    df = process_graphql_result(result)
    loop.close()
    return df


def execute_graphql_query_supergroups_sync(ids, **kwargs):
    loop = asyncio.get_event_loop()
    if loop.is_closed():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    result = loop.run_until_complete(execute_graphql_query_supergroups(ids))
    loop.close()
    return result


def insert_dataframe_to_postgres(**context):
    json_str= context['ti'].xcom_pull(task_ids='execute_graphql_query_task')
    df = pd.read_json(json_str)    
    df = df.fillna('null')

    delete_query = "DELETE FROM panda.threshold;"
    functions_2.execute_Query(delete_query,"","DELETE", False)
    threshold_kpi_list=[]

    for index, row in df.iterrows():    
        if (str(row['node_id']) + "-" + row['kpi_name']) not in threshold_kpi_list:
            if row['spatialAggregation'] == 'null':
                row['spatialAggregation'] = 'NONE'

            if row['dataAggregationTime'] == 'null':
                row['dataAggregationTime'] = 'NONE'

            if row['dataAggregationPercentileValue'] == 'null':
                row['dataAggregationPercentileValue'] = 0
            
            if row['spatialAggregationPercentileValue'] == 'null':
                row['spatialAggregationPercentileValue'] = 0

            if row['dataAggregationTime'] == 'NONE' and row['spatialAggregation'] == 'NONE':
                print("Threshold no guardado, ambas funciones son NULAS, name: ", row['node_name'])
            else:
                data = text("""
                     INSERT INTO panda.threshold (
                        node_id, 
                        node_name, 
                        "dataAggregationTime", 
                        "dataAggregationTimeGranularity", 
                        "dataAggregationFunction", 
                        "dataAggregationPercentileValue", 
                        "spatialAggregation", 
                        "spatialAggregationFunction",
                        "spatialAggregationPercentileValue", 
                        "id_spatialLocationTypeAggregation",
                        "spatialLocationTypeAggregation", 
                        "id_spatialManagedObjectAggregation",
                        "spatialManagedObjectAggregation", 
                        "managedObject_id",
                        "managedObject_name", 
                        "dataTableName", 
                        expression_id, 
                        "ngExclusion", 
                        "id_ngExclusion", 
                        "ngInclusion", 
                        "id_ngInclusion", 
                        "id_ngInclusion_SG",
                        "typeAssociation", 
                        "equipmentExclusion",
                        "id_equipmentExclusion", 
                        "equipmentInclusion", 
                        "id_equipmentInclusion", 
                        kpi_id, 
                        kpi_name, 
                        "kpi_defaultTimeGranularity", 
                        "kpi_isRate",
                        "kpi_unit", 
                        "kpi_hourly",
                        "kpi_daily",
                        "kpi_weekly",
                        "kpi_monthly",
                        "kpi_yearly", 
                        "kpi_fifteendays",
                        "kpi_quarterly",
                        "kpi_semesterly",
                        hourlyPercentile,
                        dailyPercentile,
                        weeklyPercentile,
                        monthlyPercentile,
                        yearlyPercentile,
                        fifteenDaysPercentile,
                        quarterlyPercentile,
                        semesterlyPercentile,
                        algorithm,
                        algorithmComparisonValue,
                        algorithmComparisonType,
                        intervalDays,
                        times,
                        effectiveDays,
                        optionsDays,
                        effectiveHours,
                        execution_date,
                        "timeZone"
                        )
                    VALUES (
                        :node_id, 
                        :node_name,
                        :dataAggregationTime, 
                        :dataAggregationTimeGranularity,
                        :dataAggregationFunction,
                        :dataAggregationPercentileValue, 
                        :spatialAggregation,
                        :spatialAggregationFunction, 
                        :spatialAggregationPercentileValue, 
                        :id_spatialLocationTypeAggregation,
                        :spatialLocationTypeAggregation, 
                        :id_spatialManagedObjectAggregation, 
                        :spatialManagedObjectAggregation,
                        :managedObject_id,
                        :managedObject_name, 
                        :dataTableName, 
                        :expression_id, 
                        :ngExclusion, 
                        :id_ngExclusion,
                        :ngInclusion, 
                        :id_ngInclusion, 
                        :id_ngInclusion_SG, 
                        :typeAssociation,
                        :equipmentExclusion,
                        :id_equipmentExclusion,
                        :equipmentInclusion,
                        :id_equipmentInclusion, 
                        :kpi_id, 
                        :kpi_name, 
                        :kpi_defaultTimeGranularity, 
                        :kpi_isRate,
                        :kpi_unit,
                        :kpi_hourly,
                        :kpi_daily,
                        :kpi_weekly,
                        :kpi_monthly,
                        :kpi_yearly,
                        :kpi_fifteendays,
                        :kpi_quarterly,
                        :kpi_semesterly,
                        :hourlyPercentile,
                        :dailyPercentile,
                        :weeklyPercentile,
                        :monthlyPercentile,
                        :yearlyPercentile,
                        :fifteenDaysPercentile,
                        :quarterlyPercentile,
                        :semesterlyPercentile,
                        :algorithm,
                        :algorithmComparisonValue,
                        :algorithmComparisonType,
                        :intervalDays,
                        :times,
                        :effectiveDays,
                        :optionsDays,
                        :effectiveHours,
                        :execution_date,
                        :timeZone
                    );
                """)                            

                my_parameters = {
                    'node_id': row['node_id'],
                    'node_name': row['node_name'],
                    'dataAggregationTime': row['dataAggregationTime'],
                    'dataAggregationTimeGranularity': row['dataAggregationTimeGranularity'],
                    'dataAggregationFunction': row['dataAggregationFunction'],
                    'dataAggregationPercentileValue': row['dataAggregationPercentileValue'],
                    'spatialAggregation': row['spatialAggregation'],
                    'spatialAggregationFunction': row['spatialAggregationFunction'],
                    'spatialAggregationPercentileValue': row['spatialAggregationPercentileValue'],
                    'id_spatialLocationTypeAggregation': row['id_spatialLocationTypeAggregation'],
                    'spatialLocationTypeAggregation': row['spatialLocationTypeAggregation'],
                    'id_spatialManagedObjectAggregation': row['id_spatialManagedObjectAggregation'],
                    'spatialManagedObjectAggregation': row['spatialManagedObjectAggregation'],
                    'managedObject_id': row['managedObject_id'],
                    'managedObject_name': row['managedObject_name'],
                    'dataTableName': row['dataTableName'],
                    'expression_id': row['expression_id'],
                    'ngExclusion': row['ngExclusion'],
                    'id_ngExclusion': row['id_ngExclusion'],
                    'ngInclusion': row['ngInclusion'],
                    'id_ngInclusion': row['id_ngInclusion'],
                    'id_ngInclusion_SG': row['id_ngInclusion_SG'],
                    'typeAssociation': row['typeAssociation'],
                    'equipmentExclusion': row['equipmentExclusion'],
                    'id_equipmentExclusion': row['id_equipmentExclusion'],
                    'equipmentInclusion': row['equipmentInclusion'],
                    'id_equipmentInclusion': row['id_equipmentInclusion'],
                    'kpi_id': row['kpi_id'],
                    'kpi_name': row['kpi_name'],
                    'kpi_defaultTimeGranularity': row['kpi_defaultTimeGranularity'],
                    'kpi_isRate': row['kpi_isRate'],
                    'kpi_unit': row['kpi_unit'],
                    'kpi_hourly': row['kpi_hourly'],
                    'kpi_daily': row['kpi_daily'],
                    'kpi_weekly': row['kpi_weekly'],
                    'kpi_monthly': row['kpi_monthly'],
                    'kpi_yearly': row['kpi_yearly'],
                    'kpi_fifteendays': row['kpi_fifteendays'],
                    'kpi_quarterly': row['kpi_quarterly'],
                    'kpi_semesterly': row['kpi_semesterly'],
                    'hourlyPercentile': row['hourlyPercentile'],
                    'dailyPercentile': row['dailyPercentile'],
                    'weeklyPercentile': row['weeklyPercentile'],
                    'monthlyPercentile': row['monthlyPercentile'],
                    'yearlyPercentile': row['yearlyPercentile'],
                    'fifteenDaysPercentile': row['fifteenDaysPercentile'],      
                    'quarterlyPercentile': row['quarterlyPercentile'],
                    'semesterlyPercentile': row['semesterlyPercentile'],
                    'algorithm': row['algorithm'],
                    'algorithmComparisonValue': row['algorithmComparisonValue'],
                    'algorithmComparisonType': row['algorithmComparisonType'],
                    'intervalDays': row['intervalDays'],
                    'times': row['times'],
                    'effectiveDays': row['effectiveDays'],
                    'optionsDays': row['optionsDays'],
                    'effectiveHours': row['effectiveHours'],                    
                    'execution_date': row['execution_date'],
                    'timeZone': row['timeZone']
                }

                #functions_2.execute_Query(data, my_parameters, "INSERT", False)
                with engine.connect() as conn:
                    conn.execute(data, my_parameters) 
                
                threshold_kpi_list.append(str(row['node_id'])+"-"+row['kpi_name'])


#Definición del Dag
with DAG(
    dag_id = "dag_alarms_aggregation_controller",
    default_args = default_args,
    start_date = datetime(2024, 4, 1),
    catchup = False,
    schedule_interval='*/30 * * * *',  # Ejecutar cada 30 minuto
    tags=["alarms", "aggregation"],
) as dag:
    
    # Definición de tareas
    start_task = DummyOperator(
        task_id ='start',
    )

    execute_graphql_query_task = PythonOperator(
        task_id = 'execute_graphql_query_task',
        python_callable = execute_graphql_query_sync,
        provide_context = True,
    )

    insert_dataframe_task = PythonOperator(
        task_id = 'insert_dataframe_to_postgres_task',
        python_callable = insert_dataframe_to_postgres,
        provide_context = True
    )
    
    end_task = DummyOperator(
        task_id = 'end',
    )
    
    # Definir las dependencias de las tareas
    start_task >> execute_graphql_query_task >> insert_dataframe_task >> end_task
