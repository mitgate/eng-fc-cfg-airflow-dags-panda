from airflow.hooks.base_hook import BaseHook
from sqlalchemy import text
from airflow.models import Variable
from airflow.models import Connection
from datetime import datetime, timedelta
from airflow.models import Variable
from io import StringIO
from io import BytesIO
from confluent_kafka import Producer


import pandas as pd
import os
import json
import asyncio
import jaydebeapi 
import s3fs
import pyarrow.parquet as pq
import pyarrow as pa
import tempfile
import ssl

from gql import Client, gql
from gql.transport.aiohttp import AIOHTTPTransport
from gql.transport.exceptions import TransportServerError
from gql.transport.aiohttp import log as aiohttp_logger
from airflow.exceptions import AirflowException


async def execute_graphql_query():
    mutation = '''
        query Node {
            busyHours {
                edges {
                    node {
                        id
                        name
                        description
                        status
                        aggregationFunction
                        percentileValue  
                        date 
                        managedObject {
                            id
                            name
                            model {
                                tableName
                            }
                        }
                        kpi {
                            id
                            name
                            defaultTimeGranularity
                            status
                        }
                        timeSettings {          
                            id
                            period
                            firstBH
                            secondBH
                            thirdBH
                        }
                        effectiveDay
                        days {
                            day1
                            day2
                            day3
                        }
                        effectiveHour {
                            optionsEffectiveHours
                        }        
                    }             
                }
            }
        }'''

    url_apollo = Variable.get("url-symphony_dev")   
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

def process_graphql_result(result, periodicity):   
    edges = result['busyHours']['edges'] 
    rows = []
    for edge in edges:
        node = edge['node']     
        if node is not None:
            managedObject= node['managedObject']
            tableName=''
            if managedObject['model'] is not None:
                tableName = managedObject['model'].get('tableName')
            kpi = node['kpi'] 
            timeSettings = node['timeSettings']
            node_status = node.get('status')
            if node_status == 'ENABLE':
                effective_hours = [hour['optionsEffectiveHours'] for hour in node['effectiveHour']]
                formatted_hours = [':'.join(hora.split('_')[1:]) if hora.startswith('_') else hora for hora in effective_hours]

                days1 = []
                days2 = []
                days3 = []
                for item in node['days']:
                    days1.append(item.get('day1', None))  
                    days2.append(item.get('day2', None))
                    days3.append(item.get('day3', None))

                for setting in timeSettings:
                    period = setting["period"]
                    levels = []

                    if setting["firstBH"]:
                        levels.append(1)
                    if setting["secondBH"]:
                        levels.append(2)
                    if setting["thirdBH"]:
                        levels.append(3)

                    row = {
                        'node_id': node.get('id'),
                        'node_name': node.get('name'),
                        'node_description': node.get('description'),
                        'aggregationFunction':node.get('aggregationFunction'),
                        'percentileValue':node.get('percentileValue'),
                        'kpi_id': kpi.get('id'),
                        'kpi_name': kpi.get('name'),
                        'kpi_status': kpi.get('status'),
                        'defaultTimeGranularity': kpi.get('defaultTimeGranularity'),
                        'managedObject_id': managedObject.get('id'),
                        'managedObject_name': managedObject.get('name'),
                        'dataTableName': tableName,
                        'period': period,
                        'level': levels,
                        'effectiveDay': node['effectiveDay'],
                        'day1': days1, 
                        'day2': days2,  
                        'day3': days3,  
                        'effective_hours': formatted_hours,
                        'date': node['date']
                    }
            
                    rows.append(row)

    df = pd.DataFrame(rows)
    df['day1'] = df['day1'].apply(tuple)
    df['day2'] = df['day2'].apply(tuple)
    df['day3'] = df['day3'].apply(tuple)
    df['level'] = df['level'].apply(tuple)
    df['effective_hours'] = df['effective_hours'].apply(tuple)
    df['percentileValue'] = df['percentileValue'].fillna(0)

    for column in ['day1', 'day2', 'day3','effective_hours','level']:
         df[column] = df[column].apply(lambda x: json.dumps([str(item) for item in x]))

    df_filtrado = df[df['period'] == periodicity]
    return df_filtrado


def get_busyHour_configuration(periodicity):    
    loop = asyncio.get_event_loop()
    if loop.is_closed():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
    result = loop.run_until_complete(execute_graphql_query())
    df = process_graphql_result(result, periodicity)
    loop.close()
        
    df_json = df.to_json(orient='records')
    return df_json


def get_time_variables (var_datetime, periodicity):
    if periodicity=='DAILY_BUSY_HOUR':
        var_start = datetime(var_datetime.year, var_datetime.month, var_datetime.day) - timedelta(days=1)
        var_end = datetime(var_datetime.year, var_datetime.month, var_datetime.day, 23, 59, 59)  - timedelta(days=1)              
    elif periodicity=='WEEKLY_BUSY_HOUR':
        var_week = var_datetime - timedelta(days=var_datetime.weekday()+7)         
        var_start = datetime(var_week.year, var_week.month, var_week.day)
        var_end = var_start + timedelta(days=6, hours=23, minutes=59, seconds=59)                
    elif periodicity=='MONTHLY_BUSY_HOUR':
        month=(12 if var_datetime.month==1 else var_datetime.month-1)
        year=(var_datetime.year-1 if  var_datetime.month==1 else var_datetime.year)
        var_start = (datetime(year, month, 1))  
        var_end = (datetime(var_datetime.year, var_datetime.month, 1)) - timedelta(seconds=1) 

    num_week = var_start.isocalendar()[1]
    var_month = var_start.month
    formatted_init_datetime  = var_start.strftime("%Y-%m-%d %H:%M:%S")
    formatted_end_datetime  = var_end.strftime("%Y-%m-%d %H:%M:%S")      
    return formatted_init_datetime,formatted_end_datetime, num_week, var_month


def get_granularityKpi(defaultTimeGranularity):
    if defaultTimeGranularity=="PT5M":
        granularityInterval="PT300S"                                                  
    elif defaultTimeGranularity=="PT15M":
        granularityInterval="PT900S"                                                                             
    elif defaultTimeGranularity=="PT30M":
        granularityInterval="PT1800S"                    
    elif defaultTimeGranularity=='PT1H':
        granularityInterval="PT3600S"    
    return  granularityInterval

def execute_query_druid(sql):
    results=pd.DataFrame()
    url_druid = Variable.get("url-druidDb")    
    jdbc_url = "jdbc:avatica:remote:url="+url_druid+"/avatica-protobuf/;transparent_reconnection=true;serialization=protobuf"
    #jdbc_url = "jdbc:avatica:remote:url=https://druid.apps.ocp-01.tdigital-vivo.com.br/druid/v2/sql/avatica-protobuf/;transparent_reconnection=true;serialization=protobuf"

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

    # Configuración de las propiedades de conexión
    connection_properties = {
        "enableWindowing": "true",
        "maxSubqueryRows": "1000000000"
    }

    try:
        # Establecer la conexión        
        connection = jaydebeapi.connect(driver_class, jdbc_url, connection_properties, jar_files )       
        cursor = connection.cursor()        
        cursor.execute(sql)
        columns = [desc[0] for desc in cursor.description]
        rows = cursor.fetchall()
        results=pd.DataFrame(rows, columns=columns) 
    except Exception as e:
        print(f"Se produjo un error: {e}")
    finally:
        # Cerrar la conexión
        if 'cursor' in locals():
            cursor.close()
        if 'connection' in locals():
            connection.close()    
    return results


def create_query_groups(param_df, param_execution_date, periodicity):
    data = json.loads(param_df)
    df_config = pd.DataFrame(data)
    #df_filtrado = df_config.head(1)
    additionalBHOnDifferentDay = Variable.get("allowAdditionalBHOnDifferentDay").upper()
    # #additionalBHOnDifferentDay='FALSE'
         
    # Validación si los BH se calcula en días diferentes
    dense_num = 0
    if additionalBHOnDifferentDay=='TRUE' and periodicity!='DAILY_BUSY_HOUR':
         dense_num = 1

    var_periodicity = periodicity.split('_')[0].lower()
    var_execution_date = datetime.strftime(param_execution_date, "%Y-%m-%d %H:%M:%S")
    print('var_execution_date:', var_execution_date)

    rows = []
    for index, row in df_config.iterrows(): 
        var_validacion = True
        data_date_dow = None
        
        node_id = row['node_id']
        node_name = row['node_name']
        kpi_id = row['kpi_id']
        kpi_name = row['kpi_name']
        granularityInterval = get_granularityKpi(row['defaultTimeGranularity'])
        managedObject_name = row['managedObject_name']
        managedObject_id = row['managedObject_id']
        dataTableName = row['dataTableName']

        if dataTableName=='':
            var_validacion=False

        #OJO: Eliminar después que pase a Symph Prd
        if dataTableName=='fastoss-pm-metrics-snmp':
            dataTableName='snmp-enriched-metrics'

        #Determinar BH Level a calcular
        level = row['level']    
        level_list = json.loads(level) 
        level_sql = tuple(map(int, level_list))
        if len(level_sql) == 1:
            row_where = f"WHERE row_num = {level_sql[0]}"  
        else:
            row_where = f"WHERE row_num in {level_sql}"  
        
        var_effective_day = row['effectiveDay']
        range_day_where=''
        dayofweek_where=''    
        if var_effective_day=='SPECIALDATES':
            var_specialDate = datetime.strptime(row['date'], "%Y-%m-%dT%H:%M:%S%z") #Fecha configurada en Special Date
            var_specialDate_start = var_specialDate.replace(hour=0, minute=0, second=0)
            var_specialDate_end =  var_specialDate.replace(hour=23, minute=59, second=59)
            
            var_date_start =var_specialDate_start.strftime("%Y-%m-%d %H:%M:%S")
            var_date_end =var_specialDate_end.strftime("%Y-%m-%d %H:%M:%S")

            var_num_week = var_specialDate.isocalendar()[1]
            var_month = var_specialDate.month
        else:
            var_date_start, var_date_end, var_num_week, var_month = get_time_variables (param_execution_date, periodicity)
            var_data_datetime = datetime.strptime(var_date_start, "%Y-%m-%d %H:%M:%S")
            data_date_dow = var_data_datetime.weekday() + 1

            if var_effective_day=='PERIODDAYS':
                if "WEEKEND" in row['day1']:
                    dayofweek_where="AND EXTRACT(DOW FROM __time) in (6,7)" #Sabado - Domingo
                    if (periodicity=='DAILY_BUSY_HOUR') and (data_date_dow not in (6,7)):
                        var_validacion = False
                elif "WORKDAYS" in row['day1']:
                    dayofweek_where="AND EXTRACT(DOW FROM __time) in (1,2,3,4,5)" #Lunes a Viernes
                    if (periodicity=='DAILY_BUSY_HOUR') and (data_date_dow not in (1,2,3,4,5)):
                        var_validacion = False
            elif var_effective_day=='SPECIFICDAYS':
                day_to_number = {"MONDAY":1,"TUESDAY":2,"WEDNESDAY":3,"THURSDAY":4,"FRIDAY":5,"SATURDAY":6,"SUNDAY":7}
                days = json.loads(row['day2'].replace("'", '"'))
                day_numbers = [day_to_number[day] for day in days]
                day_numbers_sql = ', '.join(map(str, day_numbers))
                dayofweek_where=f"AND EXTRACT(DOW FROM __time) in ({day_numbers_sql})" 
                if (periodicity=='DAILY_BUSY_HOUR') and (data_date_dow not in day_numbers):
                        var_validacion = False
        
        range_day_where = f" AND __time BETWEEN TIMESTAMP '{var_date_start}' AND TIMESTAMP '{var_date_end}'"
        
        print("node_name:", node_name)
        print("data_date_dow:", data_date_dow)
        print("var_validacion:", var_validacion)

        if var_validacion:
            var_effective_hours = row['effective_hours']
            hours_where=''
            if 'ALL'not in var_effective_hours:
                hours_list = json.loads(var_effective_hours)
                hours_int = [int(hour.split(':')[0]) for hour in hours_list]
                hours_sql = ', '.join(map(str, hours_int))
                hours_where = f" AND EXTRACT(HOUR FROM __time) in ({hours_sql})"
            
            #Función de agregación del kpi
            agg_function = row['aggregationFunction']
            if agg_function == 'PERCENTILE':
                percentileValue = float(row['percentileValue'])/100
                agg_function_sql = f'ARRAY_QUANTILE(ARRAY_AGG("value", 40960), {percentileValue}/100)'
            else:   
                agg_function_sql = f'{agg_function}("value")'

            sql = f'''
                    SELECT "additionalDn", count("value") as conteo
                    FROM druid."{dataTableName}"
                    WHERE "metricId"='{kpi_id}' AND "granularityInterval"='{granularityInterval}' 
                            AND "managedObjectId"= '{managedObject_id}'
                            {range_day_where}
                            {dayofweek_where}
                            {hours_where}  
                    GROUP BY 1
                    '''     
            print("sql:", sql)
            response= execute_query_druid(sql)            
            df_result = pd.DataFrame(response)

            df_result['used'] = False

            df_sorted = df_result.sort_values(by='conteo', ascending=False).reset_index(drop=True)
            #max_capacity = 2000000
            max_capacity = int(Variable.get("BH_additionalDN_capacity"))
            used = [False] * len(df_sorted)            
            
            for index, row in df_sorted.iterrows():
                if used[index]:
                    continue
                
                current_capacity = row['conteo']
                current_group = [row['additionalDn']]
                used[index] = True
                
                for inner_index in range(index + 1, len(df_sorted)):
                    if not used[inner_index]:
                        additional_value = df_sorted.at[inner_index, 'conteo']
                        if current_capacity + additional_value <= max_capacity:
                            current_group.append(df_sorted.at[inner_index, 'additionalDn'])
                            current_capacity += additional_value
                            used[inner_index] = True
                
                data = {
                        #'var_date_start': datetime.strptime(var_date_start, "%Y-%m-%d %H:%M:%S"),
                        #'var_date_end': datetime.strptime(var_date_end, "%Y-%m-%d %H:%M:%S"),
                        #'var_execution_date': datetime.strptime(var_execution_date, "%Y-%m-%d %H:%M:%S"), 
                        'var_date_start': var_date_start,
                        'var_date_end': var_date_end,
                        'var_execution_date': var_execution_date, 
                        'var_num_week': var_num_week,
                        'var_month': var_month,
                        'additionalDn_group': current_group,
                        'additionalDn_total': current_capacity,
                        'node_id': node_id,
                        'node_name': node_name,
                        'var_periodicity': var_periodicity,
                        'kpi_id': kpi_id,
                        'kpi_name': kpi_name,
                        'managedObject_id': managedObject_id,
                        'managedObject_name': managedObject_name,
                        'granularityInterval': granularityInterval,
                        'agg_function_sql': agg_function_sql,
                        'dataTableName': dataTableName,
                        'range_day_where':range_day_where,
                        'dayofweek_where': dayofweek_where,
                        'hours_where': hours_where,
                        'dense_num': dense_num,
                        'row_where':row_where
                        }
                rows.append(data)
        else:
            print("BH ",node_id,"-", node_name, " not executed in Druid")
            if dataTableName=='':
                print("Eror. Table name is null")           

    df = pd.DataFrame(rows)
    df['additionalDn_group'] = df['additionalDn_group'].apply(tuple)
    #df_limited = df.head(10)

    size_by_node_name = df.groupby('node_name').size()
    #print("size_by_node_name", size_by_node_name)
    for node_name, size in size_by_node_name.items():
        print(f"node_name {node_name}: {size} filas")

    df_json = df.to_json(orient='records')
    #df_json = df_limited.to_json(orient='records')
    #return df_json
    
    # Guardar JSON en un archivo temporal
    temp_file = tempfile.NamedTemporaryFile(delete=False, suffix=".json")
    with open(temp_file.name, "w") as f:
        f.write(df_json)
    
    print("temp_file:", temp_file.name)
    
    # Devolver la ruta al archivo
    return temp_file.name

    
def calculate_busyhour(param_df, task_id):
    print(param_df)
    print(task_id)
    print(type(param_df))
    if not isinstance(param_df, list):
        raise ValueError("param_df debe ser una lista")
    
    if len(param_df) > 0 and not isinstance(param_df[0], dict):
        raise ValueError("Los elementos de param_df deben ser diccionarios")
    
    # df = pd.DataFrame(param_df)
    # df['additionalDn_group'] = df['additionalDn_group'].apply(lambda x: x if isinstance(x, list) else [x])
    # values_list = [item for sublist in df['additionalDn_group'] for item in sublist]
    # additionalDn_where = f"""AND "additionalDn" in ({', '.join(f"'{value}'" for value in values_list)})"""
    # catalogo_druid='pm_druid'
    # dataTableName = df['dataTableName'][0]
    # node_id = df['node_id'][0]
    # node_name = df['node_name'][0]
    # var_periodicity = df['var_periodicity'][0]

    # agg_function_sql = df['agg_function_sql'][0]
    # kpi_id = df['kpi_id'][0]
    # kpi_name = df['kpi_name'][0]
    # granularityInterval = df['granularityInterval'][0]
    # managedObject_id = df['managedObject_id'][0]
    # managedObject_name = df['managedObject_name'][0]
    # range_day_where = df['range_day_where'][0]
    # dayofweek_where = df['dayofweek_where'][0]
    # hours_where = df['hours_where'][0]
    # dense_num = df['dense_num'][0]
    # row_where = df['row_where'][0]

    # var_date_start = df['var_date_start'][0]
    # var_date_end = df['var_date_end'][0]
    # var_execution_date =  df['var_execution_date'][0] 
    # var_num_week = df['var_num_week'][0]
    # var_month = df['var_month'][0]
    # var_data_datetime = datetime.strptime(var_date_start, "%Y-%m-%d %H:%M:%S") 
    # file_date = var_data_datetime.strftime("%Y%m%d")    

    # var_date_start = datetime.strptime(var_date_start, '%Y-%m-%d %H:%M:%S')
    # var_date_end = datetime.strptime(var_date_end, '%Y-%m-%d %H:%M:%S')
    # var_execution_date = datetime.strptime(var_execution_date, '%Y-%m-%d %H:%M:%S')  

    # print("task_id: ",task_id)
    # print("kpi_name:", kpi_name)
    # print("var_date_start: ",var_date_start)
    # print("var_date_end: ",var_date_end)
    # #print("var_execution_date: ",var_execution_date)
    # #print("var_num_week: ",var_num_week)
    # #print("var_month: ",var_month)

    # dense_where = ''
    # row_number = ''
    # if dense_num == 1:
    #     dense_where = " AND dense_num=1"
    #     row_number = "ROW_NUMBER() OVER (PARTITION BY dn, dense_num ORDER BY metric DESC, __time ASC) AS row_num"
    # else:
    #     row_number= "ROW_NUMBER() OVER (PARTITION BY dn ORDER BY dn ASC, metric DESC, __time ASC) AS row_num"

    # sql = f'''
    #         WITH datos_agrupados AS (
    #             SELECT "__time", day_date, "dn", "category", metric,
    #                 DENSE_RANK() OVER (PARTITION BY dn, day_date ORDER BY metric DESC, __time ASC) AS dense_num
    #             FROM (
    #                 SELECT DATE_TRUNC('hour', __time) AS __time, DATE_TRUNC('day', __time) AS day_date, 
    #                         "dn", "category", {agg_function_sql} AS metric
    #                 FROM druid."{dataTableName}"
    #                 WHERE "metricId"='{kpi_id}' AND "granularityInterval"='{granularityInterval}' 
    #                     AND "managedObjectId"= '{managedObject_id}'
    #                     {range_day_where}
    #                     {dayofweek_where}
    #                     {hours_where}                  
    #                     {additionalDn_where}                                   
    #             GROUP BY DATE_TRUNC('hour', __time), DATE_TRUNC('day', __time),"dn", "category"
    #             ) AS subquery
    #         ), datos_ordenados AS (
    #         SELECT "__time", day_date, "dn", "category", metric, dense_num,
    #             {row_number}
    #         FROM datos_agrupados            
    #         )
    #         SELECT "__time", day_date, "dn", "category", metric, dense_num, row_num
    #         FROM datos_ordenados
    #         {row_where}{dense_where}
    #         ORDER BY dn, row_num, dense_num
    #     '''
    # print("SQL:", sql)

    # try:
    #     print("SQL:", sql)
    #     df_result= execute_query_druid(sql)
    #     print("Tamaño del resultado:", len(df_result))
    #     #df_result = pd.DataFrame(response)
    #     if not df_result.empty:
    #         #df_result['__time_str'] = df_result['__time'].dt.strftime('%Y-%m-%d %H:%M:%S')
    #         rows = []
    #         for _, result in df_result.iterrows(): 
    #             data = {
    #                 'data_date_start': var_date_start,
    #                 'data_date_end': var_date_end,
    #                 'data_date_week': var_num_week,
    #                 'data_date_month': var_month,
    #                 'BH_configuration_id': node_id,
    #                 'BH_configuration_name': node_name,
    #                 'periodicity': var_periodicity,
    #                 'execution_date': var_execution_date, 
    #                 'dn': result['dn'],
    #                 'kpi_id': kpi_id,
    #                 'kpi_name': kpi_name,
    #                 'category': str(result['category']),
    #                 'MO_id': managedObject_id,
    #                 'MO_name': managedObject_name,
    #                 'granularityInterval': granularityInterval,
    #                 #'BH_hour': result['__time'] , #result['__time_str'],
    #                 'BH_hour': pd.to_datetime(result['__time']) , #result['__time_str'],
    #                 'BH_value': result['metric'],
    #                 'BH_level': result['row_num'],
    #                 }

    #             rows.append(data)
            
    #         df = pd.DataFrame(rows)
    #         save_to_datalake(df, node_id, file_date, var_periodicity, task_id)
    #     else:
    #         print("No records found.")
    # except Exception as e:
    #     raise AirflowException(f"Error: {e}")     
    
            
def save_to_datalake(param_df, param_node_id, param_date, param_periodicity, task_id):
    fs = s3fs.S3FileSystem(
        key='lHWxa5VzXNpcP0Hn5gDL',
        secret='3GlM6514haf0v9E6gPCxN1bmYEDnaXAvJIgtyuBC',
        client_kwargs={'endpoint_url': 'http://minio-dev.panda-symphony-dev.svc.cluster.local:9000'}
    )

    bucket_name = "busy-hour"
    if not fs.exists(bucket_name):
        fs.mkdir(bucket_name)
    
    #file_path = f'{bucket_name}/datalake/assurance/pm/busyhour/{filename}'}
    file_base = f'{param_date}_{param_node_id}_busyhour_{param_periodicity}_values'
    directory_path = f'{bucket_name}/stage/{param_periodicity}/{file_base}'
    filename = f'{file_base}-{task_id}.parquet'
    file_path = f'{directory_path}/{filename}'

    # Eliminar el archivo si ya existet
    if fs.exists(file_path):
        fs.rm(file_path)

    with fs.open(file_path, 'wb') as f:
        param_df.to_parquet(f, engine='pyarrow')

    print(f"Parquet file uploaded {file_path}")


def consolidate_output_files(periodicity):
    fs = s3fs.S3FileSystem(
        key='lHWxa5VzXNpcP0Hn5gDL',
        secret='3GlM6514haf0v9E6gPCxN1bmYEDnaXAvJIgtyuBC',
        client_kwargs={'endpoint_url': 'http://minio-dev.panda-symphony-dev.svc.cluster.local:9000'}
    )

    var_periodicity = periodicity.split('_')[0].lower()

    bucket_name = "busy-hour"
    stage_path = f'{bucket_name}/stage/{var_periodicity}'
    print("stage_path", stage_path)

    # Obtener todas las subcarpetas dentro de la periodicidad
    if fs.exists(stage_path):
        sub_dirs = fs.ls(stage_path)
    else:
        print(f"El directorio {stage_path} no existe.")
        sub_dirs = []

    print("sub_dirs:", sub_dirs)

    for sub_dir in sub_dirs:
        # Obtener todos los archivos en la carpeta
        file_paths = fs.glob(f'{sub_dir}/*.parquet')

        tables = []
        for file_path in file_paths:
            with fs.open(file_path, 'rb') as f:
                table = pq.read_table(f)
                tables.append(table)

        if tables:
            combined_table = pa.concat_tables(tables)

            # Derivar el nombre del archivo combinado de la carpeta
            directory_name = sub_dir.split('/')[-1]
            combined_filename = f'{directory_name}.parquet'
            #combined_file_path = f'{bucket_name}/datalake/assurance/pm/busyhour/{combined_filename}'
            combined_file_path = f'{stage_path}/{combined_filename}'

            with fs.open(combined_file_path, 'wb') as f:
                pq.write_table(combined_table, f)

            print(f"Archivos combinados: {combined_file_path}")

            # Eliminar la carpeta completa
            fs.rm(sub_dir, recursive=True)

    # tables = []
    # for sub_dir in sub_dirs:
    #     print("sub_dir:", sub_dir)

    #     # Obtener todos los archivos en la carpeta
    #     file_paths = fs.glob(f'{sub_dir}/*.parquet')
        
    #     for file_path in file_paths:
    #         # Obtener el directorio
    #         directory = '/'.join(file_path.split('/')[:-1])
            
    #         with fs.open(file_path, 'rb') as f:
    #             table = pq.read_table(f)
    #             tables.append({
    #                 'directory': directory,  # Guardar el directorio
    #                 'file_path': file_path,  # Guardar la ruta
    #                 'data': table  # Guardar el contenido
    #             })

        # Eliminar la carpeta completa
        #fs.rm(sub_dir, recursive=True)
        #print(f"Carpeta eliminada: {sub_dir}")

    # if tables:
    #     print(f"Número de tablas guardadas: {len(tables)}")

    #     tables_by_group = {}
    #     for table_info in tables:
    #         directory_group = table_info['directory']  

    #         # Incrementar el contador por directorio
    #         tables_by_group.setdefault(directory_group, 0)
    #         tables_by_group[directory_group] += 1

    #     # Mostrar el conteo de tablas por ruta
    #     for directory, count in tables_by_group.items():
    #         print(f"Ruta: {directory} tiene {count} tablas")

    #     return tables  # Retorna los datos y las rutas                
    # else:
    #     print(f"No se encontraron archivos en {sub_dir}")
    #     return None
        
           
def save_combined_files_to_datalake(periodicity):
    fs = s3fs.S3FileSystem(
        key='lHWxa5VzXNpcP0Hn5gDL',
        secret='3GlM6514haf0v9E6gPCxN1bmYEDnaXAvJIgtyuBC',
        client_kwargs={'endpoint_url': 'http://minio-dev.panda-symphony-dev.svc.cluster.local:9000'}
    )

    var_periodicity = periodicity.split('_')[0].lower()

    bucket_name = "busy-hour"
    stage_path = f'{bucket_name}/stage/{var_periodicity}'
    print("stage_path", stage_path)
    
    file_paths = fs.glob(f"{stage_path}/*.parquet")
    print(f"Archivos encontrados: {file_paths}")

    try:
        for file in file_paths:
            print(f"Procesando archivo: {file}")
            filename = file.split('/')[-1]            
            combined_file_path = f'{bucket_name}/datalake/assurance/pm/busyhour'
            output_file = f"{combined_file_path}/{filename}"

            # Mover el archivo
            fs.mv(file, output_file)
            print(f"Archivo guardado en {output_file}")
    except Exception as e:
        print(f"Error al guardar el archivo {file}: {e}")
        
    #with fs.open(combined_file_path, 'wb') as f:
    #    pq.write_table(table, f)
    
    #print(f"Archivo guardado en MinIO: {combined_file_path}")

    # grouped_data = {}    
    # for table_info in param_tables:
    #     directory = table_info['directory']
    #     json_data = table_info['data']  # Aquí es donde recibes los datos en formato JSON
    #     df = pd.read_json(StringIO(json_data), orient='records') 
    #     data_table = pa.Table.from_pandas(df)
        
    #     # Si el directorio ya existe en el diccionario, combinar las tablas
    #     if directory in grouped_data:
    #         grouped_data[directory] = pa.concat_tables([grouped_data[directory], data_table])
    #     else:
    #         grouped_data[directory] = data_table

    # combined_file_path = f'{bucket_name}/datalake/assurance/pm/busyhour'

    # # Guardar las tablas combinadas a MinIO
    # for directory, combined_table in grouped_data.items():
    #     # Crear el nombre del archivo
    #     filename = directory.split('/')[-1]
    #     output_file = f"{combined_file_path}/{filename}"
        
    #     with fs.open(output_file, 'wb') as f:
    #         pq.write_table(combined_table, f)

    #     print(f"Archivos combinados: {output_file}")


def send_to_kafka(periodicity):
    #topic = Variable.get("topic")
    kafka_topic = "fastoss-pm-busyhour"
    pem_content = Variable.get("pem_content_dev")

    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(pem_content.encode())

    ssl_location = temp_file.name

    connection_id = 'kafka_dev'  
    connection = Connection.get_connection_from_secrets(connection_id)

    bootstrap_servers = connection.extra_dejson.get('bootstrap.servers')
    security_protocol = connection.extra_dejson.get('security.protocol')
    sasl_mechanism = connection.extra_dejson.get('sasl.mechanism')
    sasl_username = connection.extra_dejson.get('sasl.username')
    sasl_password = connection.extra_dejson.get('sasl.password')
    
    conf = {
        'bootstrap.servers': bootstrap_servers,
        'security.protocol': security_protocol,
        'sasl.mechanism': sasl_mechanism,
        'sasl.username': sasl_username,
        'sasl.password': sasl_password,
        'ssl.ca.location': ssl_location
        }

    ssl_context = ssl.create_default_context()
    ssl_context.load_verify_locations(conf['ssl.ca.location'])
    producer =  Producer(**conf)  
    
    fs = s3fs.S3FileSystem(
        key='lHWxa5VzXNpcP0Hn5gDL',
        secret='3GlM6514haf0v9E6gPCxN1bmYEDnaXAvJIgtyuBC',
        client_kwargs={'endpoint_url': 'http://minio-dev.panda-symphony-dev.svc.cluster.local:9000'}
    )

    var_periodicity = periodicity.split('_')[0].lower()

    bucket_name = "busy-hour"
    stage_path = f'{bucket_name}/stage/{var_periodicity}'
    print("stage_path", stage_path)
    
    file_paths = fs.glob(f"{stage_path}/*.parquet")
    print(f"Archivos encontrados: {file_paths}")
    cont=0

    try:
        for file_path in file_paths:
            print(f"Procesando archivo: {file_path}")
            with fs.open(file_path, 'rb') as f:
                table = pq.read_table(f)  # Leer usando el descriptor de archivo

            df = table.to_pandas()

            for _, record in df.iterrows():
                cont += 1
                record_str = record.to_json()  # Convertir cada fila a JSON
                record_bytes = record_str.encode('utf-8')  # Convertir a bytes

                # Enviar mensaje a Kafka
                try:
                    producer.produce(kafka_topic, key='key', value=record_bytes)
                    if cont % 100000 == 0:
                        producer.flush()
                        print(f"Se realizó flush después de {cont} registros")
                except Exception as e:
                    print(f"Error al enviar a Kafka: {e}")

        # Asegurarse de que todos los mensajes sean enviados
        producer.flush()
        print(f"Mensajes enviados a Kafka en el tópico: {kafka_topic}")
        print(f"No. de Mensajes Enviados: {cont}")
    
    finally:       
        os.remove(ssl_location)  # Eliminar el archivo temporal

    # for table_info in param_tables:
    #     json_data = table_info['data']  
    #     if isinstance(json_data, str):
    #         json_data = json.loads(json_data)
            
    #     if isinstance(json_data, list):
    #         for record in json_data:
    #             cont=cont+1
    #             # Convertir cada registro a string y luego a bytes
    #             record_str = json.dumps(record)
    #             record_bytes = record_str.encode('utf-8')

    #             # Enviar cada registro como un mensaje independiente
    #             try:
    #                 producer.produce(kafka_topic, key='key', value=record_bytes)
    #                 if cont % 100000 == 0:
    #                     producer.flush()
    #                     print(f"Se realizó flush después de {cont} registros")
    #             except Exception as e:
    #                 print(f"Error al enviar a Kafka: {e}")                    
    
    # producer.flush()
    # print(f"Mensajes enviados a Kafka en el tópico: {kafka_topic}")
    # print("No. de Mensajes Enviados:", cont)


