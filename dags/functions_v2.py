# Nome da DAG: functions_v2
# Owner / responsável: CoE
# Descrição do objetivo da DAG: Módulo de funciones en Python para el procesamiento de DAGs de Alarmas en el ambiente producción
# Usa Druid?: Si
# Principais tabelas / consultas Druid acessadas: 
# Frequência de execução (schedule): 
# Dag Activo?: 
# Autor: CoE
# Data de modificação: 2025-05-26

from airflow.hooks.base_hook import BaseHook
from sqlalchemy import create_engine
from sqlalchemy import text
from airflow.models import Variable
from airflow.models import Connection
from datetime import datetime, timedelta
from confluent_kafka import Producer
from airflow.providers.http.hooks.http import HttpHook
import calendar

from io import StringIO
import ast 

import pandas as pd
import requests
import tempfile
import json
import time
import re
import ssl
import jaydebeapi 

#connection = BaseHook.get_connection('postgres_default')
connection = BaseHook.get_connection('postgres_prd')
engine = create_engine(f"postgresql://{connection.login}:{connection.password}@{connection.host}:{connection.port}/{connection.schema}")

#tb_threshold = "panda.threshold_dev"
#tb_control_load = "panda.control_load_data_dev"

tb_threshold = "panda.threshold"
tb_control_load = "panda.control_load_data"

#SELECT
def executeQuery(query):    
    with engine.connect() as conn:
        result = pd.read_sql(query, conn)
    return result 

#INSERT, UPDATE, DELETE
def execute_Query(query, parameters, action, get_id):
    id = 0
    with engine.connect() as conn:
        if action in ["DELETE", "UPDATE"]:
            conn.execute(text(query))  
        elif action == "INSERT":
            result = conn.execute(text(query), **parameters)
            if get_id and result.returns_rows:  
                row = result.fetchone()
                if row is not None:
                    id = row[0]
    return id


def postgresData(aggretation, granularity):
    aggregation_filter = ""
    if aggretation == "dataAggregationTimeGranularity": 
        aggregation_filter = " AND (\"dataAggregationTime\"= 'NONE' or \"dataAggregationTime\"='null')"
    
    query = f"""               
        SELECT node_id, node_name, "dataAggregationTime", "dataAggregationTimeGranularity", 
            "dataAggregationFunction", "dataAggregationPercentileValue", "spatialAggregation", 
            "spatialAggregationFunction", "spatialAggregationPercentileValue", "id_spatialLocationTypeAggregation",
            "spatialLocationTypeAggregation", "id_spatialManagedObjectAggregation", "spatialManagedObjectAggregation", 
            "managedObject_id", "managedObject_name", "dataTableName", expression_id, "ngExclusion", "id_ngExclusion", 
            "ngInclusion", "id_ngInclusion", "id_ngInclusion_SG", "typeAssociation", "equipmentExclusion", "id_equipmentExclusion", 
            "equipmentInclusion", "id_equipmentInclusion", kpi_id, kpi_name, "kpi_defaultTimeGranularity", "kpi_isRate",
            "kpi_unit", "kpi_hourly", "kpi_daily", "kpi_weekly", "kpi_monthly", "kpi_yearly", "kpi_fifteendays",
            "kpi_quarterly", "kpi_semesterly", hourlypercentile, dailypercentile, weeklypercentile,  monthlypercentile,
            yearlypercentile, fifteendayspercentile,  quarterlypercentile, semesterlypercentile,
            algorithm, algorithmComparisonValue, algorithmComparisonType, intervalDays, times, effectiveDays, optionsDays, effectiveHours, 
            execution_date, load_status,"timeZone"
        FROM {tb_threshold}
        WHERE upper("{aggretation}")='{granularity}'{aggregation_filter};
        """ 
    print(query)
    result = executeQuery(query)
    resut_json = result.to_json(orient='records')
    return resut_json


def get_time_variables(var_datetime, data_aggregation_time, v_timeZone):
    var_data_aggregation_time = ""
    sw = 1
    if data_aggregation_time == 'HOUR':
        sw = 0
        var_data_aggregation_time = "PT1H"
        var_date = datetime(var_datetime.year, var_datetime.month, var_datetime.day, var_datetime.hour, minute=00, second=00)                                    
        var_start = var_date - timedelta(minutes=60) 
        var_end = var_date - timedelta(seconds=1)        
    elif data_aggregation_time == 'DAY': 
        var_data_aggregation_time = "P1D"
        var_start = datetime(var_datetime.year, var_datetime.month, var_datetime.day) - timedelta(days=1)
        var_end = datetime(var_datetime.year, var_datetime.month, var_datetime.day, 23, 59, 59)  - timedelta(days=1)              
    elif data_aggregation_time == 'WEEK':
        var_data_aggregation_time = "P1W" 
        var_week = var_datetime - timedelta(days=var_datetime.weekday()+7)
        var_start = datetime(var_week.year, var_week.month, var_week.day)
        var_end = var_start + timedelta(days=6, hours=23, minutes=59, seconds=59)                
    elif data_aggregation_time == 'MONTH':
        var_data_aggregation_time = "P1M"                   
        month = (12 if var_datetime.month == 1 else var_datetime.month-1)
        year = (var_datetime.year-1 if var_datetime.month == 1 else var_datetime.year)
        var_start = (datetime(year, month, 1))  
        var_end = (datetime(var_datetime.year, var_datetime.month, 1)) - timedelta(seconds=1) 
    elif data_aggregation_time == 'FITTEEN_DAYS':
        var_data_aggregation_time = "P15D"    
        v_day = var_datetime.day    
        year = (var_datetime.year-1 if var_datetime.month==1 else var_datetime.year)    
        month = (12 if var_datetime.month == 1 else var_datetime.month-1)
        last_day = calendar.monthrange(year, month)[1]
        if v_day >= 1 and v_day <= 15:
            var_start = datetime(year, month, 16)
            var_end = datetime(year, month, last_day) + timedelta(minutes=59, seconds=59)   
        else:
            var_start = datetime(var_datetime.year, var_datetime.month, 1) 
            var_end = datetime(var_datetime.year, var_datetime.month, 16) - timedelta(seconds=1) 
    
    timeZone = "Z"  # Valor por defecto

    if sw == 1:
        timeZone_list = v_timeZone.split(",")
        print("timeZone_list:", timeZone_list[0])
        if (len(timeZone_list) > 1) and (timeZone_list[0] != "+00:00"):
            timeZone = timeZone_list[0]
            # Cambiar el signo si es necesario
            #timeZone = timeZone.replace("+", "-") if timeZone[0] == "+" else timeZone.replace("-", "+")  
        
    formatted_init_datetime = var_start.strftime("%Y-%m-%dT%H:%M:%S") + timeZone
    formatted_end_datetime = var_end.strftime("%Y-%m-%dT%H:%M:%S") + timeZone
    timeCriteria = f'''\"timeGranularity\": \"{var_data_aggregation_time}\",''' 
    return formatted_init_datetime, formatted_end_datetime, timeCriteria


def get_spatial_time_variables(var_datetime, minutos):
    datetime_ = datetime(var_datetime.year, var_datetime.month, var_datetime.day, var_datetime.hour, minute=var_datetime.minute, second=00)
    var_minutos = datetime_.minute 
    print("Fecha Base:", datetime_)

    if minutos == 5:
        # Delta
        datetime_ = datetime_ - timedelta(minutes=10)
        print("Fecha Base (Espacial 5M - Delta 10minutes):", datetime_)
   
    var_mod = (var_minutos%minutos)                 
    var_end = (datetime_ - timedelta(minutes=var_mod))                       
    var_start = var_end -timedelta(minutes=minutos)       
    var_end = var_end-timedelta(seconds=1)

    formatted_init_datetime = var_start.strftime("%Y-%m-%dT%H:%M:%SZ")
    formatted_end_datetime = var_end.strftime("%Y-%m-%dT%H:%M:%SZ")
    return formatted_init_datetime,formatted_end_datetime


def get_spacial_variables(spatial_Aggregation,spatial_Aggregation_Function,spatial_LocationType_Aggregation,spatial_ManagedObject_Aggregation):
    sliceBy = ""
    if spatial_Aggregation_Function != "NONE":
        if spatial_Aggregation == "BY_LOCATION":
            sliceBy = "\"location." + str(spatial_LocationType_Aggregation) + ".name\","       
        elif spatial_Aggregation == "BY_TOPOLOGY":
            sliceBy ="\"topology." + str(spatial_ManagedObject_Aggregation) + ".name\","
        elif spatial_Aggregation == "BY_GROUP":
            sliceBy = "\"networkGroups\","           
    return sliceBy


def get_cadena_query_adapter(dataTableName,formatted_init_datetime,formatted_end_datetime,data_aggregation_time_granularity,timeCriteria,cadena,sliceBy,neGroupingFilter,manageObjectId,dimFilter, elementType, vendor, equipmentId, locationId, hours_query, dayofweek_query):
    query_druid=f'''{{
                \"query\": \"query getSql($datasource: String!, $preAggType: PreAggType!, $timeFilter: TimeFilterInput!, $criteria: MetricInput!){{getSqlQueryFromCriteria(dataSource: $datasource preAggType: $preAggType timeFilter: $timeFilter criteria: $criteria)}}\",
                \"variables\": {{
                \"datasource\": \"{dataTableName}\",
                \"preAggType\": \"RAW\",
                \"timeFilter\": {{
                    \"fixed\": {{
                    \"fromTime\": \"{formatted_init_datetime}\",
                    \"toTime\": \"{formatted_end_datetime}\"
                    }}
                }},
                \"criteria\": {{
                    "useLookups":true,
                    \"granularityIntervalFilter": \"{data_aggregation_time_granularity}\",
                    {timeCriteria}
                    \"metrics\": [{cadena}
                    ],
                    \"sliceBy\": [
                    {sliceBy}                        
                    \"id\",
                    \"metricId\",
                    \"metric.name\",
                    \"networkType\",
                    \"category\",
                    \"subcategory\",
                    \"familyId\",
                    \"familyName\",
                    \"metricType\", 
                    {elementType}                       
                    \"technology\",
                    \"cellName\",                        
                    {vendor}
                    {equipmentId}
                    {locationId}
                    \"domain\",
                    \"equipmentTypeId\",                   
                    \"equipment_type.name\",
                    \"managedObjectId\",
                    \"managed_object.name\"                     
                    ], 
                    {neGroupingFilter}                   
                    \"includeDims\": false,
                    \"dimFilter\": [
                    {{
                        \"dimName\": \"managedObjectId\",
                        \"filterOp\": \"EQUALS\",
                        \"stringValue\": \"{manageObjectId}\"
                    }}                        
                    {dimFilter}
                    ]
                }}
                }}
            }}'''
    print(query_druid)
    query_druid=query_druid.encode('utf-8')
    return query_druid


def data_load_control(param_df_threshold,var_datetime,aggregationTime,minutos):
    df = pd.read_json(StringIO(param_df_threshold), orient='records')
    if len(df) != 0:
        distinct_thresholds = df['node_id'].unique()       
        
        for threshold in distinct_thresholds:
            row_thresholds = df[df['node_id'] == threshold]
            
            for _, row in row_thresholds.iterrows():   
                #manageObjectName = row['managedObject_name'] 
                manageObjectId = row['managedObject_id']
                data_aggregation_time_granularity = row['dataAggregationTimeGranularity'] if row['dataAggregationTimeGranularity'] != "null" else row['kpi_defaultTimeGranularity']
                dataTableName = row['dataTableName']
                networkgroups = row['id_ngInclusion']
                equipments = row['equipmentInclusion']
                #kpi_name = row['kpi_name']
                node_id = row['node_id']
                kpi_id = row['kpi_id']

                data_aggregation_time = row['dataAggregationTime'].upper()
                spatial_Aggregation = row['spatialAggregation'].upper() 
                if data_aggregation_time != "NONE":
                    formatted_init_datetime, formatted_end_datetime, timeCriteria=get_time_variables(var_datetime, aggregationTime, "")
                
                if spatial_Aggregation != "NONE" and data_aggregation_time == "NONE":                    
                    formatted_init_datetime, formatted_end_datetime=get_spatial_time_variables(var_datetime,minutos)   
            
            get_data_load(node_id, dataTableName, kpi_id, manageObjectId, data_aggregation_time_granularity, formatted_init_datetime, formatted_end_datetime, networkgroups, equipments, aggregationTime, minutos)   
                     

def get_granularity(data_aggregation_time_granularity):
    if data_aggregation_time_granularity == "PT5M":
        var_data_time_granularity = "PT300S"                                                  
    elif data_aggregation_time_granularity == "PT15M":
        var_data_time_granularity = "PT900S"                                                                             
    elif data_aggregation_time_granularity == "PT30M":
        var_data_time_granularity = "PT1800S"                    
    elif data_aggregation_time_granularity == 'PT1H':
        var_data_time_granularity = "PT3600S"
    
    return  var_data_time_granularity
     

def druid_adapter_query(param_df_druid, aggregationTime, var_datetime, minutos):
    url_druid_adapter = Variable.get("url-druidAdapter")
    print("url:", url_druid_adapter)
    headers = {'Content-Type': 'application/json'}
 
    df = pd.read_json(StringIO(param_df_druid), orient='records')
    if len(df) != 0:
        sql_queries = []       
        cadena = ''
        var_data_time_granularity = ""
        for _, row in df.iterrows():   
            data_aggregation_time = row['dataAggregationTime'].upper()
            spatial_Aggregation = row['spatialAggregation'].upper() 
            spatial_Aggregation_Function = row['spatialAggregationFunction'] 
            data_aggregation_function = row['dataAggregationFunction']
            data_aggregation_percentile_value = row['dataAggregationPercentileValue']
            spatial_Aggregation_PercentileValue = row['spatialAggregationPercentileValue']
            spatial_LocationType_Aggregation = row['spatialLocationTypeAggregation']
            spatial_ManagedObject_Aggregation = row['id_spatialManagedObjectAggregation']
            percentil_spatial = ",\"percentile\":" + str(spatial_Aggregation_PercentileValue)            
            data_aggregation_time_granularity = row['dataAggregationTimeGranularity'] if row['dataAggregationTimeGranularity'] != "null" else row['kpi_defaultTimeGranularity']
            dataTableName = row['dataTableName']
            #manageObjectName = row['managedObject_name']       
            manageObjectId = row['managedObject_id']       
            kpi_isRate = row['kpi_isRate']  
            kpi_unit = row['kpi_unit']
            kpi_hourly = row['kpi_hourly'] 
            kpi_daily = row['kpi_daily'] 
            kpi_weekly = row['kpi_weekly'] 
            kpi_monthly = row['kpi_monthly'] 
            kpi_yearly = row['kpi_yearly'] 
            #kpi_fifteendays = row['kpi_fifteendays'] 
            #kpi_quarterly = row['kpi_quarterly'] 
            #kpi_semesterly = row['kpi_semesterly'] 
            hourlyPercentile = row['hourlypercentile'] 
            dailyPercentile = row['dailypercentile'] 
            weeklyPercentile = row['weeklypercentile'] 
            monthlyPercentile = row['monthlypercentile'] 
            yearlyPercentile = row['yearlypercentile'] 
        	#fifteenDaysPercentile = row['fifteendaysPercentile']       
            #quarterlyPercentile = row['quarterlyPercentile'] 
        	#semesterlyPercentile = row['semesterlyPercentile'] 
            
            algorithm = row["algorithm"]  

            effectivehours = row["effectivehours"]
            hours_query  = ''
            if 'ALL' not in effectivehours:
                if isinstance(effectivehours, str) and effectivehours.startswith("[") and effectivehours.endswith("]"):
                    effectivehours = ast.literal_eval(effectivehours) 
                effectivehours_int = [str(int(h)) for h in effectivehours] 
                hours_query = f'"hours": [{", ".join(effectivehours_int)}],'
            #print ("hours_query:", hours_query)

            effectivedays = row["effectivedays"]  
            optionsdays = row["optionsdays"]  
            dayofweek_query = ''
            if (effectivedays == 'PERIODDAYS') and 'ALLDAYS' not in optionsdays:
                if "WEEKEND" in optionsdays:
                    dayofweek = '[\"SATURDAY\", \"SUNDAY\"]' 
                elif "WORKDAYS" in optionsdays:
                    dayofweek = '\"MONDAY\", \"TUESDAY\", \"WEDNESDAY\", \"THURSDAY\", \"FRIDAY\"'
                else:
                    dayofweek = []
               
                dayofweek_query = f'''\"days\": {{
                                            \"daysOfWeek\": [{dayofweek}]
                                        }},'''
            # elif effectivedays=='SPECIFICDAYS':
            #     day_to_number = {"MONDAY":1,"TUESDAY":2,"WEDNESDAY":3,"THURSDAY":4,"FRIDAY":5,"SATURDAY":6,"SUNDAY":7}
            #     days = json.loads(row['day2'].replace("'", '"'))
            #     day_numbers = [day_to_number[day] for day in days]
            #     day_numbers_sql = ', '.join(map(str, day_numbers))
            #print ("dayofweek_query:", dayofweek_query)

            ngInclusion = ""
            equimentInclusion = ""
            timeCriteria = "" 
            aggregation = ""
            sliceBy = ""
            dimFilter = ""
            elementType = ""
            is_rate = ""
            showAsPercentage = ""
            vendor = ""
            equipmentId = ""
            locationId = ""
            
            if kpi_isRate:
                is_rate = "\"isRate\": true,"

            if kpi_unit == "Percentage":
                showAsPercentage = "\"showAsPercentage\": true,"
            
            v_timeZone = row['timeZone']
            var_data_time_granularity = get_granularity(data_aggregation_time_granularity)
                
            if data_aggregation_time != "NONE":    #Temporal o #Temporal-Espacial
                formatted_init_datetime, formatted_end_datetime, timeCriteria = get_time_variables(var_datetime, aggregationTime, v_timeZone)
                aggregation = "temporal"

                if spatial_Aggregation == "NONE":   #Temporal
                    sliceBy = '''\"dn\",
                    \"additionalDn\",''' 
                    
                    elementType = "\"elementType\","     
                    vendor = "\"vendorId\","        
                    equipmentId = "\"equipmentId\","  
                    locationId = "\"locationId\","                    
                else:
                    aggregation += "-"  #Temporal-Espacial
            else:   
                if spatial_Aggregation != "NONE":     #Espacial               
                    formatted_init_datetime, formatted_end_datetime = get_spatial_time_variables(var_datetime,minutos) 
            
            if spatial_Aggregation != "NONE" and spatial_Aggregation_Function != "null":
                #sliceBy = get_spacial_variables(spatial_Aggregation, spatial_Aggregation_Function, spatial_LocationType_Aggregation, spatial_ManagedObject_Aggregation)
                
                if spatial_Aggregation_Function != "NONE":
                    if spatial_Aggregation == "BY_LOCATION":
                        sliceBy = "\"location." + str(spatial_LocationType_Aggregation) + ".name\","       
                    elif spatial_Aggregation == "BY_TOPOLOGY":
                        sliceBy ="\"topology." + str(spatial_ManagedObject_Aggregation) + ".name\","
                    elif spatial_Aggregation == "BY_GROUP":
                        sliceBy = "\"networkGroups\","  
                
                aggregation += "spatial"

            neGroupingFilter = ""
            dnFilter = ""
            typeAssociation = json.loads(row['typeAssociation'])
            if all(assoc == "SUPERGROUP" for assoc in typeAssociation):
                ngInclusion = row['id_ngInclusion_SG']
            else:
                ngInclusion = row['id_ngInclusion']

            if ngInclusion != "[]":                
                neGroupingFilter = "\"neGroupingFilter\":" + ngInclusion + ","
            
            equimentInclusion = row['equipmentInclusion']
            if equimentInclusion != "[]":                
                dimFilter = f''',{{\"dimName\": \"dn\",
                                \"filterOp\": \"IN\",
                                \"listValue\": {equimentInclusion}              
                }}'''
                dn = equimentInclusion.replace("[","(").replace("]",")").replace('"', "'")
                dnFilter = "AND \"dn\" IN "+ dn       
            
            groupFilter = ""
            if neGroupingFilter != "":
                groupFilter = "AND ARRAY_OVERLAP(\"networkGroups\", ARRAY " + ngInclusion.replace('"', "'") + ")"
            
            query_select = f'''
                    SELECT min(__time) min_record, max(__time) max_record, '{aggregation}' aggregation, '{data_aggregation_time}' aggregationTime, 
                        '{spatial_Aggregation}' spatialAggregation, '{spatial_LocationType_Aggregation}' spatiallocation,
                        '{spatial_ManagedObject_Aggregation}' spatialMO, '{ngInclusion}' networkGroup, '{row['node_id']}' node_id,
                        '{kpi_isRate}' isRate
                    FROM \"{dataTableName}\"
                    WHERE \"metricId\" = '{row['kpi_id']}' AND \"managedObjectId\" = '{manageObjectId}' AND \"granularityInterval\" = '{var_data_time_granularity}'
                    AND __time BETWEEN TIME_PARSE('{formatted_init_datetime}') AND TIME_PARSE('{formatted_end_datetime}') {groupFilter} {dnFilter} 
                '''
           
            functionTime = ""
            functionSpatial = ""
            percentile_value = ""
            percentil_time = ""
            if data_aggregation_time != "NONE" and data_aggregation_function != "null":
                if data_aggregation_function == 'DEFAULT':
                    if data_aggregation_time == 'HOUR':
                        functionTime = kpi_hourly
                        percentile_value = hourlyPercentile
                    elif data_aggregation_time == 'DAY':
                        functionTime = kpi_daily
                        percentile_value = dailyPercentile
                    elif data_aggregation_time == 'WEEK':
                        functionTime = kpi_weekly
                        percentile_value = weeklyPercentile
                    elif data_aggregation_time == 'MONTH':
                        functionTime = kpi_monthly
                        percentile_value = monthlyPercentile
                    elif data_aggregation_time == 'YEAR':
                        functionTime = kpi_yearly
                        percentile_value = yearlyPercentile
                else:
                    functionTime = data_aggregation_function
                    percentile_value = data_aggregation_percentile_value
            else:
                functionTime = "NONE"
            
            if functionTime == "PERCENTILE":
                percentil_time = ",\"percentile\":" + str(percentile_value)

            if spatial_Aggregation != "NONE" and spatial_Aggregation_Function != "null":
                functionSpatial = spatial_Aggregation_Function
            else:
                functionSpatial = "NONE"

            cadena = f'''
                        {{  \"id\": \"{row['kpi_id']}\",
                            \"name\": \"{row['kpi_name']}\",
                            {is_rate}
                            {showAsPercentage}
                            \"temporalAggSpec\": {{
                                \"func\": \"{functionTime}\"
                                {percentil_time}                                   
                            }},
                            \"spatialAggSpec\": {{
                            \"func\": \"{functionSpatial}\"
                                {percentil_spatial if spatial_Aggregation_Function!="NONE" and spatial_Aggregation_Function == "PERCENTILE" else ""}                                   
                            }}
                        }}'''
        
            query_druid = get_cadena_query_adapter(dataTableName, formatted_init_datetime, formatted_end_datetime, data_aggregation_time_granularity, timeCriteria, cadena, sliceBy, neGroupingFilter, manageObjectId, dimFilter, elementType, vendor, equipmentId, locationId, hours_query, dayofweek_query)
            
            try:    
                response = requests.request("POST", url_druid_adapter, headers=headers, data=query_druid)
                #print("Respuesta:",response)
                if response.status_code == 200:
                    sql = response.json()['data']['getSqlQueryFromCriteria']
                    sql_queries.append(sql + '&&' + query_select) 
                else:
                    query_update = f'''
                        UPDATE {tb_control_load}  
                        SET status ='ERROR', log ='Error al ejecutar druid adapter'
                        WHERE threshold_id ='{row['node_id']}' AND status='LOAD'
                    ''' 
                    execute_Query(query_update, "", "UPDATE", False)   
            except json.JSONDecodeError as e:
                print("Error al ejecutar druid adapter:", e)
                raise e
        
        sql_queries_json = json.dumps(sql_queries)
    else:
        sql_queries_json=json.dumps("")
    return sql_queries_json 


def execute_query_druid(sql):
    results = pd.DataFrame()
    #url_druid = Variable.get("url-druidDb_dev") 
    url_druid = Variable.get("url-druidDb")       
    jdbc_url = "jdbc:avatica:remote:url=" + url_druid + "/avatica/"
    print("URL:", jdbc_url)
    
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
        #print("Conexión cerrada.")
    
    return results

  
def get_datos_sql_druid(sql,e):
    try:
        print("SQL:", sql)
        kpi_names = re.search(r'"metricId" IN\s*\((.*?)\)', sql, re.DOTALL)
        fechas = re.findall(r'TIME_PARSE\(\'(.*?)\'\)', sql)
        managed_object = re.search(r'"managedObjectId" = \'(.*?)\'', sql)
        granularityInterval = re.search(r'"granularityInterval" = \'(.*?)\'', sql)
        networkGroup = re.findall(r'\[(.*?)\]', sql)

        if networkGroup:        
            var_networkGroup = " AND networkGroups='[" + networkGroup[0] + "]'" 
        else:
            var_networkGroup = ""

        equipments = re.findall(r'dn IN \((.*?)\)', sql)

        if equipments:
            cadena_dn = " AND equipments='[" + equipments[0] + "]'"          
        else:
            cadena_dn = ""

        if managed_object:
            valor_managed_object = managed_object.group(1)
        if granularityInterval:
            valor_granularityInterval = granularityInterval.group(1)
        
        if kpi_names:
            valores_name_in = kpi_names.group(1)
            valores_name_in = [valor.strip().strip("'") for valor in valores_name_in.split(',')]
            print("Lista de valores:", valores_name_in)
        else:
            print("No se encontraron valores en metricId IN")
            valores_name_in = []
        
        for kpi in valores_name_in:
            query_update = f'''
                UPDATE {tb_control_load}
                SET status ='ERROR', log ='"Error al intentar ejecutar consultas en druid:"{e} ' 
                WHERE kpi_id='{kpi}' AND managedobject_id='{valor_managed_object}' AND granularityinterval='{valor_granularityInterval}'
                    {var_networkGroup} {cadena_dn} and start_date='{fechas[0]}' and end_date='{fechas[1]}' AND status='LOAD'
            ''' 
            execute_Query(query_update, "", "UPDATE", False)
    except Exception as e:
        print(repr(e))
        raise e


def execute_druid_query(sql_queries_str):    
    try:
        sql_queries = json.loads(sql_queries_str)
    except json.JSONDecodeError as e:
        print("Error al decodificar JSON:", e)
        raise e
        return
    
    #print("sql_queries", sql_queries)
    results = [] 
    for sql in sql_queries:
        lista_sql = sql.split("&&")
        if len (lista_sql) > 1:
            #print("lista_sql:",lista_sql)
            try:
                print("SELECT #1:", lista_sql[0])     
                print("SELECT #2:", lista_sql[1])     
                response_sql = execute_query_druid(lista_sql[0])                
                response_select = execute_query_druid(lista_sql[1])               
                lista_de_sql = [response_sql,response_select]
                if not response_sql.empty:
                    results.append(lista_de_sql) 
                            
            except Exception as e:
                get_datos_sql_druid(lista_sql[0],e)               
                print(repr(e))
                raise e 
    #print ("results druid:", results)
    return results


def delivery_report(err, msg):   
    if err is not None:
        print(f"Error de entrega del mensaje: {err}")
    else:
        print(f"Mensaje entregado a {msg.topic()} [{msg.partition()}]")


def create_nested_json(response_list, reportInterval, granularityInterval):       
    json_list = []
    if len(response_list) > 0:               
        for response in response_list:
            df = response[0]            
            df_select = response[1]           
                                
            for index, row in df_select.iterrows(): 
                if row['min_record'] != '':
                    starDate = row['min_record']
                    endDate = row['max_record']
                    aggregate = row['aggregation']
                    #aggregationTime=row['aggregationTime']
                    spatialaggregation = row['spatialAggregation']
                    spatial_location = row['spatiallocation']
                    spatial_MO = row['spatialMO']
                    networkGroup = row['networkGroup'].replace("$$", ',') 
                    node_id = row['node_id']    
                    isRate = row['isRate']
                    
                    if starDate:
                        beginTime = datetime.strptime(starDate, "%Y-%m-%d %H:%M:%S")  
                    if endDate:
                        endTime= datetime.strptime(endDate, "%Y-%m-%d %H:%M:%S")    

            for index, row in df.iterrows(): 
                query = "SELECT load_status FROM " + tb_threshold + " WHERE node_id='" + node_id + "' AND kpi_id='" + row['metricId'] + "'"
                result = executeQuery(query)
                load_status = ""
                if not result.empty:
                    load_status = result['load_status'].iloc[0]  
                else:
                    print("Error: El DataFrame está vacío.")
                    print("Query:", query)

                json_data = createJson(beginTime, endTime, row, aggregate, spatialaggregation, spatial_location, spatial_MO, networkGroup, reportInterval, granularityInterval, load_status, node_id, isRate)
                json_list.append(json_data) 

    return json_list

def valueOfSerie(series, column_name):
    value = ""
    for name, s in series.items():  
        print("name:", name)
        if name.startswith(column_name):        
            value = s
            break
    return value 


def createJson(beginTime, endTime, row, aggregate, spatialaggregation, spatial_location, spatial_MO, networkGroup, reportInterval, granularityInterval, load_status,node_id, isRate):
    var_cellname = ""
    spatial = ""
    var_vendorID = ""

    if spatialaggregation == "BY_LOCATION":
        var_cellname = spatial_location        
        spatial = valueOfSerie(row, "location_")         
    elif spatialaggregation == "BY_TOPOLOGY":
        var_cellname = spatial_MO        
        spatial = valueOfSerie(row, "topology_")
    elif spatialaggregation == "BY_GROUP":
        spatial = row['neGroup']
    else: 
        var_vendorID = row['vendorId']

    var_dn = ""
    var_elementType = ""
    var_equipmentId = ""
    var_locationId = ""
    if "spatial" in aggregate: 
        var_dn = f"{row['metricId']}-{row['metric_name']}-{spatialaggregation}-{spatial}"
        var_additionalDn = spatialaggregation       
    else:
        if row['dn']:        
            var_dn =  row['dn'].replace("$$", ",").replace('"', '')
        
        if aggregate=="temporal":
            var_additionalDn = row['additionalDn']
            var_elementType = row['elementType']
            var_equipmentId = row['equipmentId']
            var_locationId = row['locationId']

    beginTime_epoch = int(beginTime.timestamp())
    if aggregate == "spatial":
        endTime_epoch = beginTime_epoch + int(reportInterval) - 1
    else:
        endTime_epoch = int(endTime.timestamp())
               
    #var_networkGroups = networkGroup.replace('"','')
    var_networkGroups = json.loads(networkGroup)
    
    jsonValue = {
        "metricId": row['metricId'],
        "code": row['metricId'],
        #"name": row['metric_name'],
        "networkType": row['networkType'],
        "category": row['category'],
        "subcategory": row['subcategory'],
        "familyId": row['familyId'],
        #"familyName": row['familyName'],
        #"metricType": row['metricType'],
        "dn": var_dn,
        "additionalDn": var_additionalDn,
        "technology": row['technology'],
        #"cellName": var_cellname,
        #"elementType": var_elementType,
        "beginTime": beginTime_epoch*1000,
        "endTime": endTime_epoch*1000,
        "timestamp": beginTime_epoch*1000,
        "reportInterval": str(reportInterval),
        "granularityInterval": granularityInterval,
        "vendorId": var_vendorID,
        "equipmentId": var_equipmentId,
        "locationId": var_locationId,
        "sourceSystem": "",
        #"domain": row['domain'],
        #"managerIp": "",
        #"managerFilename": "",
        "value": float(row['metric0']),
        "numerator": None,
        "denominator": None,
        "enrichment": {},
        "equipmentTypeId": row['equipmentTypeId'],
        "equipmentType": row['equipment_type_name'],
        "managedObjectId": row['managedObjectId'],
        "managedObject": row['managed_object_name'],
        "networkGroups": var_networkGroups,
        "processTime": int(time.time())*1000,
        "isRate": isRate,
        "aggregation": aggregate,
        "load_status": load_status,
        "thresholdId": node_id,
        "aggregationLevel":spatialaggregation
        }    
    
    jsonValue_ = json.dumps(jsonValue, ensure_ascii=False ) 
    return jsonValue_


def clean_none_values(data):
    if isinstance(data, dict):
        return {
            k: (
                None if v == "None" else
                True if v == "True" else   
                False if v == "False" else
                ast.literal_eval(v) if k == "networkGroups" and isinstance(v, str) else  
                json.loads(v) if k == "enrichment" and v == "{}" else  
                v
            )
            for k, v in data.items()
        }
    elif isinstance(data, list):
        return [clean_none_values(item) for item in data]
    else:
        return None if data == "None" else data


def produce_message_to_topic_kafka(response_list, reportInterval, granularityInterval, algorithm):  
    conf, topic = get_kafka_config()
    p = Producer(**conf)
        
    if algorithm != 'AVERAGE_HISTORICAL_VALUE':     
        if len(response_list) > 0:
            for response in response_list:
                df = response[0]            
                df_select = response[1]         

                for index, row in df_select.iterrows(): 
                    if row['min_record'] != '':
                        print("Registro Num:", index)
                        starDate = row['min_record']
                        print("starDate", starDate)
                        endDate = row['max_record']
                        print("endDate", endDate)
                        aggregate = row['aggregation']
                        #aggregationTime=row['aggregationTime']
                        spatialaggregation = row['spatialAggregation']
                        spatial_location = row['spatiallocation']
                        spatial_MO = row['spatialMO']
                        networkGroup = row['networkGroup'].replace("$$", ',') 
                        node_id = row['node_id']    
                        isRate = row['isRate']
                        
                        if starDate:
                            beginTime = datetime.strptime(starDate, "%Y-%m-%d %H:%M:%S")  
                        if endDate:
                            endTime= datetime.strptime(endDate, "%Y-%m-%d %H:%M:%S")    

                for index, row in df.iterrows():                    
                    where=" WHERE threshold_id='" + node_id + "' AND kpi_id='" + row['metricId'] + "' AND status='LOAD'"
                    query = "SELECT load_status FROM "+ tb_threshold + " WHERE node_id='" + node_id + "' AND kpi_id='" + row['metricId'] + "'"
                    result = executeQuery(query)
                    
                    load_status = result['load_status'][0]

                    json_data = createJson(beginTime,endTime,row,aggregate,spatialaggregation,spatial_location,spatial_MO,networkGroup,reportInterval,granularityInterval, load_status,node_id,isRate)
                    print("json_data:", json_data)

                    data_dict = json.loads(json_data)
                    data_dict_clean = clean_none_values (data_dict)
                    var_dn = data_dict_clean.get("dn", "default_dn")  
                    var_threshold_id = data_dict_clean.get("thresholdId", "default_threshold")  

                    data_str = json.dumps(data_dict_clean, ensure_ascii=False ) 
                    data_bytes = data_str.encode('utf-8') 

                    key_str = f"{var_dn}+{var_threshold_id}"  
                    key_bytes = key_str.encode('utf-8')  

                    send_to_kafka(p, topic, key_bytes, data_bytes, where)
    else:
        parsed_data = [json.loads(item) for sublist in response_list for item in sublist if isinstance(item, str)]
        df = pd.DataFrame(parsed_data)
            
        group_keys = [col for col in df.columns if col not in ["beginTime", "endTime", "processTime", "timestamp", "value"]]
        for col in group_keys:
            df[col] = df[col].astype(str)
        
        grouped = df.groupby(group_keys, as_index=False)
        json_grouped = []
        for key, df_group in grouped:
            group_sorted = df_group.sort_values(by="beginTime", ascending=False) 

            json_entry = {
                "current": clean_none_values(group_sorted.iloc[0].to_dict()),  
                "values": {row["beginTime"]: clean_none_values(row.to_dict()) for _, row in group_sorted.iloc[1:].iterrows()},
                "thresholdId": group_sorted.iloc[0]["thresholdId"]
            }

            json_grouped.append(json_entry) 

        for json_data_ in json_grouped:
            data_str = json.dumps(json_data_, ensure_ascii=False)
            data_bytes = data_str.encode('utf-8') 
            print("json_data:", data_str)

            var_dn = json_data_["current"].get("dn", "default_dn")  
            var_threshold_id = json_data_.get("thresholdId", "default_threshold")
            var_metric_id = json_data_["current"].get("metricId")  
    
            where = " WHERE threshold_id='" + var_metric_id + "' AND kpi_id='" + var_threshold_id + "' AND status='LOAD'"
                    
            key_str = f"{var_dn}+{var_threshold_id}"  
            key_bytes = key_str.encode('utf-8')  
            send_to_kafka(p, topic, key_bytes, data_bytes, where)


def get_kafka_config():
    #topic = Variable.get("topic")
    topic = "fastoss-threshold-prep-data"
    
    pem_content = Variable.get("pem_content")
    #pem_content = Variable.get("pem_content_dev")

    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(pem_content.encode())
    
    ssl_location = temp_file.name

    connection_id = 'kafka_default'  #Prod
    #connection_id = 'kafka_dev' #Dev
    connection = Connection.get_connection_from_secrets(connection_id)

    conf = {
        'bootstrap.servers': connection.extra_dejson.get('bootstrap.servers'),
        'security.protocol': connection.extra_dejson.get('security.protocol'),
        'sasl.mechanism': connection.extra_dejson.get('sasl.mechanism'),
        'sasl.username': connection.extra_dejson.get('sasl.username'),
        'sasl.password': connection.extra_dejson.get('sasl.password'),
        'ssl.ca.location': ssl_location
    }
    
    ssl_context = ssl.create_default_context()
    ssl_context.load_verify_locations(conf['ssl.ca.location'])
    return conf, topic


def send_to_kafka(producer, topic, key_bytes, data_bytes, where):
    try:       
        producer.produce(topic, key=key_bytes, value=data_bytes, callback=delivery_report)                    
        producer.flush()   
        status = "SUCESSFULL"
        log = "Datos cargados con exito en Kafka al topico:" + topic        
        query_update = f'''UPDATE {tb_control_load} SET status ='{status}', log ='{log}', pending = 0
                        {where}'''
        print("status:", status)
    except Exception as e:
        print("Error al enviar mensaje: {}".format(e))
        status = "ERROR"
        log = "Error de entrega en Kafka:" + e
        query_update = f'''UPDATE {tb_control_load} SET status ='{status}', log ='{log}'
                        {where}''' 
        raise e
    finally:
        execute_Query(query_update,"","UPDATE",False)  
        producer.flush()


def insert_load_data(kpi_id,manageObject_id,var_data_time_granularity,networkgroups,equipments,v_delay,v_compleness,records_load_druid,status,formatted_init_datetime,formatted_end_datetime,aggregationTime,node_id,log,load_aggregationTime, load_status):
    data = f''' INSERT INTO {tb_control_load} (kpi_id, managedobject_id, granularityinterval, 
                networkgroups, equipments, delay, completness, records, status, start_date, end_date, 
                aggregation_time, load_datatime,pending,log,threshold_id) 
            VALUES ('{kpi_id}', '{manageObject_id}', '{var_data_time_granularity}', '{networkgroups}', '{equipments}', {v_delay}, {v_compleness}, {records_load_druid}, '{status}', '{formatted_init_datetime}', '{formatted_end_datetime}','{aggregationTime}', '{formatted_end_datetime}',{records_load_druid},'{log}','{node_id}')
            '''
                    
    datos_a_insertar = {
        'kpi_id': kpi_id,
        'managedobject_id': manageObject_id,
        'granularityinterval': var_data_time_granularity,
        'networkgroups': networkgroups,
        'equipments': equipments,
        'delay': v_delay,
        'completness': v_compleness,
        'records': records_load_druid,
        'status': status,
        'start_date':formatted_init_datetime ,
        'end_date':formatted_end_datetime,
        'aggregation_time': aggregationTime,
        'load_datatime': formatted_end_datetime,
        'pending':records_load_druid,
        'log':log,
        'threshold_id':node_id
        }
    
    execute_Query (data,datos_a_insertar, "INSERT", True)
    
    query_update =f'''UPDATE {tb_threshold} 
                      SET load_status = '{load_status}' 
                      WHERE "node_id" = '{node_id}' AND "kpi_id" = '{kpi_id}'{load_aggregationTime}
                '''
    #print("update",query_update) 
    execute_Query (query_update,"", "UPDATE", False)


def get_data_load (node_id, dataTableName, kpi_id, manageObject_id, data_aggregation_time_granularity, formatted_init_datetime, formatted_end_datetime, networkgroups, equipments, aggregationTime, minutos):
    var_data_time_granularity = get_granularity(data_aggregation_time_granularity)
    
    if aggregationTime != "":
        load_aggregationTime = " AND \"dataAggregationTime\"='" + aggregationTime + "'"
    else:
        load_aggregationTime = " AND \"dataAggregationTimeGranularity\"='"
        if(minutos == 5):
            load_aggregationTime += "PT5M'"   
            aggregationTime = "PT5M"        
        elif(minutos == 15):
            load_aggregationTime += "PT15M'"
            aggregationTime = "PT15M" 
        elif(minutos == 30):
            load_aggregationTime += "PT30M'"
            aggregationTime = "PT30M" 
        elif(minutos == 60):
            load_aggregationTime += "PT1H"
            aggregationTime = "PT1H" 
        
    ng_filter = ""
    var_networkgroups = ""
    if networkgroups != "[]":
        ng_filter = " AND ARRAY_OVERLAP(\"networkGroups\", ARRAY " + networkgroups.replace('"', "'") + ")"
        var_networkgroups = " AND \"networkgroups\"='" + networkgroups + "'"

    dn_filter = ""
    var_equipments = ""
    if equipments != "[]":
        dn_filter = " AND \"dn\"='" + equipments + "'"
        var_equipments = " AND \"equipments\"='" + equipments + "'"

    query_metrics = f'''SELECT count(*) records 
                        FROM \"{dataTableName}\"
                        WHERE \"metricId\" ='{kpi_id}' AND \"managedObjectId\" = '{manageObject_id}' AND \"granularityInterval\" = '{var_data_time_granularity}'
                            AND __time BETWEEN TIME_PARSE('{formatted_init_datetime}') AND TIME_PARSE('{formatted_end_datetime}') {ng_filter} {dn_filter} 
                    ''' 

    print("query_metrics:", query_metrics)

    query_postgress = f'''SELECT percentile_cont(0.5) WITHIN GROUP (ORDER BY records) AS records 
                        FROM {tb_control_load}
                        WHERE kpi_id ='{kpi_id}' AND managedObject_id = '{manageObject_id}' AND granularityinterval = '{var_data_time_granularity}'
                            AND aggregation_time='{aggregationTime}' {var_networkgroups} {var_equipments} 
                        '''
    records_postgress = executeQuery(query_postgress)

    #query_update=f'''UPDATE PANDA.threshold SET load_status ='' WHERE "node_id"='{node_id}' AND "kpi_id" ='{kpi_id}' {load_aggregationTime}''' 
    #execute_Query(query_update,"","UPDATE",False)

    if minutos == 5:
        delay = Variable.get("delay_5M(sec)")  
    elif minutos == 15:
         delay = Variable.get("delay_15M(sec)")  
    elif minutos == 30:
         delay = Variable.get("delay_30M(sec)")  
    elif minutos == 60:
         delay = Variable.get("delay_60M(sec)")
    else:  
        delay = Variable.get("delay(sec)")   
    
    print("delay:", delay)

    compleness = Variable.get("completitud(%)")
    if delay:
        v_delay = int(delay)
    if compleness:
        v_compleness = int (Variable.get("completitud(%)"))
        v_compleness_p = v_compleness/100 
    
    status = "INIT"        
    start_time = time.time()  # Obtener el tiempo actual             
    cont = 10
    while True:
        print("metricId:", kpi_id)
        records_metrics = execute_query_druid(query_metrics)          
        if len(records_metrics) > 0:       
            records = records_metrics.loc[0, 'records']
            records_load_druid= int(records) #dato cargado de metricas
            
            if records_postgress['records'][0] is None:
                records_metrics_postgress = 0
            else: 
                records_metrics_postgress = records_postgress['records'][0]
            
            print("records_metrics_postgres:", records_metrics_postgress)
            
            datos_aceptables = round(records_metrics_postgress*v_compleness_p)   
            print("records_load_druid:", records_load_druid)
            print("datos_aceptables:", datos_aceptables)             
            
            if (records_load_druid >= datos_aceptables) and (records_load_druid > 0):  
                status="LOAD"                 
                insert_load_data(kpi_id,manageObject_id,var_data_time_granularity,networkgroups,equipments,v_delay,v_compleness,records_load_druid,status,formatted_init_datetime,formatted_end_datetime,aggregationTime,node_id,'Iniciando proceso de carga',load_aggregationTime, 'Completed')
                break 
            
        elapsed_time = time.time() - start_time
        print("elapsed_time:", elapsed_time)
        
        if elapsed_time >= v_delay:
            break        
        
        if minutos > 5:
            cont=cont*2 
        else:
            cont=120 
        
        print("tiempo pausa:", cont)     
        time.sleep(cont) 

    if status == "INIT":
        status = "PARTIAL"
        insert_load_data(kpi_id,manageObject_id,var_data_time_granularity,networkgroups,equipments,v_delay,v_compleness,records_load_druid,status,formatted_init_datetime,formatted_end_datetime,aggregationTime,node_id,'No se encontraron los registros suficientes para hacer la carga',load_aggregationTime, 'Partial')
       
     
def prepared_data_load(param_df_threshold, var_datetime, aggregationTime, minutos):
    row = pd.read_json(StringIO(param_df_threshold), orient='records')
    
    manageObject_id = row.iloc[0]['managedObject_id'] 
    data_aggregation_time_granularity = row.iloc[0]['dataAggregationTimeGranularity'] if row.iloc[0]['dataAggregationTimeGranularity']!="null" else row.iloc[0]['kpi_defaultTimeGranularity']
    dataTableName = row.iloc[0]['dataTableName']
    networkgroups = row.iloc[0]['id_ngInclusion']
    equipments = row.iloc[0]['equipmentInclusion']
    kpi_id = row.iloc[0]['kpi_id']
    node_id = row.iloc[0]['node_id']
    data_aggregation_time = row.iloc[0]['dataAggregationTime'].upper()
    spatial_Aggregation = row.iloc[0]['spatialAggregation'].upper() 
      
    if  data_aggregation_time != "NONE":
        formatted_init_datetime, formatted_end_datetime = get_time_variables(var_datetime, aggregationTime,"")
        
    if spatial_Aggregation != "NONE" and data_aggregation_time=="NONE":                    
        formatted_init_datetime, formatted_end_datetime=get_spatial_time_variables(var_datetime,minutos)   
        
        
    get_data_load(node_id, dataTableName, kpi_id, manageObject_id, data_aggregation_time_granularity, formatted_init_datetime, formatted_end_datetime, networkgroups,equipments, aggregationTime, minutos)   