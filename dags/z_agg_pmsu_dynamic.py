# """                                                                                                                                                                       
#    DAG: z_agg_pmsu_dynamic                                                                                                                                                   
                                                                                                                                                                             
#    Resumo:                                                                                                                                                                   
#    DAG responsável por realizar agregação de contadores PMSU em múltiplos intervalos de tempo                                                                                
#    e tipos de agregação, lendo do Druid, processando dados e enviando resultados ao Kafka.                                                                                   
#    Ao final, atualiza variáveis do Airflow com a data da última informação processada.                                                                                       
                                                                                                                                                                             
#    Autor: Squad Airflow - leandro                                                                                                                                            
#    Última atualização: 2024-05-07                                                                                                                                            
# """                         

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.exceptions import AirflowSkipException
# from airflow.operators.dummy import DummyOperator
# from airflow.models import Variable
# from airflow.models import Connection
# from airflow.utils.dates import datetime, timedelta
# from hooks.kafka_connector import KafkaConnector
# from hooks.druid_conector import DruidConnector
# from utils.date_utils import DateUtils
# import json
# connection_id = 'pmsu_connection'
# base_connection = Connection.get_connection_from_secrets(connection_id)
# base_json = base_connection.extra_dejson

# DATE_TIME_COLUMN_DRUID = '__time'

# # Configuração base das DAGs
# DAG_DEFAULT_ARGS = {
#     'owner': 'Leandro',
#     'depends_on_past': False,
#     'retries': 0,
#     'retry_delay': timedelta(minutes=5),
# }

# # Configuração das variáveis
# VAR_DATETIME_MAX_PREFIX = 'datetime_max_agg_counters_pmsu'
# VAR_KAFKA_TOPIC_PREFIX = 'agg_counters_topic_pmsu'

# def create_base_query(aggregation, agg_param_druid, tag_counter, add_minutes_to_endtime, array_agg_size, base_report_interval, last_max_date
#                       ,internal_aggregation, internal_agg_param_druid, internal_add_minutes_to_endtime
#                       ):
#     query =  f"""
#             SELECT
#                 TIME_FLOOR (CAST("__time" AS TIMESTAMP), '{agg_param_druid}') AS __time,
#                 TIMESTAMP_TO_MILLIS (TIME_FLOOR (CAST("__time" AS TIMESTAMP), '{agg_param_druid}')) AS beginTime,
#                 "additionalDn",
#                 "dn",
#                 "vendorName",
#                 CONCAT ("name", '{tag_counter}', '_{aggregation}') AS name,
#                 CONCAT ("code", '{tag_counter}', '_{aggregation}') AS code,
#                 "familyId",
#                 "elementType",
#                 {add_minutes_to_endtime} * 60 AS "reportInterval",
#                 "sourceSystem",
#                 "managerIp",
#                 TIMESTAMP_TO_MILLIS (TIME_FLOOR (CAST("__time" AS TIMESTAMP), '{agg_param_druid}') + INTERVAL '{add_minutes_to_endtime}' MINUTE) AS "endTime",
#                 "isReliable",
#                 CONCAT('PT',CAST(({add_minutes_to_endtime} * 60) AS VARCHAR(255)), 'S') AS "granularityInterval",
#                 MV_TO_STRING(ARRAY_AGG(DISTINCT(managerFilename), {array_agg_size}),',') AS "managerFilename",
#                 {aggregation}("value") AS "value"
#             FROM
#                 "fastoss-pm-counters"
#             WHERE
#                 __time > '{last_max_date}'
#                 AND __time < CURRENT_TIMESTAMP - INTERVAL '{add_minutes_to_endtime}' MINUTE
#                 AND sourceSystem = 'SLC'
#                 AND reportInterval = '{base_report_interval}'
#             GROUP BY
#             1,2,3,4,5,6,7,8,9,10,11,12,13,14
#         """
#     if internal_aggregation: 
#         query = f"""
#         SELECT
#             TIME_FLOOR(CAST("__time" AS TIMESTAMP), '{internal_agg_param_druid}') AS __time,
#             TIMESTAMP_TO_MILLIS (TIME_FLOOR (CAST("__time" AS TIMESTAMP), '{internal_agg_param_druid}')) AS beginTime,
#             additionalDn,
#             dn,
#             vendorName,
#             CONCAT(name,'_{internal_aggregation}') AS name,
#             CONCAT(code, '_{internal_aggregation}') AS code,
#             familyId,
#             elementType,
#             {internal_add_minutes_to_endtime} * 60 AS "reportInterval",
#             sourceSystem,
#             managerIp,
#             TIMESTAMP_TO_MILLIS(MILLIS_TO_TIMESTAMP(endTime) + INTERVAL '{internal_add_minutes_to_endtime}' MINUTE) endTime,
#             isReliable,
#             CONCAT('PT',CAST(({internal_add_minutes_to_endtime} * 60) AS VARCHAR(255)), 'S') AS "granularityInterval",
#             MV_TO_STRING(ARRAY_AGG(managerFilename),',') AS managerFilename,
#             {internal_aggregation}("value") AS "value" 
#         FROM
#         (
#             {query}
#         )
#         group by 1,2,3,4,5,6,7,8,9,10,11,12,13,14
#         """  
#     return query

# def create_multilevel_aggregations_query(last_max_date):
#     return f"""
# SELECT
#     TIME_FLOOR(agg_5m.interval_5m, 'PT15M') AS "__time",
#     agg_5m.device,
#     MAX(agg_5m.max_5m) AS "max",
#     MAX(agg_5m.maxAvg_5m) AS "maxAvg",
#     SUM(agg_5m.sum_5m) AS "sum",
#     SUM(agg_5m.count_5m) AS "count",
#     SUM(agg_5m.sum_5m) / SUM(agg_5m.count_5m) AS "avg",
#     '15_minute' AS "aggregation_interval"
# FROM (
#     SELECT
#         TIME_FLOOR(raw_1m.interval_1m, 'PT5M') AS interval_5m,
#         raw_1m.device,
#         MAX(raw_1m.max_1m) AS max_5m,
#         MAX(raw_1m.avg_1m) AS maxAvg_5m,
#         SUM(raw_1m.sum_1m) AS sum_5m,
#         SUM(raw_1m.count_1m) AS count_5m,
#         SUM(raw_1m.sum_1m) / SUM(raw_1m.count_1m) AS avg_5m
#     FROM (
#         SELECT
#             TIME_FLOOR("__time", 'PT1M') AS interval_1m,
#             "additionalDn" AS device,
#             MAX("value") AS max_1m,
#             SUM("value") AS sum_1m,
#             COUNT("value") AS count_1m,
#             SUM("value") / COUNT("value") AS avg_1m
#         FROM "fastoss-pm-counters"
#         WHERE
#             "__time" > TIMESTAMP '{last_max_date}'
#             AND "__time" < CURRENT_TIMESTAMP - INTERVAL '1' MINUTE
#             AND "sourceSystem" = 'SLC'
#         GROUP BY
#             TIME_FLOOR("__time", 'PT1M'),
#             "additionalDn"
#     ) raw_1m
#     GROUP BY
#         TIME_FLOOR(raw_1m.interval_1m, 'PT5M'),
#         raw_1m.device
# ) agg_5m
# GROUP BY
#     TIME_FLOOR(agg_5m.interval_5m, 'PT15M'),
#     agg_5m.device
# """


# def set_airflow_variable(variable_name, value):
#     Variable.set(variable_name, value)

# def get_airflow_variable(variable_name, default_value=None):
#     return Variable.get(variable_name, default_var=default_value)

# # def create_dag_function(aggregation_type, aggregation_name, config):
# #     def main():
# #         string_past_24_hours = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
# #         var_datetime_max = f"{VAR_DATETIME_MAX_PREFIX}_{aggregation_type}_{aggregation_name}"
        
# #         print(f'🕓 Start date if the variable {var_datetime_max} is not found: {string_past_24_hours}')
# #         start_date = get_airflow_variable(var_datetime_max, string_past_24_hours)
        
# #         if start_date != string_past_24_hours:
# #             print(f'✅🕓 Variable found, value filtered after: {start_date}')
# #         else:
# #             print(f'✅⏳ Variable not found, value filtered after: {start_date}')
        
# #         druid = DruidConnector(druid_url_variable="druid_url")
# #         print('🟣 Created Druid connection.')
        
# #         query = create_base_query(
# #             aggregation=aggregation_name,
# #             agg_param_druid=config['agg_param_druid'],
# #             tag_counter=config['tag_counter'],
# #             add_minutes_to_endtime=config['add_minutes_to_endtime'],
# #             array_agg_size=config['array_agg_size'],
# #             base_report_interval=config['base_report_interval'],
# #             last_max_date=start_date
# #             ,internal_aggregation=config.get('internal_aggregation', None)
# #             ,internal_agg_param_druid=config.get('internal_agg_param_druid', None)
# #             ,internal_add_minutes_to_endtime=config.get('internal_add_minutes_to_endtime', None)
# #         )
# #         print(f'🟣 Query: {query}')
        
# #         result = druid.send_query(query)
# #         total_registers = len(result)
# #         print(f'🟣 Total of registers returned: {total_registers}.')
        
# #         if len(result) > 0:
# #             utils = DateUtils(data=result)
# #             print('🕓 Identifying the largest among the collected data.')
# #             max_date_from_column = utils.get_latest_date_parallel(
# #                 date_column=DATE_TIME_COLUMN_DRUID,
# #                 date_format="%Y-%m-%dT%H:%M:%S.%fZ"
# #             )
# #             print(f'🕓 Longest date identified: {max_date_from_column}')
            
# #             print('🔵 Start connection with Kafka.')
# #             kafka = KafkaConnector(
# #                 topic_var_name=VAR_KAFKA_TOPIC_PREFIX,
# #                 kafka_url_var_name="prod_kafka_url",
# #                 kafka_port_var_name="prod_kafka_port",
# #                 kafka_variable_pem_content="pem_content",
# #                 kafka_connection_id="kafka_default"
# #             )
# #             producer = kafka.create_kafka_connection()
# #             print('🔵 Created Kafka producer.')
            
# #             print('🔵 Sending data to Kafka...')
# #             kafka.send_mult_messages_to_kafka(
# #                 menssages=result,
# #                 producer=producer
# #             )
# #             print(f'✅ Messages sent on Kafka.')
            
# #             set_airflow_variable(var_datetime_max, max_date_from_column)
# #             print(f"🔼 Variable {var_datetime_max} updated with new datetime: {max_date_from_column}")
# #         else:
# #             print('✅ No records returned. Process finished!')
# #             raise AirflowSkipException('✅ No records returned. Process finished!')
    
# #     return main

# def create_dag_function(aggregation_type, aggregation_name, config):
#     def main():
#         string_past_24_hours = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
#         var_datetime_max = f"{VAR_DATETIME_MAX_PREFIX}_{aggregation_type}_{aggregation_name}"
        
#         start_date = get_airflow_variable(var_datetime_max, string_past_24_hours)

#         druid = DruidConnector(druid_url_variable="druid_url")

#         # Chama a nova query só para a agregação multinível (seu caso específico)
#         if aggregation_name == "pmsu_multilevel":
#             query = create_multilevel_aggregations_query(start_date)
#         else:
#             query = create_base_query(
#                 aggregation=aggregation_name,
#                 agg_param_druid=config['agg_param_druid'],
#                 tag_counter=config['tag_counter'],
#                 add_minutes_to_endtime=config['add_minutes_to_endtime'],
#                 array_agg_size=config['array_agg_size'],
#                 base_report_interval=config['base_report_interval'],
#                 last_max_date=start_date,
#                 internal_aggregation=config.get('internal_aggregation', None),
#                 internal_agg_param_druid=config.get('internal_agg_param_druid', None),
#                 internal_add_minutes_to_endtime=config.get('internal_add_minutes_to_endtime', None)
#             )

#         result = druid.send_query(query)

#         if result:
#             kafka = KafkaConnector(
#                 topic_var_name="agg_pm_counter_aggregates_topic",
#                 kafka_url_var_name="prod_kafka_url",
#                 kafka_port_var_name="prod_kafka_port",
#                 kafka_variable_pem_content="pem_content",
#                 kafka_connection_id="kafka_default"
#             )
#             producer = kafka.create_kafka_connection()

#             kafka.send_mult_messages_to_kafka(
#                 menssages=result,
#                 producer=producer
#             )

#             utils = DateUtils(data=result)
#             max_date_from_column = utils.get_latest_date_parallel(
#                 date_column=DATE_TIME_COLUMN_DRUID,
#                 date_format="%Y-%m-%dT%H:%M:%S.%fZ"
#             )
#             set_airflow_variable(var_datetime_max, max_date_from_column)
#         else:
#             raise AirflowSkipException('✅ No records returned. Process finished!')

#     return main


# # Criando DAGs dinamicamente
# for time_interval, aggregations in base_json.items():
#     for agg_name, config in aggregations.items():
#         dag_id = f'Agg_Counters_PMSU_{time_interval}_{agg_name}'
        
#         dag = DAG(
#             dag_id,
#             default_args=DAG_DEFAULT_ARGS,
#             description=f'DAG para agregação de contadores de PMSU - {time_interval} - {agg_name}',
#             schedule_interval=config['agendamento'],
#             start_date=datetime(2024, 12, 4),
#             catchup=False,
#             max_active_runs=1,
#             tags=['counters', 'druid', 'pmsu', time_interval, agg_name],
#         )
        
#         start = DummyOperator(task_id='start', dag=dag)
        
#         process_counters = PythonOperator(
#             task_id='process_counters',
#             python_callable=create_dag_function(time_interval, agg_name, config),
#             provide_context=True,
#             execution_timeout=timedelta(minutes=20),
#             dag=dag
#         )
        
#         end = DummyOperator(task_id='end', dag=dag)
        
#         # Definindo as dependências das tarefas
#         start >> process_counters >> end
        
#         # Registrando a DAG no namespace global
#         globals()[dag_id] = dag 
"""
DAG: z_agg_pmsu_dynamic

Resumo:
DAG responsável por realizar agregação de contadores PMSU em múltiplos intervalos de tempo
e tipos de agregação, lendo do Druid, processando dados e enviando resultados ao Kafka.
Ao final, atualiza variáveis do Airflow com a data da última informação processada.

Autor: Squad Airflow - Leandro
Última atualização: 2025-05-13
"""

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable, Connection
from airflow.utils.dates import datetime, timedelta
from hooks.kafka_connector import KafkaConnector
from hooks.druid_conector import DruidConnector
from utils.date_utils import DateUtils
import json

connection_id = 'pmsu_connection'
base_connection = Connection.get_connection_from_secrets(connection_id)
base_json = base_connection.extra_dejson

DATE_TIME_COLUMN_DRUID = '__time'

# Configuração base das DAGs
DAG_DEFAULT_ARGS = {
    'owner': 'Leandro',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}

# Prefixos de variáveis no Airflow
VAR_DATETIME_MAX_PREFIX = 'datetime_max_agg_counters_pmsu'
VAR_KAFKA_TOPIC_PREFIX = 'agg_counters_topic_pmsu'

def set_airflow_variable(variable_name, value):
    Variable.set(variable_name, value)

def get_airflow_variable(variable_name, default_value=None):
    return Variable.get(variable_name, default_var=default_value)

def format_date_for_druid(iso_date_str):
    """
    Converte string no formato ISO8601 (com microsegundos e sufixo 'Z')
    para o formato que o Druid entende em TIMESTAMP: 'yyyy-MM-dd HH:mm:ss'.
    Exemplo de entrada:  '2025-05-12T00:00:05.232418Z'
    Exemplo de saída:    '2025-05-12 00:00:05'
    """
    # Remove o 'Z' (que indica UTC) e faz parse no Python
    dt = datetime.fromisoformat(iso_date_str.replace("Z", ""))

    # Retorna no formato 'yyyy-MM-dd HH:mm:ss'
    return dt.strftime("%Y-%m-%d %H:%M:%S")


def create_base_query(
    aggregation,
    agg_param_druid,
    tag_counter,
    add_minutes_to_endtime,
    array_agg_size,
    base_report_interval,
    last_max_date,
    internal_aggregation=None,
    internal_agg_param_druid=None,
    internal_add_minutes_to_endtime=None,
):
    """
    Exemplo de query "simples" com subconsulta interna (opcional) para duplo nível,
    caso deseje 'internal_aggregation' (MIN, MAX, SUM...) a partir do result do 1º select.
    Este exemplo não usa subqueries aninhadas. Ajustar caso precise de multiplos níveis internos.
    """

    query = f"""
    SELECT
        TIME_FLOOR(CAST("__time" AS TIMESTAMP), '{agg_param_druid}') AS "__time",
        TIMESTAMP_TO_MILLIS(
            TIME_FLOOR(CAST("__time" AS TIMESTAMP), '{agg_param_druid}')
        ) AS "beginTime",
        "additionalDn",
        "dn",
        "vendorName",
        CONCAT("name", '{tag_counter}', '_{aggregation}') AS "name",
        CONCAT("code", '{tag_counter}', '_{aggregation}') AS "code",
        "familyId",
        "elementType",
        {add_minutes_to_endtime} * 60 AS "reportInterval",
        "sourceSystem",
        "managerIp",
        TIMESTAMP_TO_MILLIS(
            TIME_FLOOR(CAST("__time" AS TIMESTAMP), '{agg_param_druid}')
            + INTERVAL '{add_minutes_to_endtime}' MINUTE
        ) AS "endTime",
        "isReliable",
        CONCAT('PT',CAST(({add_minutes_to_endtime} * 60) AS VARCHAR(255)), 'S') AS "granularityInterval",
        MV_TO_STRING(ARRAY_AGG(DISTINCT("managerFilename"), {array_agg_size}),',') AS "managerFilename",
        {aggregation}("value") AS "value"
    FROM
        "fastoss-pm-counters"
    WHERE
        "__time" > TIMESTAMP '{last_max_date}'
        AND "__time" < CURRENT_TIMESTAMP - INTERVAL '{add_minutes_to_endtime}' MINUTE
        AND "sourceSystem" = 'SLC'
        AND "reportInterval" = '{base_report_interval}'
    GROUP BY
        1,2,3,4,5,6,7,8,9,10,11,12,13,14
    """

    if internal_aggregation:
        query = f"""
        SELECT
            TIME_FLOOR(CAST("__time" AS TIMESTAMP), '{internal_agg_param_druid}') AS "__time",
            TIMESTAMP_TO_MILLIS(
                TIME_FLOOR(CAST("__time" AS TIMESTAMP), '{internal_agg_param_druid}')
            ) AS "beginTime",
            "additionalDn",
            "dn",
            "vendorName",
            CONCAT("name",'_{internal_aggregation}') AS "name",
            CONCAT("code",'_{internal_aggregation}') AS "code",
            "familyId",
            "elementType",
            {internal_add_minutes_to_endtime} * 60 AS "reportInterval",
            "sourceSystem",
            "managerIp",
            TIMESTAMP_TO_MILLIS(
                MILLIS_TO_TIMESTAMP("endTime") + INTERVAL '{internal_add_minutes_to_endtime}' MINUTE
            ) AS "endTime",
            "isReliable",
            CONCAT('PT',CAST(({internal_add_minutes_to_endtime} * 60) AS VARCHAR(255)), 'S') AS "granularityInterval",
            MV_TO_STRING(ARRAY_AGG("managerFilename"),',') AS "managerFilename",
            {internal_aggregation}("value") AS "value"
        FROM (
            {query}
        )
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14
        """
    return query


def create_multilevel_aggregations_query(last_max_date):
    """
    Gera a query de agregações em 3 estágios (1m → 5m → 15m)
    usando subconsultas aninhadas em vez de WITH, para evitar erros no Druid.
    Ajustamos a cláusula TIMESTAMP '{last_max_date}' para o formato "YYYY-MM-DD HH:mm:ss".
    """
    return f"""
SELECT
    TIME_FLOOR(agg_5m.interval_5m, 'PT15M') AS "__time",
    agg_5m.device,
    MAX(agg_5m.max_5m) AS "max",
    MAX(agg_5m.maxAvg_5m) AS "maxAvg",
    SUM(agg_5m.sum_5m) AS "sum",
    SUM(agg_5m.count_5m) AS "count",
    SUM(agg_5m.sum_5m) / SUM(agg_5m.count_5m) AS "avg",
    '15_minute' AS "aggregation_interval"
FROM (
    SELECT
        TIME_FLOOR(raw_1m.interval_1m, 'PT5M') AS interval_5m,
        raw_1m.device,
        MAX(raw_1m.max_1m) AS max_5m,
        MAX(raw_1m.avg_1m) AS maxAvg_5m,
        SUM(raw_1m.sum_1m) AS sum_5m,
        SUM(raw_1m.count_1m) AS count_5m,
        SUM(raw_1m.sum_1m) / SUM(raw_1m.count_1m) AS avg_5m
    FROM (
        SELECT
            TIME_FLOOR("__time", 'PT1M') AS interval_1m,
            "additionalDn" AS device,
            MAX("value") AS max_1m,
            SUM("value") AS sum_1m,
            COUNT("value") AS count_1m,
            SUM("value") / COUNT("value") AS avg_1m
        FROM "fastoss-pm-counters"
        WHERE
            "__time" > TIMESTAMP '{last_max_date}'
            AND "__time" < CURRENT_TIMESTAMP - INTERVAL '1' MINUTE
            AND "sourceSystem" = 'SLC'
        GROUP BY
            TIME_FLOOR("__time", 'PT1M'),
            "additionalDn"
    ) raw_1m
    GROUP BY
        TIME_FLOOR(raw_1m.interval_1m, 'PT5M'),
        raw_1m.device
) agg_5m
GROUP BY
    TIME_FLOOR(agg_5m.interval_5m, 'PT15M'),
    agg_5m.device
"""


def create_dag_function(aggregation_type, aggregation_name, config):
    """
    Função que gera o callable para a Tarefa PythonOperator,
    executando a consulta no Druid e publicando no Kafka.
    """
    def main():
        # Exemplo: se não houver valor, pega de 24h atrás em ISO8601
        string_past_24_hours = (
            datetime.now() - timedelta(hours=24)
        ).strftime("%Y-%m-%dT%H:%M:%S.%fZ")

        var_datetime_max = f"{VAR_DATETIME_MAX_PREFIX}_{aggregation_type}_{aggregation_name}"
        raw_start_date = get_airflow_variable(var_datetime_max, string_past_24_hours)

        print(f"[DEBUG] Valor da variável {var_datetime_max} = {raw_start_date}")

        # Converte a data ISO8601 -> formato 'YYYY-MM-DD HH:mm:ss' aceito pelo Druid
        start_date_formatted = format_date_for_druid(raw_start_date)
        print(f"[DEBUG] Data formatada para Druid = {start_date_formatted}")

        # Conexão com Druid
        druid = DruidConnector(druid_url_variable="druid_url")
        print("[DEBUG] Conexão estabelecida com Druid")

        # Se for agregação "pmsu_multilevel", chama a query aninhada
        if aggregation_name == "pmsu_multilevel":
            query = create_multilevel_aggregations_query(start_date_formatted)
            print("[DEBUG] Usando a query multinível (1m→5m→15m)")
        else:
            query = create_base_query(
                aggregation=aggregation_name,
                agg_param_druid=config['agg_param_druid'],
                tag_counter=config['tag_counter'],
                add_minutes_to_endtime=config['add_minutes_to_endtime'],
                array_agg_size=config['array_agg_size'],
                base_report_interval=config['base_report_interval'],
                last_max_date=start_date_formatted,
                internal_aggregation=config.get('internal_aggregation'),
                internal_agg_param_druid=config.get('internal_agg_param_druid'),
                internal_add_minutes_to_endtime=config.get('internal_add_minutes_to_endtime')
            )
            print(f"[DEBUG] Usando a query para {aggregation_name}")

        print(f"[DEBUG] Query gerada:\n{query}")

        # Executa a query
        result = druid.send_query(query)
        total_records = len(result) if result else 0
        print(f"[DEBUG] Retornou {total_records} registros do Druid")

        if result:
            # Publica no Kafka
            print("[DEBUG] Publicando registros no Kafka...")
            kafka = KafkaConnector(
                topic_var_name="agg-pm-counter-aggregates-topic",  # Tópico de agregados
                kafka_url_var_name="prod_kafka_url",
                kafka_port_var_name="prod_kafka_port",
                kafka_variable_pem_content="pem_content",
                kafka_connection_id="kafka_default"
            )
            producer = kafka.create_kafka_connection()
            kafka.send_mult_messages_to_kafka(menssages=result, producer=producer)
            print("[DEBUG] Publicação no Kafka concluída")

            # Identifica a maior data de "__time" retornada e salva na variável
            utils = DateUtils(data=result)
            max_date_from_column = utils.get_latest_date_parallel(
                date_column=DATE_TIME_COLUMN_DRUID,
                date_format="%Y-%m-%dT%H:%M:%S.%fZ"
            )
            print(f"[DEBUG] Maior data encontrada = {max_date_from_column}")

            set_airflow_variable(var_datetime_max, max_date_from_column)
            print(f"[DEBUG] Variável {var_datetime_max} atualizada = {max_date_from_column}")
        else:
            print("[DEBUG] Nenhum registro retornado. Skip.")
            raise AirflowSkipException('✅ No records returned. Process finished!')

    return main


#
# Criação dinâmica das DAGs com base em 'base_json' (extra do connection pmsu_connection).
#
for time_interval, aggregations in base_json.items():
    for agg_name, config in aggregations.items():
        dag_id = f'Agg_Counters_PMSU_{time_interval}_{agg_name}'

        dag = DAG(
            dag_id,
            default_args=DAG_DEFAULT_ARGS,
            description=f'DAG para agregação de contadores de PMSU - {time_interval} - {agg_name}',
            schedule_interval=config['agendamento'],
            start_date=datetime(2024, 12, 4),
            catchup=False,
            max_active_runs=1,
            tags=['counters', 'druid', 'pmsu', time_interval, agg_name],
        )

        start = DummyOperator(task_id='start', dag=dag)

        process_counters = PythonOperator(
            task_id='process_counters',
            python_callable=create_dag_function(time_interval, agg_name, config),
            provide_context=True,
            execution_timeout=timedelta(minutes=20),
            dag=dag
        )

        end = DummyOperator(task_id='end', dag=dag)

        # Encadeamento
        start >> process_counters >> end

        # Registra a DAG no globals() para que o Airflow a enxergue
        globals()[dag_id] = dag

