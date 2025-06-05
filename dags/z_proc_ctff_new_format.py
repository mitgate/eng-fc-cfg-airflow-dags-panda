# Nome da DAG: z_proc_ctff_new_format.py
# Owner / responsável: Leandro
# Descrição do objetivo da DAG: Faz uma requisição HTTP com lógica de retry em caso de erro.          
#   :param url: URL para a requisição.          
#   :param method: Método HTTP (GET, POST, etc.).
#   :param retries: Número máximo de tentativas em caso de erro.          
#   :param delay: Tempo (em segundos) entre as tentativas.          
#   :param kwargs: Parâmetros adicionais para requests.request (headers, data, etc.).          
#   :return: Resposta da requisição ou None se falhar após todas as tentativas.
# Usa Druid?: Sim
# Principais tabelas / consultas Druid acessadas: 
# Frequência de execução (schedule): */5 * * * *
# Dag Activo?: 
# Autor: Leandro
# Data de modificação: 2025-05-26

# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.dummy import DummyOperator
# from airflow.models import Variable
# from airflow.utils.dates import datetime, timedelta
# from hooks.kafka_connector import KafkaConnector
# from concurrent.futures import ThreadPoolExecutor, as_completed
# import requests
# import re
# import json

# TOKEN = 'fYaYIZ8DOHy5QSeJyVX6IYJBPVpPlS14LPRpljdc4uigTlxSXEWM3wCvcmRzZkhU'
# TOPIC= "ctf_new_format" 
# DAG_NAME = 'Proc_CTF_Newformat'
# PORT = 8427

# dag = DAG(
#     DAG_NAME,
#     default_args={
#         'owner': 'Leandro',
#         'depends_on_past': False,
#         'retries': 0,
#         'retry_delay': timedelta(minutes=5),
#     },
#     description=f'DAG para processar dados de CTF no novo formato.)',
#     schedule_interval= '*/5 * * * *',  # Executa a cada 30 minutos
#     start_date=datetime(2025, 1, 21),
#     catchup=False,
#     max_active_runs=1,
#     tags=['processamento', 'kafka', 'ctf'],
#     dagrun_timeout=timedelta(minutes=5)
# )

# MAX_WORKERS = 6
# DEFAULT_FAMILY_ID= "ctf"

# CONFIGS=json.loads(Variable.get("ctf_hostnames_ip"))

# METRICS=Variable.get("ctf_metrics", deserialize_json=True)

# MAPPING_STRING = Variable.get("ctf_mapping_string", deserialize_json=True)

# def get_sourcesystem(metric):
#     if 'ctf5g' in metric.lower():
#         return 'ctf5g'
#     elif 'ctf4g' in metric.lower():
#         return 'ctf4g'
#     else:
#         return 'ctf_infra'
# def get_agg_string(type_name):
#     return MAPPING_STRING[type_name]
# def get_type_agg_string(metric):
#     return METRICS[metric]
# def format_dn(additionaldn,data,metric):
#     type_agg_string_name = get_type_agg_string(metric)
#     mapping_string = get_agg_string(type_agg_string_name)
#     mapping_string  = re.sub(r'<additionalDn>', additionaldn, mapping_string)
    
#     def replacer(match):
#         key = match.group(1)  # Captura o nome da chave...
#         value = data['metric'].get(key, f"null", )  # Retorna null se não encontrar nada...
#         return value
    
    
#     # Usar regex para encontrar as chaves entre <>
#     pattern = r'<(.*?)>'
#     result = re.sub(pattern, replacer, mapping_string)
#     return result
# def process_data_ctf(data,ip):
#     metric = data['metric']['__name__'] 
#     additionaldn = f"{data['metric']['sitename']}-CTFUDR-CGW01"
#     dn = format_dn(additionaldn,data,metric)
#     sourceSystem = get_sourcesystem(metric)
    
#     return {
#         "familyId": DEFAULT_FAMILY_ID,
#         "code": metric,
#         "name": metric,
#         "dn": dn,
#         "value": data['value'][1], #Posição 1 representa o valor da métrica. 
#         "beginTime": data['value'][0], 
#         "endTime": data['value'][0] + (60*5), # +5 min
#         "granularityInterval": "PT300S", #✅Padrão
#         "reportInterval": 300, #✅Padrão
#         "elementType": DEFAULT_FAMILY_ID, 
#         "additionalDn": additionaldn, 
#         "vendorName": "Druid", #✅Padrão
#         "sourceSystem": sourceSystem,
#         "isReliable": "N/A",
#         "managerIp": ip,
#         "managerFilename": "N/A"
#     }
# def get_airflow_variable(variable_name, default_value=None):
#     return Variable.get(variable_name, default_var=default_value)

# def make_request_with_retry(url, method="GET", **kwargs):
#         """
#         Faz uma requisição HTTP com lógica de retry em caso de erro.

#         :param url: URL para a requisição.
#         :param method: Método HTTP (GET, POST, etc.).
#         :param retries: Número máximo de tentativas em caso de erro.
#         :param delay: Tempo (em segundos) entre as tentativas.
#         :param kwargs: Parâmetros adicionais para requests.request (headers, data, etc.).
#         :return: Resposta da requisição ou None se falhar após todas as tentativas.
#         """
#         try:
#             response = requests.request(method, url, **kwargs)
#             response.raise_for_status()  # Levanta erro para status HTTP 4xx/5xx
#             return response.json()
#         except Exception as e:
#             print(f'❌ ERROR:{e}')
#             return None

# def pagination(ip, metric, token, port):
#     payload = f'query={metric}'
#     headers = {
#         'Content-Type': 'application/x-www-form-urlencoded',
#         'Authorization': f"Bearer {token}"
#     }
#     url = f"http://{ip}:{port}/api/v1/query"
    
#     response_json = make_request_with_retry(url=url, method="POST", data=payload, headers=headers)
#     return response_json

# def process_ip_metric(ip, metric):
#     return_data_processed = []
#     """
#     Processa uma combinação de IP e métrica.
#     """
#     try:
#         return_data = pagination(ip, metric, token=TOKEN, port=PORT)
#         if 'data' in return_data.keys():
#             if 'result' in return_data['data'].keys():
          
#                 data_return = return_data['data']['result']
#                 for item in data_return:
#                     return_data_processed.append(process_data_ctf(item,ip))
#                 return return_data_processed
#     except Exception as e:
#         error_message = (f"❌ Erro ao processar IP: {ip}, Métrica: {metric}. Detalhes: {e}")
#         print(error_message)
#     return []

# def main():
#     print(f'🟣Create connection from CTF.')

#     base = []
#     tasks = [(config['IP'], metric) for config in CONFIGS for metric in METRICS]

#     with ThreadPoolExecutor() as executor:
#         future_to_task = {executor.submit(process_ip_metric, ip, metric): (ip, metric) for ip,metric in tasks}
#         for future in as_completed(future_to_task):
#             ip, metric = future_to_task[future]
#             try:                                
#                 result = future.result()
#                 if result:
#                     base.extend(result)
#             except Exception as e:
#                 print(f"❌ Erro ao processar IP: {ip}, Métrica: {metric}. Detalhes: {e}")

#     print(f"✅ Coleta concluída. Total de registros: {len(base)}")
    
#     if len(base)>0:
        
#         print('🔵 Start connection with Kafka.')
#         kafka = KafkaConnector(
#             topic_var_name=TOPIC,
#             kafka_url_var_name="prod_kafka_url",
#             kafka_port_var_name="prod_kafka_port",
#             kafka_variable_pem_content="pem_content",
#             kafka_connection_id="kafka_default"
#         )
#         producer = kafka.create_kafka_connection()
#         print('🔵 Created Kafka producer.')

#         print('🔵 Sending elements name to Kafka...')
#         kafka.send_mult_messages_to_kafka(  
#             menssages=base,
#             producer=producer
#         )
#         print(f'✅ Data sent on Kafka.')
       
#     else:
#         print('❌ No records returned!')
#         raise SystemError

# start = DummyOperator(
#     task_id='start',
#     dag=dag
# )

# process_counters = PythonOperator(
#     task_id='process_ctf',
#     python_callable=main,
#     provide_context=True,
#     execution_timeout=timedelta(minutes=120),  # Limita a execução da task a 20 minutos
#     dag=dag
# )

# end = DummyOperator(
#     task_id='end',
#     dag=dag
# )

# # Definindo as dependências das tarefas
# start >> process_counters >> end


# Nome da DAG: z_proc_ctff_new_format.py
# Owner / responsável: Leandro
# Descrição do objetivo da DAG: Faz uma requisição HTTP com lógica de retry em caso de erro.
#   :param url: URL para a requisição.
#   :param method: Método HTTP (GET, POST, etc.).
#   :param retries: Número máximo de tentativas em caso de erro.
#   :param delay: Tempo (em segundos) entre as tentativas.
#   :param kwargs: Parâmetros adicionais para requests.request (headers, data, etc.).
#   :return: Resposta da requisição ou None se falhar após todas as tentativas.
# Usa Druid?: Sim
# Principais tabelas / consultas Druid acessadas:
# Frequência de execução (schedule): */5 * * * *
# Dag Activo?:
# Autor: Leandro
# Data de modificação: 2025-05-26

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.utils.dates import datetime, timedelta
from hooks.kafka_connector import KafkaConnector
from concurrent.futures import ThreadPoolExecutor, as_completed
import requests
import re
import json
from datetime import datetime as dt, timezone

TOKEN = 'fYaYIZ8DOHy5QSeJyVX6IYJBPVpPlS14LPRpljdc4uigTlxSXEWM3wCvcmRzZkhU'
TOPIC= "ctf_new_format"
DAG_NAME = 'Proc_CTF_Newformat'
PORT = 8427

dag = DAG(
    DAG_NAME,
    default_args={
        'owner': 'Leandro',
        'depends_on_past': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    },
    description=f'DAG para processar dados de CTF no novo formato.',
    schedule_interval= '*/5 * * * *',  # Executa a cada 5 minutos
    start_date=datetime(2025, 1, 21),
    catchup=False,
    max_active_runs=1,
    tags=['processamento', 'kafka', 'ctf'],
    dagrun_timeout=timedelta(minutes=5)
)

MAX_WORKERS = 6
DEFAULT_FAMILY_ID= "ctf"

CONFIGS=json.loads(Variable.get("ctf_hostnames_ip"))
METRICS=Variable.get("ctf_metrics", deserialize_json=True)
MAPPING_STRING = Variable.get("ctf_mapping_string", deserialize_json=True)

def round_epoch_to_minute(epoch):
    dt_ = dt.fromtimestamp(epoch, tz=timezone.utc)
    dt_rounded = dt_.replace(second=0, microsecond=0)
    return int(dt_rounded.timestamp())

def get_sourcesystem(metric):
    if 'ctf5g' in metric.lower():
        return 'ctf5g'
    elif 'ctf4g' in metric.lower():
        return 'ctf4g'
    else:
        return 'ctf_infra'

def get_agg_string(type_name):
    return MAPPING_STRING[type_name]

def get_type_agg_string(metric):
    return METRICS[metric]

def format_dn(additionaldn,data,metric):
    type_agg_string_name = get_type_agg_string(metric)
    mapping_string = get_agg_string(type_agg_string_name)
    mapping_string  = re.sub(r'<additionalDn>', additionaldn, mapping_string)
    def replacer(match):
        key = match.group(1)
        value = data['metric'].get(key, f"null")
        return value
    pattern = r'<(.*?)>'
    result = re.sub(pattern, replacer, mapping_string)
    return result

def process_data_ctf(data, ip):
    metric = data['metric']['__name__']
    additionaldn = f"{data['metric']['sitename']}-CTFUDR-CGW01"
    dn = format_dn(additionaldn, data, metric)
    sourceSystem = get_sourcesystem(metric)
    # Zera o segundo do beginTime
    begin_time = round_epoch_to_minute(data['value'][0])
    end_time = begin_time + (60*5)
    print(f"[DEBUG] process_data_ctf: metric={metric}, original_beginTime={data['value'][0]}, beginTime_rounded={begin_time}")
    return {
        "familyId": DEFAULT_FAMILY_ID,
        "code": metric,
        "name": metric,
        "dn": dn,
        "value": data['value'][1],
        "beginTime": begin_time,
        "endTime": end_time,
        "granularityInterval": "PT300S",
        "reportInterval": 300,
        "elementType": DEFAULT_FAMILY_ID,
        "additionalDn": additionaldn,
        "vendorName": "Druid",
        "sourceSystem": sourceSystem,
        "isReliable": "N/A",
        "managerIp": ip,
        "managerFilename": "N/A"
    }

def get_airflow_variable(variable_name, default_value=None):
    return Variable.get(variable_name, default_var=default_value)

def make_request_with_retry(url, method="GET", **kwargs):
    try:
        print(f"[DEBUG] Requesting URL: {url} with method {method}")
        response = requests.request(method, url, **kwargs)
        response.raise_for_status()
        print("[DEBUG] Request succeeded")
        return response.json()
    except Exception as e:
        print(f'❌ ERROR:{e}')
        return None

def pagination(ip, metric, token, port):
    payload = f'query={metric}'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded',
        'Authorization': f"Bearer {token}"
    }
    url = f"http://{ip}:{port}/api/v1/query"
    print(f"[DEBUG] pagination: IP={ip}, metric={metric}, URL={url}")
    response_json = make_request_with_retry(url=url, method="POST", data=payload, headers=headers)
    return response_json

def process_ip_metric(ip, metric):
    return_data_processed = []
    try:
        return_data = pagination(ip, metric, token=TOKEN, port=PORT)
        if return_data and 'data' in return_data.keys():
            if 'result' in return_data['data'].keys():
                data_return = return_data['data']['result']
                for item in data_return:
                    return_data_processed.append(process_data_ctf(item, ip))
                print(f"[DEBUG] process_ip_metric: IP={ip}, metric={metric}, registros={len(return_data_processed)}")
                return return_data_processed
    except Exception as e:
        error_message = (f"❌ Erro ao processar IP: {ip}, Métrica: {metric}. Detalhes: {e}")
        print(error_message)
    return []

def main():
    print(f'🟣Create connection from CTF.')
    base = []
    tasks = [(config['IP'], metric) for config in CONFIGS for metric in METRICS]
    print(f"[DEBUG] Total de tasks para execução: {len(tasks)}")
    with ThreadPoolExecutor() as executor:
        future_to_task = {executor.submit(process_ip_metric, ip, metric): (ip, metric) for ip, metric in tasks}
        for future in as_completed(future_to_task):
            ip, metric = future_to_task[future]
            try:
                result = future.result()
                if result:
                    base.extend(result)
            except Exception as e:
                print(f"❌ Erro ao processar IP: {ip}, Métrica: {metric}. Detalhes: {e}")
    print(f"✅ Coleta concluída. Total de registros: {len(base)}")
    if len(base) > 0:
        print('🔵 Start connection with Kafka.')
        kafka = KafkaConnector(
            topic_var_name=TOPIC,
            kafka_url_var_name="prod_kafka_url",
            kafka_port_var_name="prod_kafka_port",
            kafka_variable_pem_content="pem_content",
            kafka_connection_id="kafka_default"
        )
        producer = kafka.create_kafka_connection()
        print('🔵 Created Kafka producer.')
        print('🔵 Sending elements name to Kafka...')
        kafka.send_mult_messages_to_kafka(
            menssages=base,
            producer=producer
        )
        print(f'✅ Data sent on Kafka.')
    else:
        print('❌ No records returned!')
        raise SystemError

start = DummyOperator(
    task_id='start',
    dag=dag
)

process_counters = PythonOperator(
    task_id='process_ctf',
    python_callable=main,
    provide_context=True,
    execution_timeout=timedelta(minutes=120),
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

# Definindo as dependências das tarefas
start >> process_counters >> end
