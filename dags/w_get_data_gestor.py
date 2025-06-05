# Nome da DAG: w_get_data_gestor
# Owner / responsável: Leandro
# Descrição do objetivo da DAG: DAG para gerar dados agregados do Druid...
# Usa Druid?: Sim
# Principais tabelas / consultas Druid acessadas: 
# Frequência de execução (schedule): * * * * *
# Dag Activo?: 
# Autor: Leandro
# Data de modificação: 2025-05-26

#%% 4
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.utils.dates import datetime, timedelta
import os
import json
from airflow.exceptions import AirflowSkipException
import re
from concurrent.futures import ThreadPoolExecutor, as_completed
import tempfile
from confluent_kafka import Producer
import sys
import ssl
import subprocess
import requests
import time


# Definindo argumentos padrão da DAG
default_args = {
    'owner': 'Leandro',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
}

# Definindo a DAG explicitamente
dag = DAG(
    dag_id='Get_Aggregation_Data_Gestores',
    default_args=default_args,
    description='DAG para gerar dados agregados do Druid...',
    schedule_interval='* * * * *',  # Executa a cada 15 minutos
    start_date=datetime(2024, 9, 9),
    catchup=False,
    max_active_runs=1,  # Para garantir que apenas uma execução da DAG ocorra por vez
    tags=['aggregation', 'druid'],
)

# Funções Python
#=================================================================================================


KAFKA_URL = Variable.get('aggregation_kafka_url')
KAFKA_PORT = Variable.get('aggregation_kafka_port')
DEFAULT_TOPIC = Variable.get("aggregation_kafka_topic")

INTERFVALO = int(Variable.get('aggregation_intervalo_de_coleta_gestor',15))
PARTICIONAMENTO_KAFKA = int(Variable.get('aggregation_paralelismo_kafka',20))
RETRY_DRUID = int(Variable.get('aggregation_retry_druid',3))
DELAY_RETRY_DRUID = int(Variable.get('aggregation_retry_delay_druid',5))
AMBIENTE = Variable.get('aggregation_ambiente','dev')
DRUID_API_URL = Variable.get('druid_prod_url')+ '/druid/v2/sql'

def search_key(key,dict):
    for i in dict:
        if key == i['k']: 
            return i['v']
    return None

def send_query_to_druid(query, retries=RETRY_DRUID, delay=DELAY_RETRY_DRUID):
    url = DRUID_API_URL #"https://druid.apps.ocp-01.tdigital-vivo.com.br"
    print(f"Consulta que será executada: {query}")
      
    payload = json.dumps({
      "query": query,
      "resultFormat": "object",
      "header": True,
      "typesHeader": True,
      "sqlTypesHeader": True,
      "context":{
               "enableWindowing":True,
               "useParallelMerge":True,
               "executionMode":"ASYNC",
               "timeout":280000,
               "populateCache":True,
               "useCache":True
        
        }
    })
    
    headers = {
      'Accept': 'application/json, text/plain, */*',
      'Content-Type': 'application/json'
    }
    
    attempt = 0
    while attempt < retries:
        try:
            response = requests.request("POST", url, headers=headers, data=payload)
            response.raise_for_status()  # Lança uma exceção para status HTTP de erro
            return response.json()[1:]  # Retorna o resultado a partir da segunda linha (ignorando o cabeçalho)
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro na tentativa {attempt + 1}: {e}")
            attempt += 1
            if attempt < retries:
                print(f"🆎 Tentando novamente em {delay} segundos...")
                time.sleep(delay)
                
            else:
                print("❌ Número máximo de tentativas atingido. Falha ao executar a consulta.")
                raise

# Função para encontrar o maior __time e retornar no formato %Y-%m-%d %H:%M:%S
def find_latest_time(json_array):
    # Filtrar os JSONs válidos que possuem o campo '__time'
    valid_jsons = [item for item in json_array if '__time' in item and item['__time']]
    
    if not valid_jsons:
        return None  # Retorna None se não houver valores válidos

    # Encontrar o maior __time
    max_time_json = max(valid_jsons, key=lambda x: datetime.strptime(x['__time'], "%Y-%m-%dT%H:%M:%S.%fZ"))

    # Converter para o formato desejado: %Y-%m-%d %H:%M:%S
    max_time = datetime.strptime(max_time_json['__time'], "%Y-%m-%dT%H:%M:%S.%fZ")
    return max_time.strftime("%Y-%m-%d %H:%M:%S")

# Função para dividir o array em N partes iguais
def divide_into_n_parts(data_list, n):
    k, m = divmod(len(data_list), n)
    return [data_list[i * k + min(i, m):(i + 1) * k + min(i + 1, m)] for i in range(n)]
        
def delivery_callback(err, msg):
    if err:
        print('❌ Error al enviar mensaje: %s' % err)
        raise

def get_end_time(start_time_str):
    # Converter a string de data para um objeto datetime
    start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
    # Adiciona o intervalo...
    end_time = start_time + timedelta(minutes=INTERFVALO)
    # Converte para string util novamente..
    end_time_str = end_time.strftime("%Y-%m-%d %H:%M:%S")
    
    return end_time_str

def get_data_from_druid(start_date,end_date,fonte):
    
    if fonte == "SNMP":
        query = f"SELECT 'SNMP' AS \"fonte\" ,TIME_FLOOR(CAST(\"__time\" AS TIMESTAMP), 'PT1H') AS \"__time\",\nLOOKUP(CONCAT(\"equipmentTypeId\", '.', 'name'), 'equipment_type') as Eq_Type,\nLOOKUP(CONCAT(\"managedObjectId\", '.', 'name'), 'managed_object') as MO,\nLOOKUP(CONCAT(\"vendorId\"  , '.', 'name' ), 'vendor')   AS \"vendor\",\n\"additionalDn\",\"reportInterval\",  LOOKUP(CONCAT(\"equipmentId\", '.', 'Tipo de Rede'), 'equipment') AS \"tipo_de_rede\",  LOOKUP(CONCAT(\"equipmentId\", '.', 'Tipo de Device'), 'equipment') AS \"tipo_device\",  LOOKUP(CONCAT(\"metricId\", '.', 'name'), 'metric') AS \"name_lookup\", COUNT(*) AS \"count\" FROM \"druid\".\"snmp-enriched-metrics\" \n\nWHERE \"__time\" >= '{start_date}' AND \"__time\"  < '{end_date}' GROUP BY 1,2,3,4,5,6,7,8,9,10"
    if fonte == "GESTOR":
        query = f"SELECT  'GESTORES' AS fonte, TIME_FLOOR(CAST(__time AS TIMESTAMP), 'PT1H') AS __time, elementType,  additionalDn, reportInterval, LOOKUP(CONCAT(equipmentTypeId, '.name'), 'equipment_type') as Eq_Type, LOOKUP(CONCAT(managedObjectId, '.name'), 'managed_object') as MO, LOOKUP(CONCAT(metricId,  '.name'), 'metric') AS name_lookup,  LOOKUP(CONCAT(vendorId,  '.name' ), 'vendor')   AS vendor, LOOKUP(CONCAT(metricId,  '.domain'), 'metric') as dominio, LOOKUP(CONCAT(metricId,  '.category'), 'metric') as category, LOOKUP(CONCAT(formulaId, '.technology'), 'formula') as technology, COUNT(*) AS \"count\"   FROM \"druid\".\"fastoss-pm-enriched-metrics\"  WHERE \"__time\" >= '{start_date}' AND \"__time\"  < '{end_date}'  GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12"
    return_json = send_query_to_druid(query)
    return_json = return_json[1:]
    
    return return_json

def create_kafka_connection(connection_name):
    if AMBIENTE == 'prod':
        pem_content = Variable.get("pem_content")
    else:
        pem_content = Variable.get("pem_content_dev")

            
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(pem_content.encode())

    ssl_location = temp_file.name
            
    # Configurações do Kafka
    
    conf = {
        "bootstrap.servers": f"{KAFKA_URL}:{KAFKA_PORT}",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": "flink",
        "sasl.password": "NMoW680oSNitVesBQti9jlsjl7GC8u36",
        'ssl.ca.location': ssl_location,
        'message.max.bytes': '1000000000',
        'batch.num.messages': 1000000,  # Aumentar para 500 mil ou mais
        'linger.ms': 50,  # Aumentar para 50ms
        'compression.type': 'lz4',  # Habilitar compressão
        'queue.buffering.max.messages': 2000000,  # Aumentar o tamanho do buffer
        'queue.buffering.max.kbytes': 2097152,  # Aumentar o limite em KB (1 GB)
        'max.in.flight.requests.per.connection': 5,  # Aumentar requisições simultâneas
        'queue.buffering.max.ms': 500, # Tempo máximo para agrupar mensagens
        'message.send.max.retries': 5, # Retry na hora de enviar
        'retry.backoff.ms': 500, # Tempo entre cada retry
    }
    """
    # Processando 1.6 por segundo...
    {
        "bootstrap.servers": "amqstreams-kafka-external-bootstrap-panda-amq-streams-dev.apps.ocp-01.tdigital-vivo.com.br:443",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": "flink",
        "sasl.password": "NMoW680oSNitVesBQti9jlsjl7GC8u36",
        'ssl.ca.location': ssl_location,
        'message.max.bytes': '1000000000',
        'batch.num.messages': 500000,  # Aumentar para 500 mil ou mais
        'linger.ms': 50,  # Aumentar para 50ms
        'compression.type': 'lz4',  # Habilitar compressão
        'queue.buffering.max.messages': 1000000,  # Aumentar o tamanho do buffer
        'queue.buffering.max.kbytes': 1048576,  # Aumentar o limite em KB (1 GB)
        'queue.buffering.max.ms': 500, # Tempo máximo para agrupar mensagens
        'message.send.max.retries': 5, # Retry na hora de enviar
        'retry.backoff.ms': 500, # Tempo entre cada retry
    }
    """
    # Carregar certificados confiáveis no contexto SSL
    ssl_context = ssl.create_default_context()
    ssl_context.load_verify_locations(conf['ssl.ca.location'])

    producer = Producer(**conf)
    #print(f"🔹 Conexão Kafka '{connection_name}' criada com sucesso!")
    return producer

def send_to_kafka(lines, producer):    

    """
    {
    "__time": "2024-09-27T23:00:00.000Z",
    "additionalDn": "I-BR-RJ-RJO-BAR-PS-EDNS-01",
    "reportInterval": "300",
    "equipmentId": None,
    "metricId": "193273549184",
    "count": 1072,
    "":"",
    "":""...
    }
    
    # Processando 1.6 por segundo..
    try:
        for line in lines:
            nueva_linea = json.dumps(line, indent=2).encode('utf-8')               
            producer.produce(DEFAULT_TOPIC, nueva_linea, callback=delivery_callback)
        # Fazer flush uma vez após enviar todas as mensagens da partição
        producer.flush()
    except Exception as e:
        print("❌ Error al publicar líneas en Kafka:", e)
    """
    try:
        for idx, line in enumerate(lines):
            # Enviar mensagem para o Kafka
            producer.produce(DEFAULT_TOPIC, json.dumps(line).encode('utf-8'), callback=delivery_callback)

            # Realizar flush a cada 500.000 mensagens para evitar fila cheia
            if idx % 500000 == 0:
                producer.flush()
        
        # Flush final para garantir envio de todas as mensagens
        producer.flush()
    except Exception as e:
        print(f"❌ Erro ao enviar para o Kafka: {e}")

def validate_execution(var_value):
    # Agora..
    date_time_now = datetime.now()
    start_time = datetime.strptime(var_value, "%Y-%m-%d %H:%M:%S")
    return start_time < date_time_now - timedelta(hours=3)  
   
def main():
    # Informação virá de uma variável
    start_time_str = Variable.get('aggregation_date_start_druid_gestor',default_var="2024-09-01 03:00:00")

    status_execution_v = validate_execution(start_time_str)

    if status_execution_v:
        print(f"Tudo certo posso executar a coleta! 😁 | {start_time_str}")
        # Gerar lista com datas a partir da variável
        end_time_str = get_end_time(start_time_str)
        
        fonte = "GESTOR"
        
        # Iterar sobre a lista de datas...
        print(f"⏰ Rodando o período: {start_time_str} até {end_time_str}")
        
        # Coleta os dados da consulta...
        return_data_to_send_kafka = get_data_from_druid(start_time_str, end_time_str, fonte)
        
        total_de_registros = len(return_data_to_send_kafka)
        if total_de_registros > 0:
            kafka_connections = []
            print(f"🧧 Total de registros para enviar: {total_de_registros}")
            
            
            print(f"🔹 Criando {PARTICIONAMENTO_KAFKA} conexões com Kafka... ")
            for i in range(PARTICIONAMENTO_KAFKA):
                kafka_connections.append(create_kafka_connection(f"conexao_kafka_{i+1}"))
                
            # Dividir os dados em 5 partes iguais
            partitions = divide_into_n_parts(return_data_to_send_kafka, PARTICIONAMENTO_KAFKA)
            
            # Enviar as partições em paralelo usando threads
            print( "🔹 Iniciando envio ao Kafka...")
            with ThreadPoolExecutor(max_workers=PARTICIONAMENTO_KAFKA) as executor:
                for i, partition in enumerate(partitions):
                    executor.submit(send_to_kafka, partition, kafka_connections[i])
                    
            print(f"✅ Dados enviados ao Kafka")
            #maior__time = find_latest_time(return_data_to_send_kafka)
            print(f"⏰ Atualizando variável [aggregation_date_start_druid_gestor] com a data mais recentes: {end_time_str}")
            Variable.set('aggregation_date_start_druid_gestor',end_time_str)
            print(f"✅ Processo finalizado! ✅")
        else:
            print("Nenhum registro encontrado! 👌")

    else:
        print(f"Ainda é muito cedo para coletar novos dados...😥 | {start_time_str}")   
  
#=================================================================================================
# Definindo as tasks da DAG
start = DummyOperator(
    task_id='start',
    dag=dag,
)

process_dashboards_task = PythonOperator(
    task_id='get_aggregation_data_gestores',
    python_callable=main,
    provide_context=True,
    execution_timeout=timedelta(minutes=120),  # Limita a execução da task a 20 minutos
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Definindo as dependências das tarefas
start >> process_dashboards_task >> end
