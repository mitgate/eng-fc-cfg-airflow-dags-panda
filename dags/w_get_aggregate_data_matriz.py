# Nome da DAG: w_get_aggregate_data_matriz
# Owner / responsável: Leandro
# Descrição do objetivo da DAG: DAG para gerar dados agregados do Druid(v9)
# Usa Druid?: Sim
# Principais tabelas / consultas Druid acessadas: druid
# Frequência de execução (schedule): 
# Dag Activo?: 
# Autor: Leandro
# Data de modificação: 2025-05-26

import asyncio
import aiohttp
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
from requests.packages.urllib3.exceptions import InsecureRequestWarning
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import concurrent.futures
from datetime import datetime, timedelta
from concurrent.futures import ThreadPoolExecutor
# Suprime os warnings de SSL
requests.packages.urllib3.disable_warnings(InsecureRequestWarning)

MAX_WORKERS_AGG = int(Variable.get('worker_agg'))
SEGMENT_SIZE_AGG = int(Variable.get('segment_agg'))
MAX_REQUESTS_PER_SECOND = int(Variable.get('max_requests_per_second'))

default_args = {
    'owner': 'Leandro',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    dag_id='Test_Agreggation',
    default_args=default_args,
    description='DAG para gerar dados agregados do Druid(v9)',
    schedule_interval=None,
    start_date=datetime(2024, 9, 9),
    catchup=False,
    max_active_runs=1,
    tags=['agg_test'],
)

class DruidConnector(object):
    def __init__(self, r_delay=15, retries=5):
        self.retry_delay = r_delay
        self.retries = retries

    async def send_query(self, session, query=''):
        url = "https://druid.apps.ocp-01.tdigital-vivo.com.br/druid/v2/sql"
        payload = json.dumps({
            "query": query,
            "resultFormat": "object",
            "header": True,
            "typesHeader": True,
            "sqlTypesHeader": True,
            "context": {
                "enableWindowing": True,
                "useParallelMerge": True,
                "executionMode": "ASYNC",
                "populateCache": True,
                "useCache": True
            }
        })
        headers = {
            'Accept': 'application/json, text/plain, */*',
            'Content-Type': 'application/json'
        }

        attempt = 0
        while attempt < self.retries:
            try:
                async with session.post(url, headers=headers, data=payload, ssl=False) as response:
                    if response.status == 400:
                        print(query)
                    response.raise_for_status()
                    result = await response.json()
                    return result[1:]  # Retorna o resultado a partir da segunda linha (ignorando o cabeçalho)
            except Exception as e:
                print(f"❌ Erro na tentativa {attempt + 1}: {e}")
                attempt += 1
                if attempt < self.retries:
                    await asyncio.sleep(self.retry_delay)
                else:
                    raise

class KafkaConnect(object):
    def __init__(self):
        pass

    def create_kafka_connection(self, type_connection):
        DEFAULT_TOPIC = 'teste_agregation_sadir'
        KAFKA_URL = 'amqstreams-kafka-external-bootstrap-panda-amq-streams-dev.apps.ocp-01.tdigital-vivo.com.br'
        KAFKA_PORT = '443'
        pem_content = Variable.get("pem_content_dev")

        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(pem_content.encode())

        ssl_location = temp_file.name
        # Configurações do Kafka
        print(f'🔌 Informções de conexão ao tópico')
        print(f'🔌{KAFKA_URL}')
        print(f'🔌{KAFKA_PORT}')
        print(f'🔌{DEFAULT_TOPIC}')
        conf = {
            "bootstrap.servers": f"{KAFKA_URL}:{KAFKA_PORT}",
            "security.protocol": "SASL_SSL",
            "sasl.mechanism": "SCRAM-SHA-512",
            "sasl.username": "flink",
            "sasl.password": "NMoW680oSNitVesBQti9jlsjl7GC8u36",
            'ssl.ca.location': ssl_location,
            'message.max.bytes': '1000000000',
            'batch.num.messages': 1000000,
            'linger.ms': 50,
            'compression.type': 'lz4',
            'queue.buffering.max.messages': 2000000,
            'queue.buffering.max.kbytes': 2097152,
            'max.in.flight.requests.per.connection': 5,
            'queue.buffering.max.ms': 500,
            'message.send.max.retries': 5,
            'retry.backoff.ms': 500,
        }
        ssl_context = ssl.create_default_context()
        ssl_context.load_verify_locations(conf['ssl.ca.location'])
        if type_connection == 'Producer':
            return Producer(**conf)

def main():
    druid = DruidConnector()
    now = datetime.now()
    minutes = (now.minute // 15) * 15
    truncated_time = now.replace(minute=minutes, second=0, microsecond=0)
    start_date = truncated_time.strftime('%Y-%m-%d %H:%M:%S')
    end_date = (truncated_time - timedelta(minutes=15)).strftime('%Y-%m-%d %H:%M:%S')

    async def generate_query_and_send_to_kafka(dns, start_date, end_date, producer_kafka, session):
        dns = ",".join(f"'{item}'" for item in dns)
        query = f"""
        SELECT 
            TIME_FLOOR(__time, 'PT15M') - INTERVAL '3' HOUR AS __time
            ,metricId
            ,formulaId
            ,dn
            ,additionalDn
            ,elementType
            ,900 AS granularityInterval 
            ,sourceSystem
            ,managerIp
            ,isRate
            ,vendorId
            ,"sourceVendor"
            ,functionId
            ,equipmentId
            ,locationId
            ,equipmentTypeId
            ,managedObjectId
            ,networkGroups
            ,pollingRuleId
            ,SUM("value") AS value_sum
            ,MAX("value") AS value_max
            ,MIN("value") AS value_min
            ,COUNT("value") AS value_count
            ,ARRAY_AGG("value", 100000) AS value_array
        FROM  "druid"."snmp-enriched-metrics" 
        WHERE 
        dn IN ({dns})
        AND __time >= '{end_date}'
        AND __time < '{start_date}'
        GROUP BY 1,2,3,4,5,6,7,8,9,10,11,12,13,14,15,16,17,18,19
        """
        result_druid = await druid.send_query(session=session, query=query)
        await send_to_kafka(result_druid, producer_kafka, 'teste_agregation_sadir')

    async def send_to_kafka(lines, producer, topic):
        try:
            for idx, line in enumerate(lines):
                producer.produce(topic, json.dumps(line).encode('utf-8'))
                if idx % 100000 == 0:
                    producer.flush()
            producer.flush()
        except Exception as e:
            print(f"❌ Erro ao enviar para o Kafka: {e}")

    def segment_array(array, segment_size):
        return [array[i:i + segment_size] for i in range(0, len(array), segment_size)]

    async def start_process(segment_size, producer_kafka):
        async with aiohttp.ClientSession() as session:
            result_druid_dns = await druid.send_query(session, f"""
            
            SELECT DISTINCT dn FROM "druid"."snmp-enriched-metrics"
            WHERE __time >= CURRENT_TIMESTAMP - INTERVAL '15' MINUTE 
            """)
            print(f'💫 Total de objetos: {len(result_druid_dns)}')
            array_dns_result_druid = [i['dn'].replace("'", "''") for i in result_druid_dns]
            array_segments = segment_array(array_dns_result_druid, segment_size)
            print(f'🧩 Total de segmentos: {len(array_segments)}')
            tasks = []
            for idx, segment in enumerate(array_segments):
                if idx > 0 and idx % MAX_REQUESTS_PER_SECOND == 0:
                    await asyncio.sleep(1)  # Controla o limite de requisições por segundo
                tasks.append(generate_query_and_send_to_kafka(segment, start_date, end_date, producer_kafka, session))

            await asyncio.gather(*tasks)

    kafka = KafkaConnect()
    producer_kafka = kafka.create_kafka_connection('Producer')

    max_workers = MAX_WORKERS_AGG
    segment_size = SEGMENT_SIZE_AGG

    asyncio.run(start_process(segment_size, producer_kafka))

    print("🔝 Dados enviados ao Kafka ✅")

start = DummyOperator(
    task_id='start',
    dag=dag,
)

process_dashboards_task = PythonOperator(
    task_id='agg_test',
    python_callable=main,
    provide_context=True,
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

start >> process_dashboards_task >> end
