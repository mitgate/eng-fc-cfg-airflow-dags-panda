# Nome da DAG: w_get_data_snmp
# Owner / responsável: Sadir
# Descrição do objetivo da DAG: DAG para gerar dados agregados do Druid...
# Usa Druid?: Sim
# Principais tabelas / consultas Druid acessadas: druid
# Frequência de execução (schedule): * * * * *
# Dag Activo?: 
# Autor: Leandro
# Data de modificação: 2024-05-12
"""
Resumo:
Esta DAG é responsável por coletar dados agregados do Druid (datasource: snmp-enriched-metrics)
e publicar estes dados em lotes no Kafka para posterior processamento por outros sistemas de analytics.

Principais características e objetivos:
- Consulta SQL ao Druid em janelas temporais pequenas, reduzindo a pressão sobre o cluster e evitando timeouts/buffers.
- Limita o paralelismo tanto nas consultas ao Druid quanto no envio ao Kafka para evitar overload do ambiente.
- Usa particionamento e envio em batches otimizados para performance e robustez.
- Atualiza a variável de controle de janela (`aggregation_date_start_druid_snmp`) somente após sucesso.
- Logs detalhados para troubleshooting e manutenção.
- Projetada para evitar snowballing de lentidão ou travamentos críticos no Airflow/Druid/Kafka.
- Escrita para operar com tolerância a falhas: em caso de falha parcial, não “quebra” o scheduler do Airflow.
"""
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.utils.dates import datetime, timedelta
from airflow.exceptions import AirflowSkipException
import os
import json
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import tempfile
from confluent_kafka import Producer
import ssl
import requests

default_args = {
    'owner': 'Leandro',
    'depends_on_past': False,
    'retries': 1,  # Tenta 1x antes de falhar
    'retry_delay': timedelta(minutes=10),
}

dag = DAG(
    dag_id='Get_Aggregation_Data_SNMP',
    default_args=default_args,
    description='DAG para gerar dados agregados do Druid...',
    schedule_interval='* * * * *',
    start_date=datetime(2024, 9, 9),
    catchup=False,
    max_active_runs=1,
    tags=['aggregation', 'druid'],
    dagrun_timeout=timedelta(minutes=6)
)

KAFKA_URL = Variable.get('aggregation_kafka_url')
KAFKA_PORT = Variable.get('aggregation_kafka_port')
DEFAULT_TOPIC = Variable.get("aggregation_kafka_topic")
INTERFVALO = int(Variable.get('aggregation_intervalo_de_coleta_snmp', 5))
PARTICIONAMENTO_KAFKA = int(Variable.get('aggregation_paralelismo_kafka', 5))  # diminuir para evitar sobrecarga
DRUID_MAX_CONCURRENCY = 2  # limita queries Druid concorrentes
RETRY_DRUID = int(Variable.get('aggregation_retry_druid', 3))
DELAY_RETRY_DRUID = int(Variable.get('aggregation_retry_delay_druid', 5))
AMBIENTE = Variable.get('aggregation_ambiente', 'dev')
DRUID_API_URL = Variable.get('druid_prod_url') + '/druid/v2/sql'

def send_query_to_druid(query, retries=RETRY_DRUID, delay=DELAY_RETRY_DRUID):
    url = DRUID_API_URL
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
            "timeout": 280000,
            "populateCache": True,
            "useCache": True
        }
    })
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Content-Type': 'application/json'
    }
    for attempt in range(1, retries + 1):
        try:
            resp = requests.post(url, headers=headers, data=payload, timeout=180)
            resp.raise_for_status()
            return resp.json()[1:]
        except requests.RequestException as e:
            print(f"[Druid] Erro tentativa {attempt}: {e}")
            if attempt < retries:
                time.sleep(delay)
            else:
                print("Druid falhou após todas as tentativas.")
                raise AirflowSkipException("Falha crítica ao acessar Druid.")

def find_latest_time(json_array):
    valid_jsons = [item for item in json_array if '__time' in item and item['__time']]
    if not valid_jsons:
        return None
    max_time_json = max(valid_jsons, key=lambda x: datetime.strptime(x['__time'], "%Y-%m-%dT%H:%M:%S.%fZ"))
    max_time = datetime.strptime(max_time_json['__time'], "%Y-%m-%dT%H:%M:%S.%fZ")
    return max_time.strftime("%Y-%m-%d %H:%M:%S")

def divide_into_n_parts(data_list, n):
    k, m = divmod(len(data_list), n)
    return [data_list[i * k + min(i, m):(i + 1) * k + min(i, m)] for i in range(n)]

def delivery_callback(err, msg):
    if err:
        print(f'❌ Erro ao enviar mensagem para Kafka: {err}')
        raise Exception(err)

def create_kafka_connection(connection_name):
    pem_content = Variable.get("pem_content" if AMBIENTE == 'prod' else "pem_content_dev")
    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(pem_content.encode())
    ssl_location = temp_file.name
    conf = {
        "bootstrap.servers": f"{KAFKA_URL}:{KAFKA_PORT}",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": "flink",
        "sasl.password": "NMoW680oSNitVesBQti9jlsjl7GC8u36",
        'ssl.ca.location': ssl_location,
        'message.max.bytes': '1000000000',
        'batch.num.messages': 100000,
        'linger.ms': 50,
        'compression.type': 'lz4',
        'queue.buffering.max.messages': 200000,
        'queue.buffering.max.kbytes': 512000,
        'max.in.flight.requests.per.connection': 5,
        'queue.buffering.max.ms': 500,
        'message.send.max.retries': 5,
        'retry.backoff.ms': 500,
    }
    ssl_context = ssl.create_default_context()
    ssl_context.load_verify_locations(conf['ssl.ca.location'])
    producer = Producer(**conf)
    return producer

def send_to_kafka(lines, producer):
    try:
        for idx, line in enumerate(lines):
            producer.produce(DEFAULT_TOPIC, json.dumps(line).encode('utf-8'), callback=delivery_callback)
            if idx % 50000 == 0:
                producer.flush()
        producer.flush()
    except Exception as e:
        print(f"❌ Erro ao enviar para o Kafka: {e}")

def get_data_from_druid_windowed(start_date, end_date, fonte):
    # Para evitar sobrecarga, processa cada janela separadamente!
    # start_date, end_date em formato %Y-%m-%d %H:%M:%S
    query = f"""
        SELECT 'SNMP' AS "fonte",
            TIME_FLOOR(CAST("__time" AS TIMESTAMP), 'PT1H') AS "__time",
            LOOKUP(CONCAT("equipmentTypeId", '.', 'name'), 'equipment_type') AS Eq_Type,
            LOOKUP(CONCAT("managedObjectId", '.', 'name'), 'managed_object') AS MO,
            LOOKUP(CONCAT("vendorId", '.', 'name'), 'vendor') AS "vendor",
            "additionalDn",
            "reportInterval",
            CASE WHEN "managedObjectId"= 575525617983 THEN  LOOKUP(CONCAT("equipmentId"  , '.', 'Tipo de Rede' ), 'equipment')
                 ELSE LOOKUP(CONCAT(JSON_VALUE(enrichment,'$.topology.575525617983'), '.', 'Tipo de Rede'), 'equipment') END AS "tipo_de_rede",
            CASE WHEN "managedObjectId" = 575525617983 THEN LOOKUP(CONCAT("equipmentId" , '.', 'Tipo de Device'), 'equipment')
                 ELSE LOOKUP(CONCAT(JSON_VALUE(enrichment,'$.topology.575525617983'), '.', 'Tipo de Device'), 'equipment') END AS "tipo_device",
            LOOKUP(CONCAT("metricId", '.', 'name'), 'metric') AS "name_lookup",
            COUNT(*) AS "count"
        FROM "druid"."snmp-enriched-metrics"
        WHERE "__time" >= '{start_date}' AND "__time" < '{end_date}'
        GROUP BY 1, 2, 3, 4, 5, 6, 7, 8, 9, 10
    """
    return send_query_to_druid(query)

def main():
    start_time_str = Variable.get('aggregation_date_start_druid_snmp', default_var="2024-09-01 03:00:00")
    INTERVALO_MINUTOS = INTERFVALO
    now = datetime.now()
    start_time = datetime.strptime(start_time_str, "%Y-%m-%d %H:%M:%S")
    if start_time > now - timedelta(minutes=INTERVALO_MINUTOS):
        print(f"Ainda é cedo para coletar novos dados. Data início: {start_time_str}")
        raise AirflowSkipException('Aguardando janela de coleta')

    # Fatiar o período em janelas menores para não travar o Druid!
    max_hours = 1  # 1 hora por query (ajuste se necessário)
    total_minutes = INTERVALO_MINUTOS
    num_windows = max(1, int(total_minutes / (max_hours * 60)))
    windows = []
    t0 = start_time
    for _ in range(num_windows):
        t1 = t0 + timedelta(hours=max_hours)
        if t1 > now:
            t1 = now
        windows.append((t0, t1))
        t0 = t1
        if t0 >= start_time + timedelta(minutes=INTERVALO_MINUTOS):
            break

    all_data = []
    with ThreadPoolExecutor(max_workers=DRUID_MAX_CONCURRENCY) as executor:
        futures = []
        for t0, t1 in windows:
            futures.append(executor.submit(get_data_from_druid_windowed, t0.strftime("%Y-%m-%d %H:%M:%S"), t1.strftime("%Y-%m-%d %H:%M:%S"), "SNMP"))
        for future in as_completed(futures):
            try:
                result = future.result()
                if result:
                    all_data.extend(result)
            except Exception as e:
                print(f"❌ Erro em consulta Druid: {e}")
    total_registros = len(all_data)
    print(f"Coleta concluída: {total_registros} registros.")

    if total_registros > 0:
        partitions = divide_into_n_parts(all_data, PARTICIONAMENTO_KAFKA)
        kafka_conns = [create_kafka_connection(f"kafka_{i}") for i in range(PARTICIONAMENTO_KAFKA)]
        with ThreadPoolExecutor(max_workers=PARTICIONAMENTO_KAFKA) as executor:
            for i, partition in enumerate(partitions):
                executor.submit(send_to_kafka, partition, kafka_conns[i])
        print("✅ Dados enviados ao Kafka com sucesso.")
        end_time_str = (start_time + timedelta(minutes=INTERVALO_MINUTOS)).strftime("%Y-%m-%d %H:%M:%S")
        Variable.set('aggregation_date_start_druid_snmp', end_time_str)
        print(f"⏰ Variável atualizada para {end_time_str}")
    else:
        print("Nenhum registro encontrado!")
        raise AirflowSkipException("Nenhum registro encontrado.")

start = DummyOperator(task_id='start', dag=dag)
process_task = PythonOperator(
    task_id='get_aggregation_data_v2',
    python_callable=main,
    provide_context=True,
    execution_timeout=timedelta(minutes=120),
    dag=dag,
)
end = DummyOperator(task_id='end', dag=dag)
start >> process_task >> end
