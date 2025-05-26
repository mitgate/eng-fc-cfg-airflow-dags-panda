# Nome da DAG: w_get_logs_druid
# Owner / responsável: Sadir
# Descrição do objetivo da DAG: DAG para coletar os logs do Druid
# Usa Druid?: Sim
# Principais tabelas / consultas Druid acessadas: 
# Frequência de execução (schedule): */5 * * * *
# Dag Activo?: 
# Autor: Sadir
# Data de modificação: 2025-05-26

# 2

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

# Definindo argumentos padrão da DAG
default_args = {
    'owner': 'Sadir',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=10),
}

# Definindo a DAG explicitamente
dag = DAG(
    dag_id='Get_Logs_Druid',
    default_args=default_args,
    description='DAG para coletar os logs do Druid',
    schedule_interval='*/5 * * * *',  # Executa a cada 15 minutos
    start_date=datetime(2024, 9, 9),
    catchup=False,
    max_active_runs=1,  # Para garantir que apenas uma execução da DAG ocorra por vez
    tags=['logs', 'druid'],
)

# Funções Python
#=================================================================================================


user = Variable.get("logs_druid_user_open_shift") 
password = Variable.get("logs_druid_password_open_shift") 
host = "https://api.ocp-01.tdigital-vivo.com.br:6443"
namespace = "panda-druid"
log_dir = "/druid/data/sql/"  # Diretório onde os arquivos de logs estão no pod 
DEFAULT_TOPIC = Variable.get("logs_druid_topic") #"druid-test"
TIME_FILTER_MINUTES= int(Variable.get("logs_druid_time_filter")) #10
KAFKA_URL = "amqstreams-kafka-external-bootstrap-panda-amq-streams-dev.apps.ocp-01.tdigital-vivo.com.br"
#KAFKA_URL = "amqstreams-kafka-external-bootstrap.panda-amq-streams-dev.svc.cluster.local"
KAFKA_PORT = "443"
#KAFKA_PORT = "9095"



recent_time_threshold = (datetime.now() ) - timedelta(minutes=TIME_FILTER_MINUTES)
print(f"⏰ Hora base do filtro {recent_time_threshold}")


# Función para manejar la entrega de mensajes
def delivery_callback(err, msg):
    if err:
        print('❌ Error al enviar mensaje: %s' % err)
        raise

# Función para leer un archivo línea por línea y publicar cada línea en Kafka
def publish_file_to_kafka(logs):    
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
    # Carregar certificados confiáveis no contexto SSL
    ssl_context = ssl.create_default_context()
    ssl_context.load_verify_locations(conf['ssl.ca.location'])

    producer = Producer(**conf)
    print( "🔝 Iniciando envio ao Kafka...")
    quantidade_de_logs = len(logs)
    print(f"🔝 Total de logs: {quantidade_de_logs}")
    contador = 1
    try:
        for line in logs:
            nueva_linea = json.dumps(line, indent=2).encode('utf-8') #.replace('\t', ',').replace('\n', '')               
            producer.produce(DEFAULT_TOPIC, nueva_linea, callback=delivery_callback)
            print(f"▶ Mensagem {contador} enviada.")
            contador += 1
                          
        producer.flush()  
    #    print("Todas las líneas se han publicado en Kafka exitosamente.")
    except Exception as e:
        print("Error al publicar líneas en Kafka:", e)
        
# Função para executar comandos no terminal
def run_command(command):
    try:
        result = subprocess.run(command, shell=True, check=True, capture_output=True, text=True)
        return result.stdout
    except subprocess.CalledProcessError as e:
        print(f"🔴 Erro ao executar comando: {e}")
        return None

# Função para fazer login no OpenShift
def oc_login(user, password, host):
    print("🎩 Realizando login no OpenShift...")
    login_command = f"oc login --server={host} -u {user} -p {password} --insecure-skip-tls-verify=true"
    return run_command(login_command)

# Função para listar os pods no namespace panda-druid
def get_pods(namespace):
    print(f"⛏ Obtendo pods no namespace {namespace}...")
    get_pods_command = f"oc get pods -n {namespace} --no-headers -o custom-columns=NAME:.metadata.name"
    return run_command(get_pods_command).splitlines()

# Função para listar os arquivos de log dentro do diretório do pod
def list_log_files_in_pod(pod_name, namespace, log_dir):
    print(f"📓 Listando arquivos de log no diretório {log_dir} do pod {pod_name}...")
    list_files_command = f"oc exec {pod_name} -n {namespace} -- ls {log_dir}"
    files = run_command(list_files_command)
    if files:
        log_files = [file for file in files.splitlines() if file.endswith('.log')]
        return log_files
    else:
        print(f"❌ Falha ao listar arquivos no diretório {log_dir} do pod {pod_name}.")
        return []

# Função para baixar o arquivo de log do pod e salvar em um arquivo temporário
def download_log_file_to_temp(pod_name, namespace, log_file_path):
    print(f"🔎 Baixando o arquivo {log_file_path} do pod {pod_name} para um arquivo temporário...")
    logs_command = f"oc exec {pod_name} -n {namespace} -- cat {log_file_path}"
    logs = run_command(logs_command)
    if logs:
        # Criar um arquivo temporário para armazenar os logs
        temp_file = tempfile.NamedTemporaryFile(delete=False, mode='w')
        temp_file.write(logs)
        temp_file.close()
        return temp_file.name  # Retorna o caminho do arquivo temporário
    else:
        print(f"❌ Falha ao baixar logs do arquivo {log_file_path} no pod {pod_name}.")
        return None

# Função para filtrar os logs dos últimos X minutos
def filter_recent_logs_from_file(file_path, minutes=TIME_FILTER_MINUTES):
    print(f"⏰ Filtrando logs dos últimos {minutes} minutos no arquivo {file_path}...")
    recent_logs = []

    # Ler os logs do arquivo temporário
    with open(file_path, 'r') as file:
        for line in file:
            # Extrair timestamp no início da linha
            match = re.match(r"(\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z)", line)
            if match:
                log_time_str = match.group(1)
                log_time = datetime.strptime(log_time_str, "%Y-%m-%dT%H:%M:%S.%fZ")
                # Comparar com o tempo atual menos X minutos
                if log_time >= recent_time_threshold:
                    recent_logs.append(line)

    # Remover o arquivo temporário após o uso
    os.remove(file_path)
    return recent_logs

# Função que extrai as informações do log com base no padrão fornecido
def extract_log_info(line):
    fields = line.split("\t")

    log_data = {
        "timestamp": None,
        "queryType_json": None,
        "query_json": None,
        "sqlquery_time": None
    }

    # Extrair o timestamp (data e hora do evento) - sempre o primeiro campo
    if len(fields) > 0:
        log_data["timestamp"] = fields[0]

    # Extrair o JSON com "queryType" e "query"
    for field in fields:
        if field.startswith('{"queryType":'):
            try:
                ajustado = json.loads(field.encode('utf-8'))
                if 'intervals' in ajustado.keys():
                    if 'segments' in ajustado['intervals'].keys():
                        ajustado['intervals']['segments'] = len(ajustado['intervals']['segments'])
                log_data["queryType_json"] = ajustado
            except json.JSONDecodeError as e:
                print(f"❎❌ Erro ao decodificar JSON em 'queryType': {e}")
                print(f"String com erro: {field[1:50]}")
                log_data["queryType_json"] = None  # Marcar como None se o JSON não for válido

        elif field.startswith('{"query":'):
            try:
                log_data["query_json"] = json.loads(field)
            except json.JSONDecodeError as e:
                print(f"❎❌ Erro ao decodificar JSON em 'query': {e}")
                print(f"String com erro: {field[1:50]}")
                log_data["query_json"] = None  # Marcar como None se o JSON não for válido

        elif field.startswith('{"sqlQuery/time":'):
            try:
                log_data["sqlquery_time"] = json.loads(field)
            except json.JSONDecodeError as e:
                print(f"❎❌ Erro ao decodificar JSON em 'sqlQuery/time': {e}")
                print(f"String com erro: {field[1:50]}")
                log_data["sqlquery_time"] = None  # Marcar como None se o JSON não for válido

    return log_data

# Função que lida com a extração e filtragem de logs de um pod
def process_pod_logs(pod, namespace, log_dir, minutes=TIME_FILTER_MINUTES):
    all_recent_logs = []  # Lista para armazenar todos os logs recentes deste pod
    log_files = list_log_files_in_pod(pod, namespace, log_dir)
    for log_file in log_files:
        log_file_path = f"{log_dir}/{log_file}"
        temp_file = download_log_file_to_temp(pod, namespace, log_file_path)
        if temp_file:
            recent_logs = filter_recent_logs_from_file(temp_file, minutes=minutes)
            all_recent_logs.extend(recent_logs)
    return all_recent_logs

# Função principal para orquestrar o processo completo com execução paralela
def extract_and_combine_recent_logs():

    # 1. Fazer login no OpenShift
    if oc_login(user, password, host) is None:
        pass
        #return

    # 2. Obter a lista de pods no namespace panda-druid
    pods = get_pods(namespace)
    if not pods:
        print("😅 Nenhum pod encontrado no namespace.")
        #return

    # 3. Filtrar pods do broker (presumindo que os nomes contenham 'broker')
    broker_pods = [pod for pod in pods if 'broker' in pod]
    if not broker_pods:
        print("😅 Nenhum pod do broker encontrado.")
        #return

    # 4. Processar logs de cada pod em paralelo
    all_recent_logs = []  # Lista para armazenar todos os logs recentes de todos os pods
    with ThreadPoolExecutor(max_workers=len(broker_pods)) as executor:
        future_to_pod = {executor.submit(process_pod_logs, pod, namespace, log_dir, TIME_FILTER_MINUTES): pod for pod in broker_pods}

        for future in as_completed(future_to_pod):
            pod = future_to_pod[future]
            try:
                recent_logs = future.result()
                if recent_logs:
                    all_recent_logs.extend(recent_logs)
            except Exception as exc:
                print(f"🔴 Pod {pod} gerou uma exceção: {exc}")

    logs_clean = sorted(set(all_recent_logs))

    logs_to_send = [extract_log_info(i) for i in logs_clean]

    publish_file_to_kafka(logs=logs_to_send)



#=================================================================================================
# Definindo as tasks da DAG
start = DummyOperator(
    task_id='start',
    dag=dag,
)

process_dashboards_task = PythonOperator(
    task_id='get_logs',
    python_callable=extract_and_combine_recent_logs,
    provide_context=True,
    execution_timeout=timedelta(minutes=30),  # Limita a execução da task a 20 minutos
    dag=dag,
)

end = DummyOperator(
    task_id='end',
    dag=dag,
)

# Definindo as dependências das tarefas
start >> process_dashboards_task >> end
