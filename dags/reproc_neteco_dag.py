from airflow import DAG
from airflow.operators.python import PythonOperator
from ftplib import FTP_TLS
import ssl
import os
from typing import List
from datetime import datetime, timedelta
import json
import tempfile
import requests
import time
from airflow.models import Variable
from confluent_kafka import Producer

# Configurações de ambiente
RETRY_DRUID = int(Variable.get('aggregation_retry_druid', 3))
DELAY_RETRY_DRUID = int(Variable.get('aggregation_retry_delay_druid', 5))
AMBIENTE = Variable.get('reproc_ericsson_ambiente', 'dev')
KAFKA_URL = Variable.get('aggregation_kafka_url')
KAFKA_PORT = Variable.get('aggregation_kafka_port')

# Argumentos padrão da DAG
default_args = {
        'owner': 'Patrick',
        'depends_on_past': False,
        'retries': 1,
        'retry_delay': timedelta(minutes=5),
        'execution_timeout': timedelta(hours=4),
        'email_on_failure': False,
        'email_on_retry': False,
    }

def delivery_callback(err, msg):
    """Callback para verificar o status de entrega da mensagem"""
    if err:
        print(f'❌ Erro ao enviar mensagem: {err}')
    else:
        print(f'✅ Mensagem enviada com sucesso: {msg.topic()}[{msg.partition()}]')

class KafkaHandler:
    def __init__(self):
        self.producer = self._create_kafka_connection()
    
    def _create_kafka_connection(self):
        """Cria conexão com o Kafka com configurações de segurança"""
        # Obter ambiente
        ambiente = Variable.get("AMBIENTE", default_var="dev")
        
        # Obter conteúdo do certificado PEM baseado no ambiente
        if ambiente == 'prod':
            pem_content = Variable.get("pem_content")
        else:
            pem_content = Variable.get("pem_content_dev")

        # Criar arquivo temporário para o certificado
        with tempfile.NamedTemporaryFile(delete=False) as temp_file:
            temp_file.write(pem_content.encode())
        
        ssl_location = temp_file.name
        
        # Configurações do Kafka
        conf = {
            "bootstrap.servers": f"{Variable.get('aggregation_kafka_url')}:{Variable.get('aggregation_kafka_port')}",
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
            'queue.buffering.max.kbytes': 2097151,
            'max.in.flight.requests.per.connection': 5,
            'queue.buffering.max.ms': 500,
            'message.send.max.retries': 5,
            'retry.backoff.ms': 500,
            'request.timeout.ms': 30000,
            'socket.timeout.ms': 60000,
            'socket.keepalive.enable': True,
            'reconnect.backoff.ms': 1000,
            'reconnect.backoff.max.ms': 10000,
        }
        
        # Carregar certificados confiáveis no contexto SSL
        ssl_context = ssl.create_default_context()
        ssl_context.load_verify_locations(conf['ssl.ca.location'])
        
        return Producer(**conf)
    
    def publish_message_with_key(self, value: str, topic: str, key: str) -> bool:
        """Publica mensagem no Kafka com uma chave específica"""
        try:
            print(f"🔄 Tentando enviar mensagem para o tópico {topic}")
            print(f" Key: {key}")
            print(f"📝 Value: {value}")
            
            self.producer.produce(
                topic=topic,
                key=key,
                value=value.encode('utf-8'),
                callback=delivery_callback
            )
            
            self.producer.flush(timeout=30)
            return True
        except Exception as e:
            print(f"❌ Erro ao enviar mensagem para o Kafka: {e}")
            print(f"⚙️ Configurações atuais do producer: {self.producer.config}")
            return False

class FTPSClient:
    def __init__(self, host: str, username: str, password: str, port: int = 21):
        self.host = host
        self.username = username
        self.password = password
        self.port = port
        
    def list_files_in_directories_period(self, base_path: str, extension: str, data_inicio: datetime, data_fim: datetime) -> List[tuple]:
        """
        Lista arquivos dentro do período especificado com a extensão determinada
        Retorna uma lista de tuplas (caminho_arquivo, data_modificacao)
        """
        found_files = []
        
        try:
            # Criar conexão FTPS
            ftps = FTP_TLS()
            ftps.set_debuglevel(2)  # Ativa logs para debug
            
            print(f"\n🔍 Conectando ao servidor: {self.host}:{self.port}")
            ftps.connect(host=self.host, port=self.port, timeout=360)
            ftps.login(user=self.username, passwd=self.password)
            
            # Ativar proteção de dados
            ftps.prot_p()
            ftps.set_pasv(True)
            
            print(f"📂 Explorando diretório base: {base_path}")
            try:
                ftps.cwd(base_path)
            except Exception as e:
                print(f"❌ Erro ao acessar diretório base: {str(e)}")
                raise
            
            # Listar diretórios
            directories = []
            def append_dir(line):
                directories.append(line)
            ftps.dir(append_dir)
            
            total_dirs = len(directories)
            print(f"📁 Total de diretórios encontrados: {total_dirs}")
            
            # Processar cada diretório
            for idx, item in enumerate(directories, 1):
                parts = item.split()
                if len(parts) >= 9:
                    name = " ".join(parts[8:])
                    
                    if item.startswith('d'):
                        try:
                            folder_path = f"{base_path}/{name}"
                            print(f"\n📁 Processando diretório ({idx}/{total_dirs}): {folder_path}")
                            ftps.cwd(folder_path)
                            
                            # Listar arquivos no subdiretório
                            files = []
                            def append_file(line):
                                files.append(line)
                            ftps.dir(append_file)
                            
                            for file_item in files:
                                if file_item.startswith('-'):  # É um arquivo
                                    file_parts = file_item.split()
                                    if len(file_parts) >= 9:
                                        file_name = " ".join(file_parts[8:])
                                        if file_name.endswith(extension):
                                            try:
                                                # Obter data de modificação do arquivo
                                                file_date = datetime.strptime(" ".join(file_parts[5:8]), "%b %d %H:%M")
                                                # Ajustar o ano se necessário
                                                if file_date.month > datetime.now().month:
                                                    file_date = file_date.replace(year=datetime.now().year - 1)
                                                else:
                                                    file_date = file_date.replace(year=datetime.now().year)
                                                
                                                if data_inicio <= file_date <= data_fim:
                                                    full_path = f"{folder_path}/{file_name}"
                                                    found_files.append((full_path, file_date.strftime('%Y%m%d %H:%M:%S')))
                                                    print(f"📄 Arquivo encontrado: {full_path}")
                                            except ValueError as e:
                                                print(f"⚠️ Erro ao processar data do arquivo {file_name}: {e}")
                                                continue
                            
                            ftps.cwd(base_path)
                            
                        except Exception as e:
                            print(f"❌ Erro ao processar diretório {name}: {str(e)}")
                            continue
            
            ftps.quit()
            print(f"\n✅ Total de arquivos encontrados: {len(found_files)}")
            return found_files
                
        except Exception as e:
            print(f"❌ Erro durante a conexão FTPS: {str(e)}")
            raise

def send_query_to_druid(query: str, retries: int = 3, delay: int = 5):
    """Envia consulta para o Druid"""
    # Garantir que a URL tenha o protocolo
    url = "https://druid.apps.ocp-01.tdigital-vivo.com.br/druid/v2/sql"
    if not url.startswith(('http://', 'https://')):
        url = 'https://' + url
    
    print(f"🐲 Consulta que será executada: {query}")
      
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
    
    attempt = 0
    while attempt < retries:
        try:
            response = requests.request("POST", url, headers=headers, data=payload)
            response.raise_for_status()
            return response.json()[1:]  # Ignora o cabeçalho e retorna os resultados
        except requests.exceptions.RequestException as e:
            print(f"❌ Erro na tentativa {attempt + 1}: {e}")
            attempt += 1
            if attempt < retries:
                print(f"❌🕐 Tentando novamente em {delay} segundos...")
                time.sleep(delay)
            else:
                print("❌ Número máximo de tentativas atingido. Falha ao executar a consulta.")
                raise

def consultar_druid_dados_periodo(data_inicio_range: datetime, data_fim_range: datetime, vendor_name: str):
    """Consulta dados no Druid para o período especificado"""
    query = f'''
        SELECT DISTINCT "managerFilename"
        FROM "druid"."fastoss-pm-enriched-metrics"
        WHERE "__time" >= '{data_inicio_range.strftime('%Y-%m-%d %H:%M:%S')}'
        AND "__time" < '{data_fim_range.strftime('%Y-%m-%d %H:%M:%S')}'
        AND "sourceVendor" = '{vendor_name}'
        GROUP BY "managerFilename"
    '''
    return send_query_to_druid(query)

def verificar_arquivos_faltantes_druid(arquivos_faltantes: List[tuple], vendor_name: str):
    """Verifica quais arquivos realmente estão faltando no Druid"""
    arquivos_realmente_faltantes = []
    
    # Data base (2 dias antes da data atual) começando às 00:00:00
    data_atual = datetime.now()
    data_base = datetime(data_atual.year, data_atual.month, data_atual.day, 0, 0, 0) - timedelta(days=2)
    
    # Range estendido: 2 dias antes e 2 dias depois da data base
    data_inicio_range = (data_base - timedelta(days=2)).replace(hour=0, minute=0, second=0)  # D-4
    data_fim_range = (data_base + timedelta(days=2)).replace(hour=0, minute=0, second=0)     # D

    print(f"Data base: {data_base.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"Range de consulta no Druid:")
    print(f"Início do range: {data_inicio_range.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"Fim do range: {data_fim_range.strftime('%d/%m/%Y %H:%M:%S')}")

    # Processar em lotes de 30 arquivos
    for i in range(0, len(arquivos_faltantes), 30):
        lote = arquivos_faltantes[i:i + 30]
        enderecos_faltantes = [arquivo[0] for arquivo in lote]
        arquivos_faltantes_str = "', '".join(enderecos_faltantes)
        
        query = f'''
            SELECT DISTINCT
                "managerFilename"
            FROM "druid"."fastoss-pm-enriched-metrics"
            WHERE 
                "__time" >= '{data_inicio_range.strftime('%Y-%m-%d %H:%M:%S')}'
                AND "__time" < '{data_fim_range.strftime('%Y-%m-%d %H:%M:%S')}'
                AND "managerFilename" IN ('{arquivos_faltantes_str}')
                AND "sourceVendor" = '{vendor_name}'
        '''

        result = send_query_to_druid(query)
        arquivos_encontrados_druid = {item['managerFilename'] for item in result}

        # Verificar quais arquivos do lote não foram encontrados no Druid
        for arquivo in lote:
            if arquivo[0] not in arquivos_encontrados_druid:
                arquivos_realmente_faltantes.append(arquivo)

    print(f"🔴 Total de arquivos realmente faltantes no Druid: {len(arquivos_realmente_faltantes)}")
    return arquivos_realmente_faltantes

def get_data_base(**context):
    """Determina a data base para processamento (sempre D-2)"""
    try:
        # Mesma lógica do script local
        data_atual = datetime.now()
        data_base = datetime(data_atual.year, data_atual.month, data_atual.day) - timedelta(days=2)
        
        print(f"\n🕒 Período de busca:")
        print(f"Data base (D-2): {data_base.strftime('%d/%m/%Y %H:%M:%S')}")
        print(f"Início: {data_base.strftime('%d/%m/%Y %H:%M:%S')}")
        print(f"Fim: {(data_base + timedelta(days=1)).strftime('%d/%m/%Y %H:%M:%S')}")
        
        return data_base
    except Exception as e:
        print(f"❌ Erro ao determinar data base: {e}")
        raise

def list_sftp_files(**context):
    """Task 1: Lista arquivos do SFTP"""
    try:
        print("\n=== Iniciando list_sftp_files ===")
        host = "187.100.113.57"
        username = "nftpuser"
        password = "Changeme_123"
        port = 21805
        base_path = "/neteco/dld/v3/signal/value"
        extension = ".zip"
        
        # Definir período de busca (D-2)
        data_atual = datetime.now()
        data_base = datetime(data_atual.year, data_atual.month, data_atual.day) - timedelta(days=2)
        data_inicio = data_base
        data_fim = data_base + timedelta(days=1)
        
        print(f"\n🕒 Período de busca:")
        print(f"Data base (D-2): {data_base.strftime('%d/%m/%Y %H:%M:%S')}")
        print(f"Início: {data_inicio.strftime('%d/%m/%Y %H:%M:%S')}")
        print(f"Fim: {data_fim.strftime('%d/%m/%Y %H:%M:%S')}")
        
        client = FTPSClient(host, username, password, port)
        
        print("\n📂 Buscando arquivos no servidor FTPS...")
        arquivos = client.list_files_in_directories_period(base_path, extension, data_inicio, data_fim)
        
        context['task_instance'].xcom_push(key='arquivos_sftp', value=arquivos)
        context['task_instance'].xcom_push(key='data_base', value=data_base.strftime('%Y-%m-%d %H:%M:%S'))
        context['task_instance'].xcom_push(key='vendor_name', value="Huawei")
        
        print(f"📁 Total de arquivos encontrados no SFTP: {len(arquivos)}")
        return len(arquivos)
        
    except Exception as e:
        print(f"\n❌ Erro durante a execução: {str(e)}")
        raise

def list_druid_files(**context):
    """Task 2: Lista arquivos no Druid"""
    # Usar a mesma lógica de datas do script local
    data_atual = datetime.now()
    data_base = datetime(data_atual.year, data_atual.month, data_atual.day) - timedelta(days=2)
    vendor_name = context['task_instance'].xcom_pull(key='vendor_name', task_ids='list_sftp')
    
    data_inicio_range = data_base - timedelta(days=2)
    data_fim_range = data_base + timedelta(days=2)
    
    print("\n🔍 Verificando arquivos no Druid...")
    print(f"Range de consulta: {data_inicio_range.strftime('%d/%m/%Y %H:%M:%S')} até {data_fim_range.strftime('%d/%m/%Y %H:%M:%S')}")
    
    df_druid = consultar_druid_dados_periodo(data_inicio_range, data_fim_range, vendor_name)
    arquivos_druid = [item['managerFilename'] for item in df_druid]
    
    context['task_instance'].xcom_push(key='arquivos_druid', value=arquivos_druid)
    print(f"📊 Total de arquivos encontrados no Druid: {len(arquivos_druid)}")
    return len(arquivos_druid)

def double_check_druid(**context):
    """Task 3: Verificação detalhada no Druid"""
    print("\n🔍 Iniciando verificação detalhada no Druid")
    
    arquivos_sftp = context['task_instance'].xcom_pull(key='arquivos_sftp', task_ids='list_sftp')
    arquivos_druid = context['task_instance'].xcom_pull(key='arquivos_druid', task_ids='list_druid')
    vendor_name = context['task_instance'].xcom_pull(key='vendor_name', task_ids='list_sftp')
    
    if not arquivos_sftp:
        print("\n⚠️ Nenhum arquivo encontrado no SFTP")
        return 0
    
    # Converter lista de arquivos do Druid para set para busca mais rápida
    arquivos_druid_set = set(arquivos_druid)
    
    # Identificar arquivos faltantes
    arquivos_faltantes = [arquivo for arquivo in arquivos_sftp if arquivo[0] not in arquivos_druid_set]
    print(f"🔴 Arquivos faltantes iniciais: {len(arquivos_faltantes)}")
    
    if not arquivos_faltantes:
        print("✅ Todos os arquivos já estão no Druid")
        return 0
    
    # Processar em lotes maiores
    TAMANHO_LOTE = 100  # Aumentado de 30 para 100
    total_lotes = (len(arquivos_faltantes) + TAMANHO_LOTE - 1) // TAMANHO_LOTE
    
    print(f"\n📦 Processando {total_lotes} lotes de {TAMANHO_LOTE} arquivos cada...")
    
    arquivos_confirmados = []
    for i in range(0, len(arquivos_faltantes), TAMANHO_LOTE):
        lote = arquivos_faltantes[i:i + TAMANHO_LOTE]
        print(f"Processando lote {(i // TAMANHO_LOTE) + 1}/{total_lotes}")
        
        lote_confirmado = verificar_arquivos_faltantes_druid(lote, vendor_name)
        arquivos_confirmados.extend(lote_confirmado)
        
        # Adicionar um pequeno delay para evitar sobrecarga
        time.sleep(0.1)
    
    context['task_instance'].xcom_push(key='arquivos_faltantes', value=arquivos_confirmados)
    print(f"\n🔴 Total de arquivos confirmados como faltantes: {len(arquivos_confirmados)}")
    
    return len(arquivos_confirmados)

def create_kafka_connection():
    """Cria conexão com o Kafka usando certificado SSL"""
    if AMBIENTE == 'prod':
        pem_content = Variable.get("pem_content")
    else:
        pem_content = Variable.get("pem_content_dev")

    with tempfile.NamedTemporaryFile(delete=False) as temp_file:
        temp_file.write(pem_content.encode())

    ssl_location = temp_file.name
    
    # Configurações do Kafka ajustadas para melhor performance
    conf = {
        "bootstrap.servers": f"{KAFKA_URL}:{KAFKA_PORT}",
        "security.protocol": "SASL_SSL",
        "sasl.mechanism": "SCRAM-SHA-512",
        "sasl.username": "flink",
        "sasl.password": "NMoW680oSNitVesBQti9jlsjl7GC8u36",
        'ssl.ca.location': ssl_location,
        'message.max.bytes': '1000000000',
        'batch.num.messages': 10000,  # Reduzido para evitar timeouts
        'linger.ms': 100,  # Aumentado para dar mais tempo para o batch
        'compression.type': 'lz4',
        'queue.buffering.max.messages': 100000,  # Reduzido
        'queue.buffering.max.kbytes': 1048576,  # Reduzido
        'max.in.flight.requests.per.connection': 5,
        'queue.buffering.max.ms': 1000,  # Aumentado
        'message.send.max.retries': 10,  # Aumentado
        'retry.backoff.ms': 100,  # Reduzido
        'request.timeout.ms': 30000,  # 30 segundos
        'message.timeout.ms': 60000,  # 1 minuto
        'delivery.timeout.ms': 120000,  # 2 minutos
        'enable.idempotence': True,  # Garante entrega única
        'acks': 'all'  # Garante que todos os replicas receberam a mensagem
    }
  
    # Carregar certificados confiáveis no contexto SSL
    ssl_context = ssl.create_default_context()
    ssl_context.load_verify_locations(conf['ssl.ca.location'])

    producer = Producer(**conf)
    return producer

def publish_message_with_key(value: str, topic: str, key: str, producer: Producer) -> bool:
    """Publica mensagem no Kafka com uma chave específica"""
    try:
        producer.produce(
            topic=topic,
            key=key.encode('utf-8'),
            value=value.encode('utf-8'),
            callback=delivery_callback
        )
        return True
    except Exception as e:
        print(f"❌ Erro ao enviar mensagem para o Kafka: {e}")
        return False

def publish_kafka(**context):
    """Task 4: Publica arquivos faltantes no Kafka"""
    arquivos_faltantes = context['task_instance'].xcom_pull(key='arquivos_faltantes', task_ids='double_check_druid')
    vendor_name = context['task_instance'].xcom_pull(key='vendor_name', task_ids='list_sftp')
    host = "187.100.113.57"
    
    if not arquivos_faltantes:
        print("\n✅ Nenhum arquivo para publicar no Kafka")
        return 0
    
    print("\n📨 Iniciando publicação no Kafka")
    producer = create_kafka_connection()
    sucessos = 0
    
    try:
        for arquivo_path, data_mod in arquivos_faltantes:
            key = f"{vendor_name}.NETECO.{host}"
            
            if publish_message_with_key(
                value=arquivo_path,
                topic="reproc-nifi",
                key=key,
                producer=producer
            ):
                sucessos += 1
                # Flush a cada mensagem para garantir o envio
                producer.poll(0)
        
        # Flush final para garantir que todas as mensagens foram enviadas
        producer.flush()
        print(f"\n📩 Total de arquivos enviados ao Kafka: {sucessos}")
        
        if sucessos == 0:
            raise Exception("Nenhuma mensagem foi enviada com sucesso")
            
        return sucessos
        
    except Exception as e:
        print(f"❌ Erro durante o envio das mensagens: {e}")
        raise
    finally:
        producer.flush()

# Criação da DAG
with DAG(
    'Reprocessamento_Neteco',
    default_args=default_args,
    description='DAG para reprocessamento de arquivos Neteco',
    schedule_interval='30 21 * * *',  # Executa às 21:30 todos os dias
    start_date=datetime(2023, 12, 1),  # Data no passado mais distante
    catchup=False,
    max_active_runs=1,
    tags=['reprocessamento', 'dynamic'],
) as dag:
    
    task_list_sftp = PythonOperator(
        task_id='list_sftp',
        python_callable=list_sftp_files,
        provide_context=True,
        execution_timeout=timedelta(hours=1)
    )
    
    task_list_druid = PythonOperator(
        task_id='list_druid',
        python_callable=list_druid_files,
        provide_context=True,
        execution_timeout=timedelta(hours=1)
    )
    
    task_double_check = PythonOperator(
        task_id='double_check_druid',
        python_callable=double_check_druid,
        provide_context=True,
        execution_timeout=timedelta(hours=4)
    )
    
    task_publish_kafka = PythonOperator(
        task_id='publish_kafka',
        python_callable=publish_kafka,
        provide_context=True,
        execution_timeout=timedelta(hours=1)
    )

    # Fluxo de execução
    task_list_sftp >> task_list_druid >> task_double_check >> task_publish_kafka
