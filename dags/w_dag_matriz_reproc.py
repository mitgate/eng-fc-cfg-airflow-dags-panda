# Nome da DAG: w_dag_matriz_reproc
# Owner / responsável: Leandro
# Descrição do objetivo da DAG: # Função para verificar arquivos faltantes no Druid def verificar_arquivos_faltantes_druid(arquivos_faltantes, vendor_name):      arquivos_realmente_faltantes = []      enderecos_faltantes = [sublista[0] for sublista in arquivos_faltantes]      arquivos_faltantes_str = "', '".join(enderecos_faltantes)           query = f'''              SELECT DISTINCT "managerFilename"              FROM "druid"."fastoss-pm-enriched-metrics"              WHERE "managerFilename" IN ('{arquivos_faltantes_str}')              AND "sourceVendor" = '{vendor_name}'              AND __time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY          '''       result = send_query_to_druid(query)      arquivos_encontrados_druid = {item['managerFilename'] for item in result}           for arquivo_faltante in arquivos_faltantes:          if arquivo_faltante[0] not in arquivos_encontrados_druid:              arquivos_realmente_faltantes.append(arquivo_faltante)       print(f"🔴 Total de arquivos realmente faltantes no Druid: {len(arquivos_realmente_faltantes)}")      return arquivos_realmente_faltantes
# Usa Druid?: Sim
# Principais tabelas / consultas Druid acessadas: druid
# Frequência de execução (schedule): 
# Dag Activo?: 
# Autor: Leandro
# Data de modificação: 2025-05-26

# Start v5
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Connection
from airflow.exceptions import AirflowSkipException
from airflow.models import Variable
from airflow.utils.dates import datetime, timedelta
import paramiko
import pandas as pd
import os
import json
import tempfile
from confluent_kafka import Producer
import ssl
import requests
import time
import stat
from airflow.decorators import dag, task

# Variáveis e constantes
RETRY_DRUID = int(Variable.get('aggregation_retry_druid', 3))
DELAY_RETRY_DRUID = int(Variable.get('aggregation_retry_delay_druid', 5))
AMBIENTE = Variable.get('reproc_ericsson_ambiente', 'dev')
KAFKA_URL = Variable.get('aggregation_kafka_url')
KAFKA_PORT = Variable.get('aggregation_kafka_port')

connection_id = f'w_r_matriz_reprocessamento'  
connection_matriz = Connection.get_connection_from_secrets(connection_id)
matriz  = connection_matriz.extra_dejson

#=================================================================================================

# Callback error do Kafka
def delivery_callback(err, msg):
    if err:
        print('❌ Error al enviar mensaje: %s' % err)

# Função para enviar consulta para o Druid
def send_query_to_druid(query, retries=RETRY_DRUID, delay=DELAY_RETRY_DRUID):
    url = "https://druid.apps.ocp-01.tdigital-vivo.com.br/druid/v2/sql"
    print(f"🐲Consulta que será executada: {query}")
      
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

# Função para criar a conexão com o Kafka
def create_kafka_connection():
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
  
    # Carregar certificados confiáveis no contexto SSL
    ssl_context = ssl.create_default_context()
    ssl_context.load_verify_locations(conf['ssl.ca.location'])

    producer = Producer(**conf)
    return producer

# Função para enviar uma mensagem simples para o Kafka com informações de grupo
def publish_message_with_key(message, topic, key, producer):
    try:
        # Produz a mensagem para o tópico com a chave de particionamento
        producer.produce(topic, key=key, value=message.encode('utf-8'), callback=delivery_callback)
        # Garante que a mensagem seja enviada imediatamente
    except Exception as e:
        print(f"Erro ao enviar mensagem para o Kafka: {e}")

# Função para listar recursivamente arquivos em um diretório remoto
def listar_arquivos_recursivamente_periodo(sftp, path, data_inicio, data_fim):
    file_list = []
    try:
        print(f"\n🔍 Explorando diretório: {path}")
        items = sftp.listdir_attr(path)
        total_items = len(items)
        print(f"📂 Total de itens encontrados: {total_items}")
        
        for idx, item in enumerate(items, 1):
            full_path = f"{path.rstrip('/')}/{item.filename}".replace("//", "/")
            file_mtime = datetime.fromtimestamp(item.st_mtime)
            
            if stat.S_ISDIR(item.st_mode):
                print(f"📁 Entrando no subdiretório ({idx}/{total_items}): {item.filename}")
                file_list.extend(listar_arquivos_recursivamente_periodo(sftp, full_path, data_inicio, data_fim))
            elif data_inicio <= file_mtime <= data_fim:
                print(f"📄 Arquivo encontrado ({idx}/{total_items}): {item.filename}")
                file_list.append((full_path, file_mtime.strftime('%Y%m%d %H:%M:%S')))
            
            if idx % 100 == 0:  # Log a cada 100 itens
                print(f"⏳ Progresso: {idx}/{total_items} itens processados")
                
    except IOError as e:
        print(f"❌ Erro ao acessar o diretório {path}: {e}")
    except Exception as e:
        print(f"❌ Erro inesperado ao processar {path}: {e}")
    
    print(f"✅ Finalizado diretório {path}: {len(file_list)} arquivos encontrados")
    return file_list

# Função para gerar arquivo JSON em caso de erro de conexão
def gerar_json_erro(host, erro, producer, topic_error):
    data_atual = datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    erro_data = {
        'host': host,
        'erro': str(erro),
        'data': data_atual
    }
    erro_json = json.dumps(erro_data)
    publish_message_with_key(erro_json, topic_error, '*', producer)

# Função para consultar o Druid
def consultar_druid_dados_periodo(data_inicio_range, data_fim_range, vendor_name):
    query = f'''
            SELECT DISTINCT "managerFilename"
            FROM "druid"."fastoss-pm-enriched-metrics"
            WHERE "__time" >= '{data_inicio_range.strftime('%Y-%m-%d %H:%M:%S')}'
            AND "__time" < '{data_fim_range.strftime('%Y-%m-%d %H:%M:%S')}'
            AND "sourceVendor" = '{vendor_name}'
            GROUP BY "managerFilename"
        '''
    return send_query_to_druid(query)

"""# Função para verificar arquivos faltantes no Druid
def verificar_arquivos_faltantes_druid(arquivos_faltantes, vendor_name):
    arquivos_realmente_faltantes = []
    enderecos_faltantes = [sublista[0] for sublista in arquivos_faltantes]
    arquivos_faltantes_str = "', '".join(enderecos_faltantes)
    
    query = f'''
            SELECT DISTINCT "managerFilename"
            FROM "druid"."fastoss-pm-enriched-metrics"
            WHERE "managerFilename" IN ('{arquivos_faltantes_str}')
            AND "sourceVendor" = '{vendor_name}'
            AND __time >= CURRENT_TIMESTAMP - INTERVAL '7' DAY
        '''

    result = send_query_to_druid(query)
    arquivos_encontrados_druid = {item['managerFilename'] for item in result}
    
    for arquivo_faltante in arquivos_faltantes:
        if arquivo_faltante[0] not in arquivos_encontrados_druid:
            arquivos_realmente_faltantes.append(arquivo_faltante)

    print(f"🔴 Total de arquivos realmente faltantes no Druid: {len(arquivos_realmente_faltantes)}")
    return arquivos_realmente_faltantes
"""

# Função para verificar arquivos faltantes no Druid em lotes
def verificar_arquivos_faltantes_druid(arquivos_faltantes, vendor_name):
    arquivos_realmente_faltantes = []
    arquivos_encontrados_total = []

    # Data base (2 dias antes da data atual) começando às 00:00:00
    data_atual = datetime.now()
    data_base = datetime(data_atual.year, data_atual.month, data_atual.day, 0, 0, 0) - timedelta(days=2)
    
    # Range estendido: 2 dias antes e 2 dias depois da data base, sempre começando às 00:00:00
    data_inicio_range = (data_base - timedelta(days=2)).replace(hour=0, minute=0, second=0)  # D-4
    data_fim_range = (data_base + timedelta(days=2)).replace(hour=0, minute=0, second=0)     # D

    print(f"Data base: {data_base.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"Range de consulta no Druid:")
    print(f"Início do range: {data_inicio_range.strftime('%d/%m/%Y %H:%M:%S')}")
    print(f"Fim do range: {data_fim_range.strftime('%d/%m/%Y %H:%M:%S')}")

    # Dividir a lista arquivos_faltantes em lotes de 30
    for i in range(0, len(arquivos_faltantes), 30):
        lote = arquivos_faltantes[i:i + 30]
        enderecos_faltantes = [sublista[0] for sublista in lote]
        arquivos_faltantes_str = "', '".join(enderecos_faltantes)
        
        # Consulta Druid para verificar os arquivos no lote com range de datas
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

        # Armazenar os resultados encontrados no array total
        arquivos_encontrados_total.append(result)

        # Verificar quais arquivos do lote não foram encontrados no Druid
        for arquivo_faltante in lote:
            if arquivo_faltante[0] not in arquivos_encontrados_druid:
                arquivos_realmente_faltantes.append(arquivo_faltante)

    print(f"🔴 Total de arquivos realmente faltantes no Druid: {len(arquivos_realmente_faltantes)}")
    return arquivos_realmente_faltantes

def create_dag_for_vendor(key, vendor_data):
    vendor_name = vendor_data['vendor_name']
    topic_success = vendor_data['topic_success']
    topic_error = vendor_data['topic_error']
    agendamento = vendor_data['agendamento']
    
    connection_id = f'w_r_{key}'
    connection = Connection.get_connection_from_secrets(connection_id)
    SERVIDORES = connection.extra_dejson
    
    default_args = {
        'owner': 'Leandro',
        'depends_on_past': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    }

    @dag(
        dag_id=f'Reprocessamento_dynamic_{key}',
        default_args=default_args,
        description=f'DAG para reprocessar dados do fornecedor {vendor_name}.',
        schedule_interval=agendamento,
        start_date=datetime(2024, 9, 9),
        catchup=False,
        max_active_runs=1,
        tags=['reprocessamento', 'dynamic'],
    )
    def dynamic_generated_dag():
        @task
        def processar_arquivos_sftp():
            producer = create_kafka_connection()
            
            data_atual = datetime.now()
            data_base = datetime(data_atual.year, data_atual.month, data_atual.day, 0, 0, 0) - timedelta(days=2)
            data_inicio = data_base
            data_fim = data_base + timedelta(days=1)

            print(f"\n🕒 Período de busca:")
            print(f"Data base (D-2): {data_base.strftime('%d/%m/%Y %H:%M:%S')}")
            print(f"Início: {data_inicio.strftime('%d/%m/%Y %H:%M:%S')}")
            print(f"Fim: {data_fim.strftime('%d/%m/%Y %H:%M:%S')}")

            arquivos_consolidados = []
            total_servidores = len(SERVIDORES)
            
            print(f"\n📊 Total de servidores configurados: {total_servidores}")
            
            for idx, (host, info) in enumerate(SERVIDORES.items(), 1):
                print(f"\n💻 Processando servidor {idx}/{total_servidores}: {host}")
                print(f"👤 Usuário: {info['username']}")
                
                tipos_dirs = list(info['dirs'].keys())
                print(f"📂 Tipos de diretórios configurados: {tipos_dirs}")
                
                try:
                    ssh = paramiko.SSHClient()
                    ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
                    print(f"🔄 Conectando ao servidor {host}...")
                    
                    ssh.connect(
                        hostname=host,
                        username=info['username'],
                        password=info['password'],
                        port=22,
                        timeout=30
                    )
                    
                    sftp = ssh.open_sftp()
                    print(f"✅ Conexão estabelecida com {host}")

                    for tipo_dir, dir_info in info['dirs'].items():
                        print(f"\n📁 Processando diretório tipo: {tipo_dir}")
                        paths = dir_info['path']
                        vendor = dir_info['vendor']
                        
                        print(f"🏢 Vendor: {vendor}")
                        print(f"📍 Caminhos configurados: {paths}")

                        for remote_dir in paths:
                            print(f"\n🔍 Explorando: {remote_dir}")
                            try:
                                arquivos = listar_arquivos_recursivamente_periodo(
                                    sftp, remote_dir, data_inicio, data_fim
                                )
                                
                                novos_arquivos = [
                                    (arquivo, tipo_dir, vendor, host) 
                                    for arquivo, file_mtime in arquivos
                                ]
                                
                                arquivos_consolidados.extend(novos_arquivos)
                                print(f"✅ Encontrados {len(novos_arquivos)} arquivos em {remote_dir}")
                                
                            except Exception as e:
                                erro_msg = f"❌ Erro ao processar diretório {remote_dir}: {str(e)}"
                                print(erro_msg)
                                gerar_json_erro(host, f"Erro em {remote_dir}: {str(e)}", producer, topic_error)

                    sftp.close()
                    ssh.close()
                    print(f"✅ Conexão com {host} finalizada")
                    
                except Exception as e:
                    erro_msg = f"❌ Erro de conexão com {host}: {str(e)}"
                    print(erro_msg)
                    gerar_json_erro(host, str(e), producer, topic_error)

            # Resumo final
            print("\n📊 Resumo do Processamento:")
            print(f"Total de servidores processados: {total_servidores}")
            print(f"Total de arquivos encontrados: {len(arquivos_consolidados)}")
            
            if len(arquivos_consolidados) > 0:
                por_tipo = {}
                for arquivo in arquivos_consolidados:
                    tipo = arquivo[1]
                    if tipo not in por_tipo:
                        por_tipo[tipo] = 0
                    por_tipo[tipo] += 1
                    
                print("\nDistribuição por tipo:")
                for tipo, qtd in por_tipo.items():
                    print(f"- {tipo}: {qtd} arquivos")
            else:
                print("⚠️ Nenhum arquivo encontrado!")

            if not arquivos_consolidados:
                raise Exception("❌ Nenhuma conexão SFTP foi bem-sucedida")

            return arquivos_consolidados

        @task
        def comparar_com_druid(arquivos_consolidados):
            if len(arquivos_consolidados) > 0:
                print(f"Iniciando consulta ao Druid...")

                # Data atual
                data_atual = datetime.now()
                # Data base (D-2) começando às 00:00:00
                data_base = datetime(data_atual.year, data_atual.month, data_atual.day, 0, 0, 0) - timedelta(days=2)
                
                # Range estendido: 2 dias antes e 2 dias depois da data base, sempre começando às 00:00:00
                data_inicio_range = (data_base - timedelta(days=2)).replace(hour=0, minute=0, second=0)  # D-4
                data_fim_range = (data_base + timedelta(days=2)).replace(hour=0, minute=0, second=0)     # D
                
                print(f"Data base: {data_base.strftime('%d/%m/%Y %H:%M:%S')}")
                print(f"Range de consulta no Druid:")
                print(f"Início do range: {data_inicio_range.strftime('%d/%m/%Y %H:%M:%S')}")
                print(f"Fim do range: {data_fim_range.strftime('%d/%m/%Y %H:%M:%S')}")
                
                df_druid = consultar_druid_dados_periodo(data_inicio_range, data_fim_range, vendor_name)

                arquivos_druid = [item['managerFilename'] for item in df_druid]
                arquivos_faltantes_druid = [arquivo for arquivo in arquivos_consolidados if arquivo[0] not in arquivos_druid]
                print(f"🔴 Arquivos faltantes no Druid: {len(arquivos_faltantes_druid)}")
                return arquivos_faltantes_druid
            else:
                print(f"👌 Sem arquivos para processar!")
                raise AirflowSkipException
        @task
        def verificar_arquivos_faltantes(arquivos_faltantes_druid):
            arquivos_realmente_faltantes = verificar_arquivos_faltantes_druid(arquivos_faltantes_druid, vendor_name)
            return arquivos_realmente_faltantes

        @task
        def enviar_arquivos_kafka(arquivos_realmente_faltantes):
            arquivos_realmente_faltantes = arquivos_realmente_faltantes
            producer = create_kafka_connection()
            for arquivo_faltante in arquivos_realmente_faltantes:
                diretorio = arquivo_faltante[0]
                tipo_dir = arquivo_faltante[1]
                ip = arquivo_faltante[3]
                key_completa = f"{vendor_name}.{ip}.{tipo_dir}"
                #print(f"🗝 Log de chave: {key_completa}")
                publish_message_with_key(diretorio, topic_success, key_completa, producer)
            print(f"📩 Total de arquivos enviados ao Kafka: {len(arquivos_realmente_faltantes)}")
            producer.flush()

        arquivos_sftp = processar_arquivos_sftp()
        arquivos_faltantes_druid = comparar_com_druid(arquivos_sftp)
        arquivos_realmente_faltantes = verificar_arquivos_faltantes(arquivos_faltantes_druid)
        enviar_arquivos_kafka(arquivos_realmente_faltantes)

    return dynamic_generated_dag()

# Criação de DAGs dinâmicas
for key, vendor_data in matriz.items():
    dag_instance = create_dag_for_vendor(key, vendor_data)
    globals()[f"reproc_dynamic_{key}"] = dag_instance 
    
    
    