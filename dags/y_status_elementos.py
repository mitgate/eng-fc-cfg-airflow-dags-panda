# Nome da DAG: y_status_elementos
# Owner / responsável: Sadir
# Descrição do objetivo da DAG: DAG status elementos com cache Redis.
# Usa Druid?: Não
# Principais tabelas / consultas Druid acessadas: elements_status
# Frequência de execução (schedule): 
# Dag Activo?: 
# Autor: Sadir
# Data de modificação: 2025-05-26

# V0

import json
import tempfile
import ssl
import time
import logging
from datetime import datetime, timedelta
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from hooks.kafka_connector import KafkaConnector
#from airflow.hooks.postgres_hook import PostgresHook
from hooks.postgres_hook import PostgresHook
from redis import Redis
from threading import Lock

from airflow.hooks.base import BaseHook

# Configurar logger para desabilitar logs INFO do Airflow base
logging.getLogger('airflow.models.baseoperator').setLevel(logging.WARNING)

# Configurar logger para desabilitar logs INFO do PostgresHook
logging.getLogger('airflow.hooks.postgres_hook').setLevel(logging.WARNING)

# Variáveis do Kafka
ORIGIN_TOPIC = 'elements_status_topic'
CG_ID = 'airflow_local_poc_counters'
POSTGRES_CONN_ID = 'postgres_element_status'

# Obter variáveis do Airflow
MAX_PROCESSING_TIME = int(Variable.get('status_elementos_gestor_max_processing_time', default_var=300))  # 5 minutos em segundos
TIMEOUT_CONSUMER = int(Variable.get('status_elementos_gestor_timeout_consumer', default_var=298))  # Timeout para o consumer em segundos
BATCH_SIZE = int(Variable.get('status_elementos_gestor_batch_size', default_var=1000))  # Tamanho do lote para processamento em batch
NUM_PARALLEL_TASKS = int(Variable.get('status_elementos_gestor_num_parallel_tasks', default_var=1))  # Número de tasks paralelas
LOG_INTERVAL = int(Variable.get('status_elementos_gestor_log_interval', default_var=30))  # Intervalo em segundos para exibir o log
LOTE_REDIS = int(Variable.get('status_elementos_gestor_lote_redis', default_var=1000))  # Tamanho do lote para Redis

# Configurações do Redis
redis_conn = BaseHook.get_connection('redis_element_status')
REDIS_HOST = redis_conn.host
REDIS_PORT = redis_conn.port
REDIS_DB = redis_conn.schema
REDIS_USERNAME = redis_conn.login
REDIS_PASSWORD = redis_conn.password
REDIS_KEY_PREFIX = 'element-status:'
REDIS_CACHE_TTL = 86400  # 24 horas em segundos

# Variáveis globais para conexões
pg_hook = None
kafka_connector = None
redis_client = None

# Definir a DAG
dag = DAG(
    'poc_status_elementos_V5',
    default_args={'owner': 'Sadir', 'depends_on_past': False, 'retries': 0, 'retry_delay': timedelta(minutes=5)},
    description='DAG status elementos com cache Redis.',
    schedule_interval=None,
    start_date=datetime(2024, 9, 9),
    catchup=False,
    max_active_runs=1,
    tags=["kafka","status_elementos","redis"]
)

# Definir os operadores de início e fim
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

def initialize_connections():
    """Inicializa as conexões com o banco de dados, Kafka e Redis"""
    global pg_hook, kafka_connector, redis_client
    
    try:
        # Inicializar o hook PostgreSQL
        pg_hook = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
        print(f"✅ Hook PostgreSQL inicializado usando '{POSTGRES_CONN_ID}'")
        
        # Testar a conexão com PostgreSQL
        print("🔄 Testando conexão com PostgreSQL...")
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                if result and result[0] == 1:
                    print("✅ Conexão com PostgreSQL testada com sucesso")
                else:
                    raise Exception("Falha ao testar conexão com PostgreSQL")
        
        # Inicializar conexão com Redis
        print("🔄 Inicializando conexão com Redis...")
        redis_client = Redis(
            host=REDIS_HOST,
            port=REDIS_PORT,
            db=REDIS_DB,
            username=REDIS_USERNAME,
            password=REDIS_PASSWORD,
            decode_responses=True
        )
        # Testar conexão com Redis
        redis_client.ping()
        print("✅ Conexão com Redis estabelecida com sucesso")
        
        # Criar tabela se não existir
        create_table_if_not_exists(pg_hook)
        
        # Configurar conexão com Kafka
        kafka_connector = KafkaConnector(
        topic_var_name=ORIGIN_TOPIC,
        kafka_url_var_name="prod_kafka_url",
        kafka_port_var_name="prod_kafka_port",
        kafka_variable_pem_content="pem_content",
        kafka_connection_id="kafka_default"
    )
        print(f"✅ Configuração do Kafka concluída para o tópico '{ORIGIN_TOPIC}'")
        
    except Exception as e:
        print(f"❌ Erro ao inicializar conexões: {str(e)}")
        raise

def create_table_if_not_exists(pg_hook):
    """Cria a tabela de elementos se ela não existir"""
    create_table_query = """
    CREATE TABLE IF NOT EXISTS elements_status (
        created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        additional_dn VARCHAR(255),
        dn VARCHAR(255),
        source_system VARCHAR(100),
        vendor_name VARCHAR(100),
        manager_ip VARCHAR(100),
        element_type VARCHAR(100),
        ativo BOOLEAN DEFAULT TRUE,
        CONSTRAINT elements_status_dn_additional_dn_key UNIQUE (dn, additional_dn)
    );
    
    CREATE INDEX IF NOT EXISTS idx_elements_status_dn ON elements_status(dn);
    CREATE INDEX IF NOT EXISTS idx_elements_status_additional_dn ON elements_status(additional_dn);
    CREATE INDEX IF NOT EXISTS idx_elements_status_created_at ON elements_status(created_at);
    CREATE INDEX IF NOT EXISTS idx_elements_status_ativo ON elements_status(ativo);
    """
    
    create_procedure_query = """
    CREATE OR REPLACE FUNCTION update_element_status_active()
    RETURNS void AS $$
    BEGIN
        -- Atualiza registros inativos (mais de 1 dia)
        UPDATE elements_status
        SET ativo = FALSE
        WHERE created_at < CURRENT_TIMESTAMP - INTERVAL '1 day'
        AND ativo = TRUE;
        
        -- Log da quantidade de registros atualizados
        RAISE NOTICE 'Registros atualizados: %', FOUND;
    END;
    $$ LANGUAGE plpgsql;
    """
    
    try:
        print("🔄 Criando tabela e procedure se não existirem...")
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                # Criar tabela
                cursor.execute(create_table_query)
                
                # Criar procedure
                cursor.execute(create_procedure_query)
                
                conn.commit()
        print("✅ Tabela 'elements_status' e procedure 'update_element_status_active' verificadas/criadas com sucesso")
    except Exception as e:
        print(f"❌ Erro ao criar tabela/procedure: {str(e)}")
        raise

def get_cache_key(dn, additional_dn):
    """Gera a chave do cache para um elemento"""
    return f"{REDIS_KEY_PREFIX}{dn}:{additional_dn}"

def process_message(message):
    """Processa uma mensagem individual e faz upsert no Redis"""
    try:
        # Preparar o registro
        record = {
            'additional_dn': message.get('additionalDn','não_informado'),
            'dn': message.get('dn','não_informado'),
            'source_system': message.get('sourceSystem','não_informado'),
            'vendor_name': message.get('vendorName','não_informado'),
            'manager_ip': message.get('managerIp','não_informado'),
            'element_type': message.get('elementType','não_informado'),
            'ativo': True
        }
        
        # Verificar se todos os campos obrigatórios estão presentes
        required_fields = ['additional_dn', 'dn', 'source_system', 'vendor_name', 'manager_ip', 'element_type', 'ativo']
        missing_fields = [field for field in required_fields if record[field] == 'sem_' + field + '_informado']
        if missing_fields:
            print(f"⚠️ Mensagem com campos faltantes: {missing_fields}. Mensagem: {message}")
            return
        
        # Gerar chave do Redis
        key = get_cache_key(record['dn'], record['additional_dn'])
        
        # Fazer upsert no Redis
        redis_client.setex(key, REDIS_CACHE_TTL, json.dumps(record))
        
        # Verificar se o dado foi armazenado corretamente
        stored_data = redis_client.get(key)
        if not stored_data:
            print(f"⚠️ Erro ao verificar dado armazenado para a chave: {key}")
            return
            
        stored_record = json.loads(stored_data)
        if stored_record != record:
            print(f"⚠️ Dado armazenado diferente do original para a chave: {key}")
            print(f"Original: {record}")
            print(f"Armazenado: {stored_record}")
        
    except Exception as e:
        print(f"❌ Erro ao processar mensagem: {str(e)}")
        raise

def process_messages_task(task_id, **context):
    """Função para processar mensagens em uma task específica"""
    global kafka_connector, redis_client
    
    print(f"🔄 Iniciando task {task_id} para processamento de elementos em streaming.")
    
    print(f"⏳ Iniciando consumo de mensagens...")
    consumer = kafka_connector.create_kafka_connection('Consumer', CG_ID)
    print(f"✅ Consumer Kafka criado com ID {CG_ID}")
    
    # Variáveis para controle de estatísticas
    total_messages = 0
    start_time = time.time()
    last_log_time = start_time
    batch = []
    
    def process_batch():
        nonlocal batch
        if not batch:
            return
            
        try:
            for record in batch:
                process_message(record)
            batch = []
                    
        except Exception as e:
            print(f"❌ Erro ao processar batch: {str(e)}")
            for record in batch:
                try:
                    process_message(record)
                except Exception as msg_error:
                    print(f"❌ Erro ao processar mensagem individual: {str(msg_error)}")
            batch = []
    
    def message_callback(message):
        nonlocal total_messages, last_log_time, batch
        try:
            record = {
                'additionalDn': message.get('additionalDn','não_informado'),
                'dn': message.get('dn','não_informado'),
                'sourceSystem': message.get('sourceSystem','não_informado'),
                'vendorName': message.get('vendorName','não_informado'),
                'managerIp': message.get('managerIp','não_informado'),
                'elementType': message.get('elementType','não_informado')
            }
            
            batch.append(record)
            total_messages += 1
            
            if len(batch) >= BATCH_SIZE:
                process_batch()
            
            current_time = time.time()
            if current_time - last_log_time >= LOG_INTERVAL:
                elapsed_time = current_time - start_time
                rate = total_messages / elapsed_time if elapsed_time > 0 else 0
                print(f"📊 Estatísticas de processamento:")
                print(f"   - Total de mensagens processadas: {total_messages}")
                print(f"   - Taxa de processamento: {rate:.2f} mensagens/segundo")
                print(f"   - Tempo decorrido: {elapsed_time:.2f} segundos")
                print(f"   - Tamanho do batch atual: {len(batch)}")
                last_log_time = current_time
                
        except Exception as e:
            print(f"❌ Erro no callback de mensagem: {str(e)}")
            raise
    
    try:
        stats = kafka_connector.process_messages_streaming(
            consumer=consumer,
            callback_function=message_callback,
            max_processing_time=MAX_PROCESSING_TIME,
            timeout=TIMEOUT_CONSUMER,
            max_empty_attempts=MAX_EMPTY_ATTEMPTS
        )
        
        if batch:
            process_batch()
        
        total_time = time.time() - start_time
        final_rate = total_messages / total_time if total_time > 0 else 0
        
        print(f"✅ Task {task_id} concluída com sucesso!")
        print(f"📊 Estatísticas finais:")
        print(f"   - Total de mensagens processadas: {total_messages}")
        print(f"   - Tempo total de processamento: {total_time:.2f} segundos")
        print(f"   - Taxa média de processamento: {final_rate:.2f} mensagens/segundo")
        
        return True
        
    except Exception as e:
        print(f"❌ Erro durante o processamento: {str(e)}")
        raise
    finally:
        try:
            if consumer:
                consumer.close()
                print("✅ Consumer Kafka fechado com sucesso")
        except Exception as e:
            print(f"⚠️ Erro ao fechar consumer Kafka: {str(e)}")

def sync_redis_to_postgres(**context):
    """Sincroniza dados do Redis para o PostgreSQL"""
    global pg_hook, redis_client
    
    if not redis_client:
        print("❌ Cliente Redis não inicializado.")
        raise ValueError("Cliente Redis não disponível para sincronização.")
        
    if not pg_hook:
        print("❌ Hook PostgreSQL não inicializado.")
        raise ValueError("Hook PostgreSQL não disponível para sincronização.")

    print("🔄 Iniciando sincronização Redis -> PostgreSQL...")
    
    synced_count = 0
    error_count = 0
    skipped_count = 0
    
    try:
        # Obter todas as chaves do Redis com o prefixo definido
        keys = redis_client.keys(f"{REDIS_KEY_PREFIX}*")
        total_keys = len(keys)
        print(f"🔍 Encontradas {total_keys} chaves no Redis para potencial sincronização.")
        
        if total_keys == 0:
             print("✅ Nenhuma chave no Redis para sincronizar.")
             return
        
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                # Verificar se a tabela existe e está vazia
                cursor.execute("SELECT COUNT(*) FROM elements_status")
                count_before = cursor.fetchone()[0]
                print(f"📊 Quantidade de registros na tabela antes da sincronização: {count_before}")
                
                upsert_query = """
                    INSERT INTO elements_status 
                        (additional_dn, dn, source_system, vendor_name, manager_ip, element_type, ativo, created_at)
                    VALUES 
                        (%(additional_dn)s, %(dn)s, %(source_system)s, %(vendor_name)s, %(manager_ip)s, %(element_type)s, %(ativo)s, CURRENT_TIMESTAMP)
                    ON CONFLICT (dn, additional_dn) DO UPDATE SET
                        source_system = EXCLUDED.source_system,
                        vendor_name = EXCLUDED.vendor_name,
                        manager_ip = EXCLUDED.manager_ip,
                        element_type = EXCLUDED.element_type,
                        ativo = EXCLUDED.ativo,
                        created_at = CURRENT_TIMESTAMP;
                """
                
                for i in range(0, total_keys, LOTE_REDIS):
                    batch_keys = keys[i:i + LOTE_REDIS]
                    print(f"🔄 Processando lote {i // LOTE_REDIS + 1}/{(total_keys + LOTE_REDIS - 1) // LOTE_REDIS} de chaves Redis...")
                    
                    for key in batch_keys:
                        try:
                            data_str = redis_client.get(key)
                            if not data_str:
                                print(f"⚠️ Chave {key} sem dados no Redis no momento da leitura. Pulando.")
                                skipped_count += 1
                                continue
                                
                            record = json.loads(data_str)
                            
                            # Verificar se o registro tem todos os campos necessários
                            required_fields = ['additional_dn', 'dn', 'source_system', 'vendor_name', 'manager_ip', 'element_type', 'ativo']
                            missing_fields = [field for field in required_fields if field not in record]
                            if missing_fields:
                                print(f"⚠️ Registro com campos faltantes: {missing_fields}. Registro: {record}")
                                error_count += 1
                                continue
                            
                            # Executar upsert no PostgreSQL
                            cursor.execute(upsert_query, record)
                            synced_count += 1
                            
                            # Log a cada 1000 registros para acompanhar o progresso
                            if synced_count % LOTE_REDIS == 0:
                                print(f"📊 Progresso: {synced_count} registros sincronizados até agora")
                            
                        except json.JSONDecodeError as json_err:
                            print(f"❌ Erro ao decodificar JSON para a chave {key}: {str(json_err)}")
                            error_count += 1
                        except Exception as e:
                            print(f"❌ Erro ao processar chave {key} no DB: {str(e)}")
                            error_count += 1
                            #conn.rollback()
                            raise
                
                # Commit final após processar todos os lotes
                conn.commit()
                print(f"✅ Commit realizado com sucesso")
                
                # Verificar quantidade de registros após a sincronização
                cursor.execute("SELECT COUNT(*) FROM elements_status")
                count_after = cursor.fetchone()[0]
                print(f"📊 Quantidade de registros na tabela após a sincronização: {count_after}")
                print(f"📊 Registros adicionados/atualizados: {count_after - count_before}")
                
        print(f"✅ Sincronização concluída: {synced_count} registros sincronizados/atualizados no DB, {skipped_count} pulados, {error_count} erros.")
        if error_count > 0:
             raise Exception(f"{error_count} erros ocorreram durante a sincronização.")

    except Exception as e:
        print(f"❌ Erro geral durante a sincronização Redis -> PostgreSQL: {str(e)}")
        raise

# Inicializar conexões antes de criar as tasks
initialize_connections()

# Criar as tasks paralelas de processamento
tasks = []
for i in range(NUM_PARALLEL_TASKS):
    task_id = f'process_elements_{i+1}'
    task = PythonOperator(
        task_id=task_id,
        python_callable=process_messages_task,
        op_kwargs={'task_id': task_id},
        dag=dag
    )
    tasks.append(task)

# Criar a task de sincronização Redis -> Postgres
sync_redis_postgres = PythonOperator(
    task_id='sync_redis_postgres',
    python_callable=sync_redis_to_postgres,
    dag=dag,
    trigger_rule='all_done'  # Garante que a task será executada mesmo se alguma task anterior falhar
)

# Criar task de verificação de dados
check_data = DummyOperator(
    task_id='check_data',
    dag=dag
)

# Definir as dependências
start >> tasks >> check_data >> sync_redis_postgres >> end

