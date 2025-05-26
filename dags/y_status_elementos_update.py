# Nome da DAG: y_status_elementos_update
# Owner / responsável: Sadir
# Descrição do objetivo da DAG: DAG para atualização de status dos elementos.
# Usa Druid?: Não
# Principais tabelas / consultas Druid acessadas: 
# Frequência de execução (schedule): 
# Dag Activo?: 
# Autor: Sadir
# Data de modificação: 2025-05-26

import logging
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from hooks.postgres_hook import PostgresHook

# Configurar logger para desabilitar logs INFO do Airflow base
logging.getLogger('airflow.models.baseoperator').setLevel(logging.WARNING)

# Configurar logger para desabilitar logs INFO do PostgresHook
logging.getLogger('airflow.hooks.postgres_hook').setLevel(logging.WARNING)

# Variáveis do PostgreSQL
POSTGRES_CONN_ID = 'postgres_element_status'

# Variáveis globais para conexões
pg_hook = None

def initialize_connections():
    """Inicializa as conexões com o banco de dados"""
    global pg_hook
    
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
        
    except Exception as e:
        print(f"❌ Erro ao inicializar conexões: {str(e)}")
        raise

def update_element_status_task(**context):
    """Task para atualizar o status ativo/inativo dos elementos"""
    try:
        print("🔄 Iniciando atualização de status dos elementos...")
        with pg_hook.get_conn() as conn:
            with conn.cursor() as cursor:
                # Executar a procedure
                cursor.execute("SELECT update_element_status_active();")
                conn.commit()
        print("✅ Atualização de status concluída com sucesso!")
        return True
    except Exception as e:
        print(f"❌ Erro ao atualizar status: {str(e)}")
        raise

# Definir a DAG
dag = DAG(
    'poc_status_elementos_update',
    default_args={
        'owner': 'Sadir',
        'depends_on_past': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5)
    },
    description='DAG para atualização de status dos elementos.',
    schedule_interval=None,#'0 */1 * * *',  # Executa a cada hora
    start_date=datetime(2024, 9, 9),
    catchup=False,
    max_active_runs=1,
    tags=["kafka","status_elementos"]
)

# Definir os operadores de início e fim
start = DummyOperator(task_id='start', dag=dag)
end = DummyOperator(task_id='end', dag=dag)

# Criar task de atualização de status
update_status_task = PythonOperator(
    task_id='update_element_status',
    python_callable=update_element_status_task,
    dag=dag
)

# Inicializar conexões antes de criar as tasks
initialize_connections()

# Definir as dependências
start >> update_status_task >> end 