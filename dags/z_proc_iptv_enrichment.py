# Nome da DAG: z_proc_iptv_enrichment
# Owner / responsável: Sadir
# Descrição do objetivo da DAG: Sem descrição detalhada
# Usa Druid?: Não
# Principais tabelas / consultas Druid acessadas: 
# Frequência de execução (schedule): 1 3 * * *
# Dag Activo?: 
# Autor: Sadir
# Data de modificação: 2025-05-26

# v7
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.utils.dates import datetime, timedelta
from hooks.kafka_connector import KafkaConnector
from base64 import b64encode
from operators.iptv_enrichment import IPTV_Enrichment
from concurrent.futures import ThreadPoolExecutor

TOPIC= "iptv_enrichment" 
DAG_NAME = 'Proc_IPTV_Enrichment'
DEFAULT_IP = "http://10.205.177.233"
USER = "testes-panda"
PASS = "UsoExclusivoPanda2025@@"
DEFAULT_VIEW = -1
KEY="ELEMENTS_NAME"
MAX_WORKERS = 6
dag = DAG(
    DAG_NAME,
    default_args={
        'owner': 'Sadir',
        'depends_on_past': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    },
    description=f'DAG para processar dados de enriquecimento de IPTV.)',
    schedule_interval= '1 3 * * *',  # Executa a cada 30 minutos
    start_date=datetime(2025, 1, 21),
    catchup=False,
    max_active_runs=1,
    tags=['processamento', 'kafka', 'iptv', 'enrichment'],
)

# Obtendo uma variável de data
def get_airflow_variable(variable_name, default_value=None):
    return Variable.get(variable_name, default_var=default_value)

def main():
    print(f'🟣Create connection from IPTV.')
    iptv = IPTV_Enrichment(
        default_ip=DEFAULT_IP,
        user=USER,
        password=PASS
    )
    
    iptv.enable_access()
    elements = iptv.get_elements_name()
    if len(elements)>0:
        
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
            menssages=elements,
            producer=producer,
            key="ELEMENTS_NAME"
        )
        print(f'✅ Elements name sent on Kafka.')
        
        print('🟠 Start process to get counters name...')

        # Paralelizando o processamento
        with ThreadPoolExecutor(max_workers=MAX_WORKERS) as executor:
            results = executor.map(iptv.process_element, elements)

        base_counter_list = list(results)
        print('Total lists return:', len(base_counter_list))
        
        return_counters = []
        for dict_data in base_counter_list:
            return_counters.extend(dict_data) 
        
        print(f'🟠 Total counters name: {len(return_counters)}')
        print('🔵 Sending counters name to Kafka...')
        kafka.send_mult_messages_to_kafka(  
            menssages=return_counters,
            producer=producer,
            key="COUNTERS_NAME"
        )
        print(f'✅ Elements name sent on Kafka.')
        
        print('🟠 End process to get counters name...')
  
    else:
        print('❌ No records returned!')
        raise SystemError

start = DummyOperator(
    task_id='start',
    dag=dag
)

process_counters = PythonOperator(
    task_id='process_iptv',
    python_callable=main,
    provide_context=True,
    execution_timeout=timedelta(minutes=120),  # Limita a execução da task a 20 minutos
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

# Definindo as dependências das tarefas
start >> process_counters >> end
