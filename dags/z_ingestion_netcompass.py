# Nome da DAG: z_ingestion_netcompass
# Owner / responsável: Sadir
# Descrição do objetivo da DAG: DAG para gerar o lookup dos hls.
# Usa Druid?: Não
# Principais tabelas / consultas Druid acessadas: 
# Frequência de execução (schedule): 3 0 * * *
# Dag Activo?: 
# Autor: Sadir
# Data de modificação: 2025-05-26

# v2
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.utils.dates import datetime, timedelta
from hooks.kafka_connector import KafkaConnector
from operators.netcompass_operator import NetcompassOperator
from utils.date_utils import DateUtils

LIMIT = 100

# Instanciando a DAG antes das funções
dag = DAG(
    'Ingestion_Netcompass_Data',
    default_args={
        'owner': 'Sadir',
        'depends_on_past': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG para gerar o lookup dos hls.',
    schedule_interval='3 0 * * *',  # Executa a cada 30 minutos
    start_date=datetime(2024, 12, 4),
    catchup=False,
    max_active_runs=1,
    tags=['kafka', 'ingestion', 'netcompass'],
)

def main():
    
    print('🟣 Start connection from NETCOMPASS.')
    netcompass = NetcompassOperator()
    token = netcompass.get_token()
    results = netcompass.paginate_api(token, LIMIT)
    
    total_registers = len(results)
    
    if total_registers > 0:

        print('🔵 Start connection with Kafka.')
        kafka = KafkaConnector(
            topic_var_name="safira-ingestion-netcompass",
            kafka_url_var_name="prod_kafka_url",
            kafka_port_var_name="prod_kafka_port",
            kafka_variable_pem_content="pem_content",
            kafka_connection_id="kafka_default"
        )
        producer = kafka.create_kafka_connection()
        print('🔵 Created Kafka producer.')
        print('🔵 Sending data to Kafka...')
     

        kafka.send_mult_messages_to_kafka(
            menssages=results,
            producer=producer
        )
        print(f'✅ Messages sent on Kafka.')
        
    else:
        print('✅ No records returned. Process finished!')
        raise AirflowSkipException('✅ No records returned. Process finished!')

start = DummyOperator(
    task_id='start',
    dag=dag
)

process_counters = PythonOperator(
    task_id='process_netcompass',
    python_callable=main,
    provide_context=True,
    execution_timeout=timedelta(minutes=60),  # Limita a execução da task a 20 minutos
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

# Definindo as dependências das tarefas
start >> process_counters >> end
