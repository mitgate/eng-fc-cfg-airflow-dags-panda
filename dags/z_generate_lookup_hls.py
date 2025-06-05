# Nome da DAG: z_generate_lookup_hls
# Owner / responsável: Leandro
# Descrição do objetivo da DAG: DAG para gerar o lookup dos hls.
# Usa Druid?: Sim
# Principais tabelas / consultas Druid acessadas: 
# Frequência de execução (schedule): 3 3 * * *
# Dag Activo?: 
# Autor: Leandro
# Data de modificação: 2025-05-26

# v1
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.utils.dates import datetime, timedelta
from hooks.kafka_connector import KafkaConnector
from hooks.druid_conector import DruidConnector
from utils.date_utils import DateUtils

# Instanciando a DAG antes das funções
dag = DAG(
    'Generate_Lookup_HL',
    default_args={
        'owner': 'Leandro',
        'depends_on_past': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG para gerar o lookup dos hls.',
    schedule_interval='3 3 * * *',  # Executa a cada 30 minutos
    start_date=datetime(2024, 12, 4),
    catchup=False,
    max_active_runs=1,
    tags=['lookup', 'druid'],
)


def main():
   
    druid = DruidConnector(druid_url_variable="druid_url")
    print('🟣 Created Druid connection.')
    
    variable_name= 'lookup_hls_query'
    print(f'🟣 Get query from variable: {variable_name}')
    
    query =  Variable.get(variable_name)
    
    results = druid.send_query(query)
    total_registers = len(results)
    print(f'🟣 Total of registers returned: {total_registers}.')
    if total_registers > 0:

        print('🔵 Start connection with Kafka.')
        kafka = KafkaConnector(
            topic_var_name="lookup_hls_topic",
            kafka_url_var_name="prod_kafka_url",
            kafka_port_var_name="prod_kafka_port",
            kafka_variable_pem_content="pem_content",
            kafka_connection_id="kafka_default"
        )
        producer = kafka.create_kafka_connection()
        print('🔵 Created Kafka producer.')
        print('🔵 Sending data to Kafka...')
        for result in results:
            additionaldn = result['additionalDn']
            hl = result['hls']
            kafka.send_message_text(
                message=hl,
                producer=producer,
                key=additionaldn
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
    task_id='process_counters',
    python_callable=main,
    provide_context=True,
    execution_timeout=timedelta(minutes=20),  # Limita a execução da task a 20 minutos
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

# Definindo as dependências das tarefas
start >> process_counters >> end
