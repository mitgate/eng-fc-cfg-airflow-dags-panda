# Nome da DAG: z_agg_contadores_media
# Owner / responsável: Sadir
# Descrição do objetivo da DAG: DAG para agregação de contadores.
# Usa Druid?: Sim
# Principais tabelas / consultas Druid acessadas: 
# Frequência de execução (schedule): 1/15 * * * *
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
from hooks.druid_conector import DruidConnector
from utils.date_utils import DateUtils

# Instanciando a DAG antes das funções
dag = DAG(
    'Agg_Counters_media',
    default_args={
        'owner': 'Sadir',
        'depends_on_past': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG para agregação de contadores.',
    schedule_interval='1/15 * * * *',  # Executa a cada 30 minutos
    start_date=datetime(2024, 12, 4),
    catchup=False,
    max_active_runs=1,
    tags=['counters', 'druid'],
)
# Setando uma variável de data
def set_airflow_variable(variable_name, value):
    Variable.set(variable_name, value)

# Obtendo uma variável de data
def get_airflow_variable(variable_name, default_value=None):
    return Variable.get(variable_name, default_var=default_value)

def main():
    string_past_24_hours = (datetime.now() - timedelta(hours=24)).strftime("%Y-%m-%dT%H:%M:%S.%fZ")
    var_datetime_max = 'datetime_max_agg_counters'
    
    print(f'🕓 Start date if the variable {var_datetime_max} is not found: {string_past_24_hours}')
    start_date = get_airflow_variable(var_datetime_max,string_past_24_hours)
    
    if start_date != string_past_24_hours:
        print(f'✅🕓 Variable found, value filtered after: {start_date}')
    else:
         print(f'✅⏳ Variable not found, value filtered after: {start_date}') 
    
    druid = DruidConnector(druid_url_variable="druid_url")
    print('🟣 Created Druid connection.')
    
    variable_name= 'query_agg_counters_media'
    print(f'🟣 Get query from variable: {variable_name}')
    
    query =  Variable.get(variable_name) 
    query = query.replace("'FILTER_HERE'",f"'{start_date}'")
    
    result = druid.send_query(query)
    total_registers = len(result)
    print(f'🟣 Total of registers returned: {total_registers}.')
    if len(result)>0:
        
        utils = DateUtils(data=result)
        print('🕓 Identifying the largest among the collected data.')
        max_date_from_column = utils.get_latest_date_parallel(
            date_column='date_base',
            date_format="%Y-%m-%dT%H:%M:%S.%fZ"
            )
        print(f'🕓 Longest date identified: {max_date_from_column}')
        print('🔵 Start connection with Kafka.')
        kafka = KafkaConnector(
            topic_var_name="agg_counters_topic",
            kafka_url_var_name="prod_kafka_url",
            kafka_port_var_name="prod_kafka_port",
            kafka_variable_pem_content="pem_content",
            kafka_connection_id="kafka_default"
        )
        producer = kafka.create_kafka_connection()
        print('🔵 Created Kafka producer.')
    
        print('🔵 Sending data to Kafka...')
        kafka.send_mult_messages_to_kafka(
            menssages=result,
            producer=producer
        )
        print(f'✅ Messages sent on Kafka.')
        
        set_airflow_variable(var_datetime_max,max_date_from_column)
        print(f"🔼 Variable {var_datetime_max} updated with new datetime: {max_date_from_column}")
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
