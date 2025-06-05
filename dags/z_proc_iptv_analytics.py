# Nome da DAG: z_proc_iptv_analytics
# Owner / responsável: Leandro
# Descrição do objetivo da DAG: Sem descrição detalhada
# Usa Druid?: Não
# Principais tabelas / consultas Druid acessadas: 
# Frequência de execução (schedule): 
# Dag Activo?: 
# Autor: Leandro
# Data de modificação: 2025-05-26

# v0.3
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.exceptions import AirflowSkipException
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.utils.dates import datetime, timedelta
from hooks.kafka_connector import KafkaConnector
from base64 import b64encode
from operators.iptv_operator import IptvConnector


TOPIC= "procc_iptv_topic" 
DATE_FORMAT = "%Y-%m-%dT%H:%M:%SZ"
VAR_TIME ='datetime_proc_iptv_analytics'
DAG_NAME = 'Proc_IPTV_analytics'
INDEX = "analytics"


dag = DAG(
    DAG_NAME,
    default_args={
        'owner': 'Leandro',
        'depends_on_past': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    },
    description=f'DAG para processar dados de IPTV ({INDEX})',
    schedule_interval= None, #'*/5 * * * *',  # Executa a cada 30 minutos
    start_date=datetime(2024, 12, 9),
    catchup=False,
    max_active_runs=1,
    tags=['processamento', 'kafka', 'iptv'],
)

def set_airflow_variable(variable_name, value):
    Variable.set(variable_name, value)

# Obtendo uma variável de data
def get_airflow_variable(variable_name, default_value=None):
    return Variable.get(variable_name, default_var=default_value)

def main():
    string_past_24_hour = (datetime.now() - timedelta(hours=24)).strftime(DATE_FORMAT)
    var_datetime_max = VAR_TIME 
    print(f'🕓 Start date if the variable {var_datetime_max} is not found: {string_past_24_hour}')
    start_date = get_airflow_variable(var_datetime_max,string_past_24_hour)
    
    if start_date is not None:
        print(f'✅🕓 Variable found, value filtered after: {start_date}')
    else:
         print(f'🟡🕓 Variable not found, value filtered after: {start_date}') 
    
    print(f'🟣Create connection from IPTV using this index: {INDEX}.')
    iptv = IptvConnector(INDEX)
    
    result, max_datetime = iptv.main(start_date)
    
    if len(result)>0:

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

        print('🔵 Sending data to Kafka...')
        kafka.send_mult_messages_to_kafka(  
            menssages=result,
            producer=producer,
            key=INDEX
        )
        print(f'✅ Messages sent on Kafka.')
        
        set_airflow_variable(var_datetime_max,max_datetime)
        print(f"🔼 Variable {var_datetime_max} updated with new datetime: {max_datetime}")
    else:
        print('✅ No records returned. Process finished!')
        raise AirflowSkipException('✅ No records returned. Process finished!')

start = DummyOperator(
    task_id='start',
    dag=dag
)

process_counters = PythonOperator(
    task_id='process_iptv',
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
