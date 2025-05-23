# V0

import json
import tempfile
import ssl
from datetime import datetime, timedelta
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from hooks.kafka_connector import KafkaConnector

# Variáveis do Kafka
ORIGIN_TOPIC = 'redirect_topic_origin'
DESTINATION_TOPIC = 'redirect_topic_destination'

CG_ID= 'redirect-btw-topics-airflow'

dag = DAG(
    'Redirect_Messages_btw_topics',
    default_args={'owner': 'Sadir', 'depends_on_past': False, 'retries': 0, 'retry_delay': timedelta(minutes=5)},
    description='DAG redirecionar mensagens do kafka.',
    schedule_interval='* * * * *',
    start_date=datetime(2024, 9, 9),
    catchup=False,
    max_active_runs=1,
    tags=["kafka","redirect"]
)

def main():
    print("🔄 Iniciando redirecionamento.")
    kafka_from = KafkaConnector(
        topic_var_name=ORIGIN_TOPIC,
        kafka_url_var_name="prod_kafka_url",
        kafka_port_var_name="prod_kafka_port",
        kafka_variable_pem_content="pem_content",
        kafka_connection_id="kafka_default"
    )
    print(f"⏳ Coletando mensagens para direcionamento...")

    consumer = kafka_from.create_kafka_connection('Consumer',CG_ID)
    mensagens = kafka_from.process_messages(consumer)
    
    if not mensagens:
        print("✅ Sem mensagens para processar...")
        raise AirflowSkipException
    print(f"🔎 Total de mensagens coletadas para direcionamento [ {len(mensagens)} ]")
    kafka_to = KafkaConnector(
        topic_var_name=DESTINATION_TOPIC,
        kafka_url_var_name="prod_kafka_url",
        kafka_port_var_name="prod_kafka_port",
        kafka_variable_pem_content="pem_content",
        kafka_connection_id="kafka_default"
    )
    producer = kafka_to.create_kafka_connection()
    print('🔵 Created Kafka producer.')

    print('🔵 Sending elements name to Kafka...')
    kafka_to.send_mult_messages_to_kafka(  
        menssages=mensagens,
        producer=producer,
        key="redirect-by-airflow"
    )
    
    print("✅ Direcionamento concluído com sucesso!")
       
start = DummyOperator(task_id='start', dag=dag)
send_alert_task = PythonOperator(task_id='send_alerts', python_callable=main, dag=dag)
end = DummyOperator(task_id='end', dag=dag)

start >> send_alert_task >> end

