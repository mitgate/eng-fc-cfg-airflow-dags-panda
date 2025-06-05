# Nome da DAG: z_proc_ips_panda
# Owner / responsável: Leandro
# Descrição do objetivo da DAG: DAG para comparar dados do Netcompass com Druid
# Usa Druid?: Sim
# Principais tabelas / consultas Druid acessadas: 
# Frequência de execução (schedule): 10 1 * * *
# Dag Activo?: 
# Autor: Leandro
# Data de modificação: 2025-05-26

#%% v3
from datetime import datetime, timedelta
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
from datetime import datetime
import pandas as pd
from hooks.druid_conector import DruidConnector
from hooks.superset_conector import SupersetConnector
from hooks.kafka_connector import KafkaConnector
import urllib3
urllib3.disable_warnings(urllib3.exceptions.InsecureRequestWarning)

# Variáveis do Kafka
KAFKA_URL = Variable.get('dev_kafka_url')
KAFKA_PORT = Variable.get('dev_kafka_port')
VAR_BASE_DATE_KAFKA = 'alert_base_date'
QUERY_IPS_NETCOMPASS = Variable.get('query_ips_netcompass_x_panda')
SUPERSET_URL  = f"http://{Variable.get('superset_prd_host')}"

current_dateTime = datetime.now()
current_dateTime = current_dateTime.strftime("%Y-%m-%d %H:%M:%S")

superset = SupersetConnector(url=SUPERSET_URL)
druid = DruidConnector(druid_url_variable='druid_url')

def main():
    # IPS PANDA
    print("🐼 Colentando IPs do PANDA")
    panda_ips = superset.get_data_from_chart(1053)
    
    for i in panda_ips:
        i["current_dateTime"] = current_dateTime
        
    print(f"⏫ Total de registros para envio ao Kafka: {len(panda_ips)}" )
    
    print("🟣 Criando conexão com Kafka..")
    kafka = KafkaConnector(
        topic_var_name="discovery-ips-panda",
        kafka_url_var_name="prod_kafka_url",
        kafka_port_var_name="prod_kafka_port",
        kafka_variable_pem_content="pem_content",
        kafka_connection_id="kafka_default"
    )
    
    print("🟣 Criando producer Kafka...")
    producer = kafka.create_kafka_connection()

    print("🟣 Enviando mensagens...")
    kafka.send_mult_messages_to_kafka(
        menssages=panda_ips,
        producer=producer
    )
    print("✅ Concluído!")

dag = DAG(
    'Proc_IPS_PANDA',
    default_args={'owner': 'Leandro', 'depends_on_past': False, 'retries': 0, 'retry_delay': timedelta(minutes=5)},
    description='DAG para comparar dados do Netcompass com Druid',
    schedule_interval='10 1 * * *',
    start_date=datetime(2024, 9, 9),
    catchup=False,
    max_active_runs=1,
    tags=["kafka","druid","superset"]
)

start = DummyOperator(task_id='start', dag=dag)
processing_data = PythonOperator(task_id='send_alerts', python_callable=main, dag=dag)
end = DummyOperator(task_id='end', dag=dag)

start >> processing_data >> end

