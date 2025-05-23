#%%
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

def get_ips_netcompass():
    data = druid.send_query(QUERY_IPS_NETCOMPASS)
    data = [ip for ip in data  if ip['cleaned_ip'] !=  '❌Não informado']

    # Filtrando "❌Não informado" e dividindo IPs que têm ";"
    cleaned_data = []
    for item in data:
        if item != '❌Não informado':  # Ignora "❌Não informado"
            if ";" in item:  # Verifica se há múltiplos IPs no formato "IP1;IP2"
                cleaned_data.extend(item.split(";"))  # Divide e adiciona separadamente
            else:
                cleaned_data.append(item)  # Adiciona normalmente se for um único IP
    return cleaned_data

def get_ips_panda():
    return superset.get_data_from_chart(1053)

def main():
    print("🌟 Colentando IPs do Netcompass")
    # IPS Netcompass
    ips_netcompass = get_ips_netcompass()
    netcompas_df = pd.DataFrame(ips_netcompass)
    netcompas_df = netcompas_df.rename(columns={'STATUS': 'Status NETCOMAPASS'})
    netcompas_df = netcompas_df.rename(columns={'cleaned_ip': 'IP'})
    netcompas_df = netcompas_df.rename(columns={'DEVICENAME': 'DEVICENAME NETCOMAPASS'})
    netcompas_df = netcompas_df.rename(columns={'MODELO': 'MODELO NETCOMAPASS'})
    netcompas_df = netcompas_df.rename(columns={'ip_valido': 'IP Válido NETCOMAPASS'})

    # IPS PANDA
    print("🐼 Colentando IPs do PANDA")
    panda_ips = get_ips_panda()
    panda_df = pd.DataFrame(panda_ips)
    panda_df = panda_df.rename(columns={'Status': 'Status PANDA'})
    #panda_df = panda_df.rename(columns={'IP': 'IP'})
    panda_df = panda_df.rename(columns={'Nome IP': 'Nome IP PANDA'})
    panda_df = panda_df.rename(columns={'ID Externo': 'ID Externo PANDA'})
    panda_df = panda_df.rename(columns={'Regra de descoberta associada': 'Regra de Discovery PANDA'})

    print("🔃 Realizando merge...")
    df_merged = pd.merge(
        netcompas_df,
        panda_df,
        on=['IP'],
        how='left'
        )

    df_merged.insert(0,'processing_date',current_dateTime)
    df_merged  = df_merged.fillna('👌Não aplicável')
    disct_merged = df_merged.to_dict('records')

    print(f"⏫ Total de registros para envio ao Kafka: {len(disct_merged)}" )
    
    print("🟣 Criando conexão com Kafka..")
    kafka = KafkaConnector(
        topic_var_name="topic-netcompass-x-panda",
        kafka_url_var_name="prod_kafka_url",
        kafka_port_var_name="prod_kafka_port",
        kafka_variable_pem_content="pem_content",
        kafka_connection_id="kafka_default"
    )
    
    print("🟣 Criando producer Kafka...")
    producer = kafka.create_kafka_connection()

    print("🟣 Enviando mensagens...")
    kafka.send_mult_messages_to_kafka(
        menssages=disct_merged,
        producer=producer
    )
    print("✅ Concluído!")
    

dag = DAG(
    'Proc_Netcompas_X_PANDA',
    default_args={'owner': 'Sadir', 'depends_on_past': False, 'retries': 0, 'retry_delay': timedelta(minutes=5)},
    description='DAG para comparar dados do Netcompass com Druid',
    schedule_interval='20 1 * * *',
    start_date=datetime(2024, 9, 9),
    catchup=False,
    max_active_runs=1,
    tags=["kafka","druid","superset"]
)

start = DummyOperator(task_id='start', dag=dag)
processing_data = PythonOperator(task_id='send_alerts', python_callable=main, dag=dag)
end = DummyOperator(task_id='end', dag=dag)

start >> processing_data >> end

# %%
