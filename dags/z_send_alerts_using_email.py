# Nome da DAG: z_send_alerts_using_email
# Owner / responsável: Leandro
# Descrição do objetivo da DAG: DAG para enviar alertas por e-mail 📩
# Usa Druid?: Não
# Principais tabelas / consultas Druid acessadas: msg
# Frequência de execução (schedule): * * * * *
# Dag Activo?: 
# Autor: Leandro
# Data de modificação: 2025-05-26

#%%
import json
import tempfile
import ssl
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from datetime import datetime, timedelta
from hooks.kafka_connector import KafkaConnector
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.exceptions import AirflowSkipException
import requests
import json 


# Variaveis de conexão com kafka
CG_ID = 'consumer_airflow_alerts_by_email'
ORIGIN_TOPIC='email-notification'


# Configurações do servidor SMTP
SMTP_HOST = "10.215.39.106"
SMTP_PORT = 25
SMTP_USER = ""
SMTP_MAIL_FROM = "svc_avalon_redes@vivo.com.br"
SMTP_PWD = ""


def get_alarm_details(alarm_id):
    url = "https://symphony.apps.ocp-01.tdigital-vivo.com.br/apollo"

    # Monta o payload com o ID fornecido
    payload = {
        "query": """
        query FaultDetailQuery($ids: [String]) {
            alarmListByIds(ids: $ids) {
                status
                ackState
                ackUser 
                address
                alarmId
                alarmChangedTime
                alarmDetail
                alarmTypeName
                alarmClearedTime
                alarmOSSCreatedTime
                alarmRaisedTime
                eventType
                vendorName
                alarmedObjectName
                alarmedObjectId
                alarmedObjectType
                clearUser 
                cityName
                specificProblem
                perceivedSeverity
                networkType
                alarmTypeId
                externalAlarmId
                eventCount
                neId
                neType
                neName
                manufacturer
                model
                locationId
                locationName
                latitude
                longitude
                operativeState
                probableCause
                provinceName
                serviceAffecting
                siteId
                sourceSystem
                supplementaryFaultId
                childAlarms {
                    alarmId
                }
                thresholdRuleName
                algorithm
                value
                spatialAggregation
                aggregationTime
                timeZone
            }
        }
        """,
        "variables": {
            "ids": [alarm_id]
        }
    }

    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json;charset=UTF-8',
        'Authorization': 'Basic c3ltcGhvbnlAbnR0ZGF0YS5jb206c3ltcGhvbnlAbnR0ZGF0YS5jb20='
    }

    # Faz a requisição POST
    response = requests.post(url, headers=headers, json=payload)
    try:
        # Retorna o JSON resultante
        return response.json()['data']['alarmListByIds'][0]
    except:
        raise Exception("❌ Erro ao obter detalhes do alarme")

def severidade_icon_selector(severidade):
    base = {
        "CLEAR":"🟢",        
        "WARNING":"🔵",        
        "MINOR":"🟡",        
        "MAJOR":"🟠",        
        "CRITICAL":"🔴",
        "Sem informação":"⚪"     
    }
    return base[severidade]

def severidade_color_selector(severidade):
    base = {
        "CLEAR":"#00FF00",        
        "WARNING":"#3399FF",        
        "MINOR":"#FFFF00",        
        "MAJOR":"#FFA500",        
        "CRITICAL":"#FF0000",
        "Sem informação": "#ffffff"       
    }
    return base[severidade]

def html_email_base(severidade,severidade_color,severidade_icon,json_data,alarm_time):
    """ Cria a base do e-mail HTML """
    base_mail = f"""
        <html>
        <body style="font-family: Arial, sans-serif; background-color: #f8f8f8;">
            <table width="100%" style="padding: 20px;">
                <tr>
                    <td align="center">
                        <table width="600" style="background-color: #ffffff; border-radius: 8px; box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);">
                            <tr>
                                <td style="background-color: {severidade_color}; padding: 20px; text-align: center; color: white;">
                                    <h2>{severidade_icon} {severidade} {severidade_icon}</h2>
                                </td>
                            </tr>
                            <tr>
                                <td style="padding: 20px;">
                                    <p><strong>Nome:</strong> {json_data.get("alarmTypeName", "Sem informação")}</p>
                                    <p><strong>Objeto Alarmado:</strong> {json_data.get("alarmedObjectName", "Sem informação")}</p>
                                    <p><strong>Regra:</strong> {json_data.get("thresholdRuleName", "Sem informação")}</p>
                                    <p><strong>Valor Detectado:</strong> {json_data.get("value", "Sem informação")}</p>
                                    <p><strong>Horário:</strong> {alarm_time} {json_data.get("timeZone", "")}</p>
                                    <p><strong>Status do evento:</strong> {"ATIVO" if json_data.get("eventType", "Sem informação") == "x1" else "ENCERRADO"}</p>
                                </td>
                            </tr>
                        </table>
                    </td>
                </tr>
            </table>
        </body>
        </html>
    """
    
    if severidade == "CLEAR":
        data_alarm = get_alarm_details(json_data.get("alarmId"))
        
        base_mail = f"""
        <html>
            <body style="font-family: Arial, sans-serif; background-color: #f8f8f8;">
                <table width="100%" style="padding: 20px;">
                    <tr>
                        <td align="center">
                            <table width="600" style="background-color: #ffffff; border-radius: 8px; box-shadow: 0 4px 8px rgba(0, 0, 0, 0.1);">
                                <tr>
                                    <td style="background-color: {severidade_color}; padding: 20px; text-align: center; color: white;">
                                        <h2>{severidade_icon} {severidade} {severidade_icon}</h2>
                                    </td>
                                </tr>
                                <tr>
                                    <td style="padding: 20px;">
                                        <p><strong>Nome:</strong> {data_alarm.get("alarmTypeName", "Sem informação")}</p>
                                        <p><strong>Objeto Alarmado:</strong> {data_alarm.get("alarmedObjectName", "Sem informação")}</p>
                                        <p><strong>Regra:</strong> {data_alarm.get("thresholdRuleName", "Sem informação")}</p>
                                        <p><strong>Valor Detectado:</strong> {data_alarm.get("value", "Sem informação")}</p>
                                        <p><strong>Horário:</strong> {alarm_time} {data_alarm.get("timeZone", "")}</p>
                                        <p><strong>Status do evento:</strong> {"ATIVO" if data_alarm.get("eventType", "Sem informação") == "x1" else "ENCERRADO"}</p>
                                    </td>
                                </tr>
                            </table>
                        </td>
                    </tr>
                </table>
            </body>
        </html>
        """    
    return base_mail

def formatar_corpo_email(json_data,severidade):
    """ Formata o corpo do e-mail com os dados do alerta """
    alarm_time = json_data.get("alarmRaisedTime", "Sem informação")
    cleared = json_data.get("cleared", False)
    if isinstance(alarm_time, int):
        alarm_time = datetime.utcfromtimestamp(alarm_time / 1000).strftime('%Y-%m-%d %H:%M:%S')

    if cleared:
        severidade = "CLEAR"
    severidade_color = severidade_color_selector(severidade)
    severidade_icon = severidade_icon_selector(severidade)
    email_html =  html_email_base(severidade,severidade_color,severidade_icon,json_data,alarm_time)
    
  
    return email_html
   
def enviar_email(destinatarios, assunto, mensagem_json,severidade):
    """ Envia um e-mail formatado """
    if not destinatarios:
        print("❌ Nenhum destinatário encontrado. E-mail não enviado.")
        return

    msg = MIMEMultipart()
    msg['From'] = SMTP_MAIL_FROM
    msg['To'] = ', '.join(destinatarios)
    msg['Subject'] = assunto

    msg.attach(MIMEText(formatar_corpo_email(mensagem_json,severidade), 'html'))

    try:
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            if SMTP_USER:
                server.login(SMTP_USER, SMTP_PWD)

            server.sendmail(SMTP_MAIL_FROM, destinatarios, msg.as_string())
            print("✅ E-mail enviado com sucesso!")

    except Exception as e:
        print(f"❌ Erro ao enviar e-mail: {e}")

def main():
    
    print("🔄 Iniciando processamento de alertas...")
    
    # Para prdo demover comentário
    """kafka = KafkaConnector(
        topic_var_name=ORIGIN_TOPIC,
        kafka_url_var_name="prod_kafka_url",
        kafka_port_var_name="prod_kafka_port",
        kafka_variable_pem_content="pem_content",
        kafka_connection_id="kafka_default"
    )"""
    # Para produção apagar a configuração de dev a seguir:
    kafka = KafkaConnector(
        topic_var_name=ORIGIN_TOPIC,
        kafka_url_var_name="dev_kafka_url",
        kafka_port_var_name="dev_kafka_port",
        kafka_variable_pem_content="pem_content_dev",
        kafka_connection_id="kafka_dev"
    )
    
    print(f"⏳ Coletando mensagens para envio de reports...")

    consumer = kafka.create_kafka_connection('Consumer',CG_ID)
    mensagens = kafka.process_messages(consumer)
    
    if not mensagens:
        print("✅ Sem mensagens para processar...")
        raise AirflowSkipException

    print(f"📩 Total de mensagens recebidas: {len(mensagens)}")

    for mensagem in mensagens:
        destinatarios = mensagem.get("sendToEmail", {}).get("list", [])
        severidade = mensagem.get('perceivedSeverity', 'Sem informação').upper()
        severidade_icon = ''
        if severidade != 'SEM INFORMAÇÃO':
            severidade_icon = severidade_icon_selector(severidade)
        assunto = f"{severidade_icon} {mensagem.get('perceivedSeverity', 'Sem informação').upper()} ALERTA: {mensagem.get('alarmTypeName', 'Sem informação')} REGRA: {mensagem.get('thresholdRuleName', 'Sem informação')}"

        print(f"📤 Enviando e-mail para {destinatarios} com assunto: {assunto}")
        enviar_email(destinatarios, assunto, mensagem,severidade)

    print("✅ Processamento concluído com sucesso!")
       
dag = DAG(
    'Send_Alerts_Using_Email',
    default_args={'owner': 'Sadir', 'depends_on_past': False, 'retries': 0, 'retry_delay': timedelta(minutes=5)},
    description='DAG para enviar alertas por e-mail 📩',
    schedule_interval='* * * * *',
    start_date=datetime(2024, 9, 9),
    catchup=False,
    max_active_runs=1,
    tags=["kafka","alerts"]
)

start = DummyOperator(task_id='start', dag=dag)
send_alert_task = PythonOperator(task_id='send_alerts', python_callable=main, dag=dag)
end = DummyOperator(task_id='end', dag=dag)

start >> send_alert_task >> end

# %%
