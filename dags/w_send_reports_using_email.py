# # #%%
# # import requests
# # import json
# # import requests
# # import tempfile
# # import time
# # import ssl
# # import os
# # import smtplib
# # from email.mime.multipart import MIMEMultipart
# # from email.mime.text import MIMEText
# # from email.mime.base import MIMEBase
# # from email import encoders
# # from airflow import DAG
# # from airflow.operators.python import PythonOperator
# # from airflow.operators.dummy import DummyOperator
# # from airflow.models import Variable
# # from airflow.utils.dates import datetime, timedelta
# # from airflow.exceptions import AirflowSkipException
# # from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
# # import unicodedata
# # from hooks.kafka_connector import KafkaConnector

# # # Configurações do servidor SMTP
# # SMTP_HOST = "10.215.39.106"  # IP ou Hostname do servidor SMTP
# # SMTP_PORT = 25
# # SMTP_USER = ""  # Deixe vazio se não precisar de autenticação
# # SMTP_MAIL_FROM = "svc_avalon_redes@vivo.com.br"
# # SMTP_PWD = ""

# # DEFAULT_TOPIC ='topic-email-send-reports'
# # KAFKA_URL = Variable.get('dev_kafka_url') #'amqstreams-kafka-external-bootstrap-panda-amq-streams-dev.apps.ocp-01.tdigital-vivo.com.br'#
# # KAFKA_PORT = Variable.get('dev_kafka_port') #'443'
# # CG_ID= 'reports_airflow'
# # VAR_BASE_DATE_KAFKA = 'report_base_date'

# # # Instanciando a DAG antes das funções
# # dag = DAG(
# #     'Send_Reports',
# #     default_args={
# #         'owner': 'Sadir',
# #         'depends_on_past': False,
# #         'retries': 0,
# #         'retry_delay': timedelta(minutes=5),
# #     },
# #     description='DAG para enviar reports por e-mail 📩',
# #     schedule_interval='* * * * *',#'30 0-22/2 * * *',  # Executa a cada 30 minutos
# #     start_date=datetime(2024, 9, 9),
# #     catchup=False,
# #     max_active_runs=1,
# #     tags=['symphony','reports'],
# # )

# # def ajustar_nome_anexo(nome_arquivo):
# #     # Remove acentos
# #     nfkd_form = unicodedata.normalize('NFKD', nome_arquivo)
# #     nome_sem_acento = ''.join([c for c in nfkd_form if not unicodedata.combining(c)])
# #     # Substitui espaços por underscores e converte para minúsculas
# #     nome_final = nome_sem_acento.replace(' ', '_').lower()
# #     return nome_final

# # def baixar_arquivo(formato, key):
# #     url_base = "http://symphony.apps.ocp-01.tdigital-vivo.com.br/store/get?key="
# #     url = f"{url_base}{key}"
    
# #     headers = {
# #         'Authorization': 'Basic c3ltcGhvbnlAbnR0ZGF0YS5jb206c3ltcGhvbnlAbnR0ZGF0YS5jb20='
# #     }

# #     # Envia a requisição para obter o arquivo
# #     response = requests.get(url, headers=headers)

# #     # Verifica se a requisição foi bem-sucedida
# #     if response.status_code == 200:
# #         # Cria um arquivo temporário para salvar o conteúdo
# #         with tempfile.NamedTemporaryFile(delete=False, suffix=f".{formato}") as temp_file:
# #             temp_file.write(response.content)
# #             caminho_arquivo_temp = temp_file.name
        
# #         print(f"✅ Arquivo salvo temporariamente como {caminho_arquivo_temp}")
# #         return caminho_arquivo_temp
# #     else:
# #         print(f"❌ Falha ao baixar o arquivo. Status code: {response.status_code}")
# #         return None
    
# # def formatar_corpo_email(info_mensagem):
# #     return f"""
# #     <html>
# #     <body style="margin: 0; padding: 0; background-color: #f4f4f9; font-family: Arial, sans-serif;">
# #         <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color: #f4f4f9; padding: 20px 0;">
# #             <tr>
# #                 <td align="center">
# #                     <table width="600" cellpadding="0" cellspacing="0" border="0" style="background-color: #ffffff; border-radius: 8px; overflow: hidden;">
# #                         <!-- Cabeçalho -->
# #                         <tr>
# #                             <td style="background-color: #4b0082; padding: 20px; text-align: center; color: white;">
# #                                 <br>
# #                                 <h1 style="font-size: 24px; margin: 0; padding-top: 10px; color: white;">Relatório Exportado</h1><br>
# #                             </td>
# #                         </tr>
# #                         <!-- Conteúdo -->
# #                         <tr>
# #                             <td style="padding: 30px 20px; font-size: 16px; color: #333333;">
# #                                 <p style="margin: 0 0 15px 0;">Olá,</p>
# #                                 <p style="margin: 0 0 15px 0;">O arquivo do relatório: <strong>{info_mensagem}</strong> está anexado a este e-mail.</p>
# #                                 <p style="margin: 0;">Por favor, revise o conteúdo anexado e entre em contato caso precise de mais informações.</p>
# #                             </td>
# #                         </tr>
# #                         <!-- Rodapé -->
# #                         <tr>
# #                             <td style="background-color: #4b0082; padding: 15px; text-align: center; color: white; font-size: 14px;">
# #                                 <p style="margin: 0;">Atenciosamente,</p>
# #                                 <p style="margin: 0;">Equipe de suporte da Vivo Telefônica</p>
# #                             </td>
# #                         </tr>
# #                     </table>
# #                 </td>
# #             </tr>
# #         </table>
# #     </body>
# #     </html>
# #     """

# # def enviar_email(destinatarios, assunto, formato_arquivo, caminho_anexo=None, nome_anexo=None, info_mensagem=None):
# #     # Cria a mensagem do e-mail
# #     msg = MIMEMultipart()
# #     msg['From'] = SMTP_MAIL_FROM
# #     msg['To'] = ', '.join(destinatarios)
# #     msg['Subject'] = assunto

# #     # Formatar o corpo do e-mail de acordo com o formato de arquivo (xlsx ou csv)
# #     corpo_email = formatar_corpo_email(info_mensagem)

# #     # Adiciona o corpo do e-mail (em HTML)
# #     msg.attach(MIMEText(corpo_email, 'html'))

# #     # Adiciona o anexo, se houver
# #     if caminho_anexo:
# #         try:
# #             with open(caminho_anexo, 'rb') as anexo:
# #                 part = MIMEBase('application', 'octet-stream')
# #                 part.set_payload(anexo.read())
# #                 encoders.encode_base64(part)
                
# #                 # Usa o nome_anexo se fornecido, caso contrário, extrai do caminho_anexo
# #                 if not nome_anexo:
# #                     nome_anexo = os.path.basename(caminho_anexo)
                
# #                 part.add_header(
# #                     'Content-Disposition',
# #                     f'attachment; filename="{nome_anexo}.{formato_arquivo}"',
# #                 )
# #                 msg.attach(part)
# #         except Exception as e:
# #             print(f"❌ Erro ao adicionar anexo: {e}")
# #             return

# #     try:
# #         # Conecta ao servidor SMTP
# #         with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
# #             # Se necessário, faça login
# #             if SMTP_USER:
# #                 server.login(SMTP_USER, SMTP_PWD)

# #             # Envia o e-mail para múltiplos destinatários
# #             server.sendmail(SMTP_MAIL_FROM, destinatarios, msg.as_string())
# #             print("E-mail enviado com sucesso!")

# #     except Exception as e:
# #         print(f"Erro ao enviar e-mail: {e}")
# #         raise

# # def main():
# #     kafka = KafkaConnector(
# #         topic_var_name=DEFAULT_TOPIC,
# #         kafka_url_var_name="prod_kafka_url",
# #         kafka_port_var_name="prod_kafka_port",
# #         kafka_variable_pem_content="pem_content",
# #         kafka_connection_id="kafka_default"
# #     )
# #     print(f"⏳ Coletando mensagens para envio de reports...")
    
# #     consumer = kafka.create_kafka_connection('Consumer',CG_ID)
# #     messagens = kafka.process_messages(consumer)
# #     print(f'♨ Total de mensagens recebidas: {len(messagens)}')
# #     if messagens:
        
# #         print("⌛ Inicio do processo das mensagens...")
# #         for mensagem in messagens:

# #             storekey=mensagem['storekey']
# #             exportFormat=mensagem['exportFormat']
# #             recipients=mensagem['recipients'].strip('[]').split(',')
# #             print(f"📂 Formato: {exportFormat}")
# #             print(f"🔑 Chave para download: {storekey}")
# #             print(f"✉ Destinatários: {recipients}")
# #             caminho_arquivo = baixar_arquivo(formato=exportFormat,key=storekey)
# #             assunto = str(storekey).replace(exportFormat,'').replace('exports/','')
# #             nome_anexo =  ajustar_nome_anexo(assunto)
# #             info_mensagem = storekey.replace('exports/','')
# #             enviar_email(
# #                 destinatarios=recipients,
# #                 assunto = f"Envio do report agendado: {assunto}",
# #                 formato_arquivo = exportFormat,
# #                 caminho_anexo =caminho_arquivo,
# #                 nome_anexo = nome_anexo,
# #                 info_mensagem = info_mensagem
# #                 )
# #     else:
# #         print(f"🆗 Nenhuma mensagem para processar...")  
# #         raise AirflowSkipException

# # # Definindo as tarefas
# # start = DummyOperator(
# #     task_id='start',
# #     dag=dag
# # )

# # process_dashboards_task = PythonOperator(
# #     task_id='process_messages',
# #     python_callable=main,
# #     provide_context=True,
# #     execution_timeout=timedelta(minutes=20),  # Limita a execução da task a 20 minutos
# #     dag=dag
# # )

# # end = DummyOperator(
# #     task_id='end',
# #     dag=dag
# # )

# # # Definindo as dependências das tarefas
# # start >> process_dashboards_task >> end
# """
# DAG: Send_Reports

# Resumo:
# DAG responsável por coletar reportes do tópico Kafka `email-notification`,
# realizar o download dos arquivos via Symphony e enviar por e-mail para
# os destinatários indicados em cada mensagem.

# Autor: Squad Airflow - Leandro
# Última atualização: 2025-05-13
# """

# #%%
# import requests
# import json
# import requests
# import tempfile
# import time
# import ssl
# import os
# import smtplib
# from email.mime.multipart import MIMEMultipart
# from email.mime.text import MIMEText
# from email.mime.base import MIMEBase
# from email import encoders
# from airflow import DAG
# from airflow.operators.python import PythonOperator
# from airflow.operators.dummy import DummyOperator
# from airflow.models import Variable
# from airflow.utils.dates import datetime, timedelta
# from airflow.exceptions import AirflowSkipException
# from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
# import unicodedata
# from hooks.kafka_connector import KafkaConnector

# # Configurações do servidor SMTP
# SMTP_HOST = "10.215.39.106"  # IP ou Hostname do servidor SMTP
# SMTP_PORT = 25
# SMTP_USER = ""  # Deixe vazio se não precisar de autenticação
# SMTP_MAIL_FROM = "svc_avalon_redes@vivo.com.br"
# SMTP_PWD = ""

# # Mantendo as linhas comentadas originais (ambiente dev):
# # KAFKA_URL = Variable.get('dev_kafka_url') #'amqstreams-kafka-external-bootstrap-panda-amq-streams-dev.apps.ocp-01.tdigital-vivo.com.br'#
# # KAFKA_PORT = Variable.get('dev_kafka_port') #'443'

# # Ajuste para o ambiente de produção, se necessário:
# DEFAULT_TOPIC = Variable.get('topic-email-send-reports')
# KAFKA_URL = Variable.get('prod_kafka_url')  # Ajustado para bater com a config do main()
# KAFKA_PORT = Variable.get('prod_kafka_port') # Ajustado para bater com a config do main()

# CG_ID= 'reports_airflow'
# VAR_BASE_DATE_KAFKA = 'report_base_date'

# # Instanciando a DAG antes das funções
# dag = DAG(
#     'Send_Reports',
#     default_args={
#         'owner': 'Leandro',
#         'depends_on_past': False,
#         'retries': 0,
#         'retry_delay': timedelta(minutes=5),
#     },
#     description='DAG para enviar reports por e-mail 📩',
#     schedule_interval='* * * * *',#'30 0-22/2 * * *',  # Executa a cada 30 minutos
#     start_date=datetime(2024, 9, 9),
#     catchup=False,
#     max_active_runs=1,
#     tags=['symphony','reports'],
# )

# def ajustar_nome_anexo(nome_arquivo):
#     # Remove acentos
#     nfkd_form = unicodedata.normalize('NFKD', nome_arquivo)
#     nome_sem_acento = ''.join([c for c in nfkd_form if not unicodedata.combining(c)])
#     # Substitui espaços por underscores e converte para minúsculas
#     nome_final = nome_sem_acento.replace(' ', '_').lower()
#     return nome_final

# def baixar_arquivo(formato, key):
#     url_base = "http://symphony.apps.ocp-01.tdigital-vivo.com.br/store/get?key="
#     url = f"{url_base}{key}"
    
#     headers = {
#         'Authorization': 'Basic c3ltcGhvbnlAbnR0ZGF0YS5jb206c3ltcGhvbnlAbnR0ZGF0YS5jb20='
#     }

#     # Envia a requisição para obter o arquivo
#     try:
#         response = requests.get(url, headers=headers)
#     except Exception as err:
#         print(f"❌ Erro na requisição HTTP: {err}")
#         return None

#     # Verifica se a requisição foi bem-sucedida
#     if response.status_code == 200:
#         # Cria um arquivo temporário para salvar o conteúdo
#         try:
#             with tempfile.NamedTemporaryFile(delete=False, suffix=f".{formato}") as temp_file:
#                 temp_file.write(response.content)
#                 caminho_arquivo_temp = temp_file.name
#             print(f"✅ Arquivo salvo temporariamente como {caminho_arquivo_temp}")
#             return caminho_arquivo_temp
#         except Exception as e:
#             print(f"❌ Falha ao salvar arquivo temporário: {e}")
#             return None
#     else:
#         print(f"❌ Falha ao baixar o arquivo. Status code: {response.status_code}")
#         return None
    
# def formatar_corpo_email(info_mensagem):
#     return f"""
#     <html>
#     <body style="margin: 0; padding: 0; background-color: #f4f4f9; font-family: Arial, sans-serif;">
#         <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color: #f4f4f9; padding: 20px 0;">
#             <tr>
#                 <td align="center">
#                     <table width="600" cellpadding="0" cellspacing="0" border="0" style="background-color: #ffffff; border-radius: 8px; overflow: hidden;">
#                         <!-- Cabeçalho -->
#                         <tr>
#                             <td style="background-color: #4b0082; padding: 20px; text-align: center; color: white;">
#                                 <br>
#                                 <h1 style="font-size: 24px; margin: 0; padding-top: 10px; color: white;">Relatório Exportado</h1><br>
#                             </td>
#                         </tr>
#                         <!-- Conteúdo -->
#                         <tr>
#                             <td style="padding: 30px 20px; font-size: 16px; color: #333333;">
#                                 <p style="margin: 0 0 15px 0;">Olá,</p>
#                                 <p style="margin: 0 0 15px 0;">O arquivo do relatório: <strong>{info_mensagem}</strong> está anexado a este e-mail.</p>
#                                 <p style="margin: 0;">Por favor, revise o conteúdo anexado e entre em contato caso precise de mais informações.</p>
#                             </td>
#                         </tr>
#                         <!-- Rodapé -->
#                         <tr>
#                             <td style="background-color: #4b0082; padding: 15px; text-align: center; color: white; font-size: 14px;">
#                                 <p style="margin: 0;">Atenciosamente,</p>
#                                 <p style="margin: 0;">Equipe de suporte da Vivo Telefônica</p>
#                             </td>
#                         </tr>
#                     </table>
#                 </td>
#             </tr>
#         </table>
#     </body>
#     </html>
#     """

# def enviar_email(destinatarios, assunto, formato_arquivo, caminho_anexo=None, nome_anexo=None, info_mensagem=None):
#     # Cria a mensagem do e-mail
#     msg = MIMEMultipart()
#     msg['From'] = SMTP_MAIL_FROM
#     msg['To'] = ', '.join(destinatarios)
#     msg['Subject'] = assunto

#     # Formatar o corpo do e-mail de acordo com o formato de arquivo (xlsx ou csv)
#     corpo_email = formatar_corpo_email(info_mensagem)

#     # Adiciona o corpo do e-mail (em HTML)
#     msg.attach(MIMEText(corpo_email, 'html'))

#     # Adiciona o anexo, se houver
#     if caminho_anexo:
#         try:
#             with open(caminho_anexo, 'rb') as anexo:
#                 part = MIMEBase('application', 'octet-stream')
#                 part.set_payload(anexo.read())
#                 encoders.encode_base64(part)
                
#                 # Usa o nome_anexo se fornecido, caso contrário, extrai do caminho_anexo
#                 if not nome_anexo:
#                     nome_anexo = os.path.basename(caminho_anexo)
                
#                 part.add_header(
#                     'Content-Disposition',
#                     f'attachment; filename="{nome_anexo}.{formato_arquivo}"',
#                 )
#                 msg.attach(part)
#         except Exception as e:
#             print(f"❌ Erro ao adicionar anexo: {e}")
#             return

#     try:
#         # Conecta ao servidor SMTP
#         with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
#             # Se necessário, faça login
#             if SMTP_USER:
#                 try:
#                     server.login(SMTP_USER, SMTP_PWD)
#                 except Exception as e_login:
#                     print(f"❌ Erro ao autenticar no SMTP: {e_login}")
#                     return

#             # Envia o e-mail para múltiplos destinatários
#             server.sendmail(SMTP_MAIL_FROM, destinatarios, msg.as_string())
#             print("E-mail enviado com sucesso!")
#     except Exception as e:
#         print(f"Erro ao enviar e-mail: {e}")
#         raise

# def main():
#     try:
#         # Conecta ao Kafka no ambiente de produção
#         kafka = KafkaConnector(
#             topic_var_name=DEFAULT_TOPIC,
#             kafka_url_var_name="prod_kafka_url",
#             kafka_port_var_name="prod_kafka_port",
#             kafka_variable_pem_content="pem_content",
#             kafka_connection_id="kafka_default"
#         )
#     except Exception as e_kafka:
#         print(f"❌ Erro ao instanciar KafkaConnector: {e_kafka}")
#         raise

#     print(f"⏳ Coletando mensagens para envio de reports...")

#     try:
#         consumer = kafka.create_kafka_connection('Consumer', CG_ID)
#         messagens = kafka.process_messages(consumer)
#     except Exception as e_consumer:
#         print(f"❌ Erro ao criar ou processar mensagens do consumer: {e_consumer}")
#         raise

#     print(f'♨ Total de mensagens recebidas: {len(messagens)}')

#     if messagens:
#         print("⌛ Inicio do processo das mensagens...")
#         for mensagem in messagens:
#             try:
#                 storekey = mensagem['storekey']
#                 exportFormat = mensagem['exportFormat']
#                 recipients = mensagem['recipients'].strip('[]').split(',')
                
#                 print(f"📂 Formato: {exportFormat}")
#                 print(f"🔑 Chave para download: {storekey}")
#                 print(f"✉ Destinatários: {recipients}")

#                 caminho_arquivo = baixar_arquivo(formato=exportFormat, key=storekey)
#                 if not caminho_arquivo:
#                     print("❌ Arquivo não baixado, pulando envio de e-mail.")
#                     continue

#                 assunto = str(storekey).replace(exportFormat, '').replace('exports/', '')
#                 nome_anexo = ajustar_nome_anexo(assunto)
#                 info_mensagem = storekey.replace('exports/', '')

#                 enviar_email(
#                     destinatarios=recipients,
#                     assunto=f"Envio do report agendado: {assunto}",
#                     formato_arquivo=exportFormat,
#                     caminho_anexo=caminho_arquivo,
#                     nome_anexo=nome_anexo,
#                     info_mensagem=info_mensagem
#                 )
#             except Exception as e_msg:
#                 print(f"❌ Erro ao processar a mensagem {mensagem}: {e_msg}")
#                 # Continua para próxima mensagem
#     else:
#         print(f"🆗 Nenhuma mensagem para processar...")  
#         raise AirflowSkipException

# # Definindo as tarefas
# start = DummyOperator(
#     task_id='start',
#     dag=dag
# )

# process_dashboards_task = PythonOperator(
#     task_id='process_messages',
#     python_callable=main,
#     provide_context=True,
#     execution_timeout=timedelta(minutes=20),  # Limita a execução da task a 20 minutos
#     dag=dag
# )

# end = DummyOperator(
#     task_id='end',
#     dag=dag
# )

# # Definindo as dependências das tarefas
# start >> process_dashboards_task >> end

"""
DAG: Send_Reports

Resumo:
DAG responsável por coletar reportes do tópico Kafka `email-notification`,
realizar o download dos arquivos via Symphony e enviar por e-mail para
os destinatários indicados em cada mensagem.

Autor: Squad Airflow - Leandro
Última atualização: 2025-05-13
"""

#%%
import requests
import json
import requests
import tempfile
import time
import ssl
import os
import smtplib
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.mime.base import MIMEBase
from email import encoders
from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.utils.dates import datetime, timedelta
from airflow.exceptions import AirflowSkipException
from confluent_kafka import Producer, Consumer, KafkaError, KafkaException
import unicodedata
from hooks.kafka_connector import KafkaConnector

# Configurações do servidor SMTP
SMTP_HOST = "10.215.39.106"  # IP ou Hostname do servidor SMTP
SMTP_PORT = 25
SMTP_USER = ""  # Deixe vazio se não precisar de autenticação
SMTP_MAIL_FROM = "svc_avalon_redes@vivo.com.br"
SMTP_PWD = ""

# Mantendo as linhas comentadas originais (ambiente dev):
# KAFKA_URL = Variable.get('dev_kafka_url') #'amqstreams-kafka-external-bootstrap-panda-amq-streams-dev.apps.ocp-01.tdigital-vivo.com.br'#
# KAFKA_PORT = Variable.get('dev_kafka_port') #'443'

# Ajuste para o ambiente de produção, se necessário:
DEFAULT_TOPIC = Variable.get('topic-email-send-reports')
KAFKA_URL = Variable.get('prod_kafka_url')  # Ajustado para bater com a config do main()
KAFKA_PORT = Variable.get('prod_kafka_port') # Ajustado para bater com a config do main()

CG_ID= 'reports_airflow'
VAR_BASE_DATE_KAFKA = 'report_base_date'

# Instanciando a DAG antes das funções
dag = DAG(
    'Send_Reports',
    default_args={
        'owner': 'Leandro',
        'depends_on_past': False,
        'retries': 0,
        'retry_delay': timedelta(minutes=5),
    },
    description='DAG para enviar reports por e-mail 📩',
    schedule_interval='* * * * *',#'30 0-22/2 * * *',  # Executa a cada 30 minutos
    start_date=datetime(2024, 9, 9),
    catchup=False,
    max_active_runs=1,
    tags=['symphony','reports'],
)

def ajustar_nome_anexo(nome_arquivo):
    # Remove acentos
    nfkd_form = unicodedata.normalize('NFKD', nome_arquivo)
    nome_sem_acento = ''.join([c for c in nfkd_form if not unicodedata.combining(c)])
    # Substitui espaços por underscores e converte para minúsculas
    nome_final = nome_sem_acento.replace(' ', '_').lower()
    return nome_final

def baixar_arquivo(formato, key):
    url_base = "http://symphony.apps.ocp-01.tdigital-vivo.com.br/store/get?key="
    url = f"{url_base}{key}"
    
    headers = {
        'Authorization': 'Basic c3ltcGhvbnlAbnR0ZGF0YS5jb206c3ltcGhvbnlAbnR0ZGF0YS5jb20='
    }

    # Envia a requisição para obter o arquivo
    try:
        response = requests.get(url, headers=headers)
    except Exception as err:
        print(f"❌ Erro na requisição HTTP: {err}")
        return None

    # Verifica se a requisição foi bem-sucedida
    if response.status_code == 200:
        # Cria um arquivo temporário para salvar o conteúdo
        try:
            with tempfile.NamedTemporaryFile(delete=False, suffix=f".{formato}") as temp_file:
                temp_file.write(response.content)
                caminho_arquivo_temp = temp_file.name
            print(f"✅ Arquivo salvo temporariamente como {caminho_arquivo_temp}")
            return caminho_arquivo_temp
        except Exception as e:
            print(f"❌ Falha ao salvar arquivo temporário: {e}")
            return None
    else:
        print(f"❌ Falha ao baixar o arquivo. Status code: {response.status_code}")
        return None
    
def formatar_corpo_email(info_mensagem):
    return f"""
    <html>
    <body style="margin: 0; padding: 0; background-color: #f4f4f9; font-family: Arial, sans-serif;">
        <table width="100%" cellpadding="0" cellspacing="0" border="0" style="background-color: #f4f4f9; padding: 20px 0;">
            <tr>
                <td align="center">
                    <table width="600" cellpadding="0" cellspacing="0" border="0" style="background-color: #ffffff; border-radius: 8px; overflow: hidden;">
                        <!-- Cabeçalho -->
                        <tr>
                            <td style="background-color: #4b0082; padding: 20px; text-align: center; color: white;">
                                <br>
                                <h1 style="font-size: 24px; margin: 0; padding-top: 10px; color: white;">Relatório Exportado</h1><br>
                            </td>
                        </tr>
                        <!-- Conteúdo -->
                        <tr>
                            <td style="padding: 30px 20px; font-size: 16px; color: #333333;">
                                <p style="margin: 0 0 15px 0;">Olá,</p>
                                <p style="margin: 0 0 15px 0;">O arquivo do relatório: <strong>{info_mensagem}</strong> está anexado a este e-mail.</p>
                                <p style="margin: 0;">Por favor, revise o conteúdo anexado e entre em contato caso precise de mais informações.</p>
                            </td>
                        </tr>
                        <!-- Rodapé -->
                        <tr>
                            <td style="background-color: #4b0082; padding: 15px; text-align: center; color: white; font-size: 14px;">
                                <p style="margin: 0;">Atenciosamente,</p>
                                <p style="margin: 0;">Equipe de suporte da Vivo Telefônica</p>
                            </td>
                        </tr>
                    </table>
                </td>
            </tr>
        </table>
    </body>
    </html>
    """

def enviar_email(destinatarios, assunto, formato_arquivo, caminho_anexo=None, nome_anexo=None, info_mensagem=None):
    # Cria a mensagem do e-mail
    msg = MIMEMultipart()
    msg['From'] = SMTP_MAIL_FROM
    msg['To'] = ', '.join(destinatarios)
    msg['Subject'] = assunto

    # Formatar o corpo do e-mail de acordo com o formato de arquivo (xlsx ou csv)
    corpo_email = formatar_corpo_email(info_mensagem)

    # Adiciona o corpo do e-mail (em HTML)
    msg.attach(MIMEText(corpo_email, 'html'))

    # Adiciona o anexo, se houver
    if caminho_anexo:
        try:
            with open(caminho_anexo, 'rb') as anexo:
                part = MIMEBase('application', 'octet-stream')
                part.set_payload(anexo.read())
                encoders.encode_base64(part)
                
                # Usa o nome_anexo se fornecido, caso contrário, extrai do caminho_anexo
                if not nome_anexo:
                    nome_anexo = os.path.basename(caminho_anexo)
                
                part.add_header(
                    'Content-Disposition',
                    f'attachment; filename="{nome_anexo}.{formato_arquivo}"',
                )
                msg.attach(part)
        except Exception as e:
            print(f"❌ Erro ao adicionar anexo: {e}")
            return

    try:
        # Conecta ao servidor SMTP
        with smtplib.SMTP(SMTP_HOST, SMTP_PORT) as server:
            # Se necessário, faça login
            if SMTP_USER:
                try:
                    server.login(SMTP_USER, SMTP_PWD)
                except Exception as e_login:
                    print(f"❌ Erro ao autenticar no SMTP: {e_login}")
                    return

            # Envia o e-mail para múltiplos destinatários
            server.sendmail(SMTP_MAIL_FROM, destinatarios, msg.as_string())
            print("E-mail enviado com sucesso!")
    except Exception as e:
        print(f"Erro ao enviar e-mail: {e}")
        raise

def main():
    try:
        # Conecta ao Kafka no ambiente de produção
        kafka = KafkaConnector(
            topic_var_name=DEFAULT_TOPIC,
            kafka_url_var_name="prod_kafka_url",
            kafka_port_var_name="prod_kafka_port",
            kafka_variable_pem_content="pem_content",
            kafka_connection_id="kafka_default"
        )
    except Exception as e_kafka:
        print(f"❌ Erro ao instanciar KafkaConnector: {e_kafka}")
        raise

    print(f"⏳ Coletando mensagens para envio de reports...")

    try:
        # Em vez da linha abaixo:
        # consumer = kafka.create_kafka_connection('Consumer', CG_ID)
        # Vamos usar a NOVA função, sem mexer no resto do arquivo
        consumer = kafka.create_kafka_consumer_manual_offset(
            consumer_name=CG_ID,
            extra_consumer_configs={
                "enable.auto.commit": False,
                "auto.offset.reset": "earliest"
            }
        )
        
        messagens = kafka.process_messages(consumer)
    except Exception as e_consumer:
        print(f"❌ Erro ao criar ou processar mensagens do consumer: {e_consumer}")
        raise

    print(f'♨ Total de mensagens recebidas: {len(messagens)}')

    if messagens:
        print("⌛ Inicio do processo das mensagens...")
        for mensagem in messagens:
            try:
                storekey = mensagem['storekey']
                exportFormat = mensagem['exportFormat']
                recipients = mensagem['recipients'].strip('[]').split(',')
                
                print(f"📂 Formato: {exportFormat}")
                print(f"🔑 Chave para download: {storekey}")
                print(f"✉ Destinatários: {recipients}")

                caminho_arquivo = baixar_arquivo(formato=exportFormat, key=storekey)
                if not caminho_arquivo:
                    print("❌ Arquivo não baixado, pulando envio de e-mail.")
                    # NÃO comita offset => reprocessa na próxima execução
                    continue

                assunto = str(storekey).replace(exportFormat, '').replace('exports/', '')
                nome_anexo = ajustar_nome_anexo(assunto)
                info_mensagem = storekey.replace('exports/', '')

                enviar_email(
                    destinatarios=recipients,
                    assunto=f"Envio do report agendado: {assunto}",
                    formato_arquivo=exportFormat,
                    caminho_anexo=caminho_arquivo,
                    nome_anexo=nome_anexo,
                    info_mensagem=info_mensagem
                )

                # Se chegou até aqui, podemos commitar manualmente
                consumer.commit(asynchronous=False)
                print("✔ Mensagem processada e offset comitado com sucesso.")

            except Exception as e_msg:
                print(f"❌ Erro ao processar a mensagem {mensagem}: {e_msg}")
                # Sem commit => volta na próxima execução
    else:
        print(f"🆗 Nenhuma mensagem para processar...")  
        raise AirflowSkipException

# Definindo as tarefas
start = DummyOperator(
    task_id='start',
    dag=dag
)

process_dashboards_task = PythonOperator(
    task_id='process_messages',
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
start >> process_dashboards_task >> end


