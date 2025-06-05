# Nome da DAG: druid_tasks_aut_compact_netcompass_v1
# Owner / responsável: airflow
# Descrição do objetivo da DAG: Retorno esperado:
# {
#   'menor_data': f'{min_valid_date}',
#   'maior_data' f'{max_valid_date}'
# }
# Usa Druid?: Sim
# Principais tabelas / consultas Druid acessadas: sys.segments, sys.tasks
# Frequência de execução (schedule): 0 0 * * *
# Dag Activo?: 
# Autor: airflow
# Data de modificação: 2025-05-26
from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.utils.dates import days_ago
from datetime import datetime, timedelta
from airflow.models import Variable
from airflow.operators.dummy import DummyOperator
from airflow.exceptions import AirflowSkipException
from datetime import datetime, timedelta
import requests
import re
import json
import os
DRUID_URL = Variable.get("druid_url")
DRUID_MAX_ROWS_CAMPACT = Variable.get("druid_maxrowspersegment")
DRUID_TOTAL_ROWS_CAMPACT = Variable.get("druid_maxtotalrows")
DRUID_MAX_ROWS_IN_MEMORY = Variable.get("druid_maxrowsinmemory")

DEV = True
DATASOURCE='module-metrics-netcompass_v1'
# Valida se a data retornada está dentro do período
def verificar_data_no_periodo(data_para_validar, data_inicio, data_fim):
    print(type(data_para_validar))
    print(type(data_inicio))
    print(type(data_fim))
    data_inicio = datetime.strptime(data_inicio, "%Y-%m-%d").date()
    data_fim = datetime.strptime(data_fim, "%Y-%m-%d").date()
    if data_inicio <= data_para_validar <= data_fim:
        return True
    else:
        return False

# Função pegar as taks em execução
def get_date_druid(**kwargs):
    ti = kwargs['ti']

    # Melhoria, pegar url da variável do Airflow
    url = f"{DRUID_URL}/druid/v2/sql"

    # Melhoria, pegar url da variável do Airflow
    payload = json.dumps({
        "query": f"\nSELECT\n  \"datasource\",\n \n \"start\"\nFROM sys.segments\nWHERE CAST(REPLACE(CAST(\"start\" AS VARCHAR(19)),'T',' ') AS TIMESTAMP) <= DATE_TRUNC('day',CURRENT_TIMESTAMP) - INTERVAL '1' day \n  AND CAST(REPLACE(CAST(\"start\" AS VARCHAR(19)),'T',' ') AS TIMESTAMP) > DATE_TRUNC('day',CURRENT_TIMESTAMP) - INTERVAL '2' day\n  AND datasource = '{DATASOURCE}' and num_rows < 10000000\n\n",
        "resultFormat": "object",
        "context": {
            "executionMode": "ASYNC"
        }
    })
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7,es;q=0.6',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json'
    }
    if DEV:
        print("Iniciando coleta no Druid..")
    response = requests.request("POST", url, headers=headers, data=payload)

    # Parse the response JSON
    response_data = response.json()
    if DEV:
        print("aaaaaaaaaaaa - response_data", response_data)
    
    # Extract the data from the response
    data = response_data
    if DEV:
        print("aaaaaaaaaaaa - data", data)

    # Extract the dates from the data
    dates = [entry['start'] for entry in data]

    # Expressão regular para verificar o formato da data 'YYYY-MM-DDTHH:MM:SS.mmmZ'
    date_format_regex = re.compile(r'^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3}Z$')

    # Lista para armazenar as datas válidas
    valid_dates = []

    # Itera sobre as datas
    for date in dates:
        # Verifica se a data está no formato esperado
        if date_format_regex.match(date):
            # Se estiver no formato correto, adiciona à lista de datas válidas
            valid_dates.append(date)

    # Converte as datas válidas em objetos datetime
    valid_datetime_dates = [datetime.strptime(date, '%Y-%m-%dT%H:%M:%S.%fZ') for date in valid_dates]

    # Extrai somente o ano, mês e dia das datas válidas
    valid_dates_only = [date.strftime('%Y-%m-%d') for date in valid_datetime_dates]

    # Encontra a menor e a maior data dentre as datas válidas
    min_valid_date = min(valid_dates_only)
    max_valid_date = max(valid_dates_only)

    print("Menor data válida:", min_valid_date)
    print("Maior data válida:", max_valid_date)

    data = {
        'menor_data': f'{min_valid_date}',
        'maior_data': f'{max_valid_date}'
    }
    kwargs['ti'].xcom_push(key='datas_do_druid', value=data)

# Função pegar as taks em execução
def get_tasks_in_execution(**kwargs):
     
    ti = kwargs['ti']

    url = f"{DRUID_URL}/druid/v2/sql"
    payload = json.dumps({
        "query": f"SELECT\n    \"task_id\"\nFROM sys.tasks\nWHERE datasource = '{DATASOURCE}'\nAND status = 'RUNNING'\nAND task_id like 'compact_%'\n\n    \n",
        "resultFormat": "object",
        "context": {
        "executionMode": "ASYNC"
        }
    })
    
    headers = {
        'Accept': 'application/json, text/plain, */*',
        'Accept-Language': 'pt-BR,pt;q=0.9,en-US;q=0.8,en;q=0.7,es;q=0.6',
        'Connection': 'keep-alive',
        'Content-Type': 'application/json'
    }
    if DEV:
        print(url)
        print(payload)
        print(headers)
    response = requests.post(url, headers=headers, data=payload, verify=False)
    if DEV:
        print(response.text)
    data_return_tasks = response.json()
    print(f"Tasks em execução:{data_return_tasks}")
    kwargs['ti'].xcom_push(key='get_tasks_in_execution', value=data_return_tasks)

# Função para receber as taks e validar se estão em execução via processo automático.
def receive_tasks_in_execution_and_campare(**kwargs):

    ti = kwargs['ti']
    data = ti.xcom_pull(task_ids='get_dates_from_druid', key='datas_do_druid')
    """
    Retorno esperado:
        {
            'menor_data': f'{min_valid_date}',
            'maior_data' f'{max_valid_date}'
        }
    """
    tasks_in_execution = ti.xcom_pull(task_ids='get_tasks_in_execution', key='get_tasks_in_execution')
    """
    Retorno esperado:
        [
            {
                "task_id": "compact_module-metrics-netcompass_v1_kldmhlin_2024-04-03T19:38:29.116Z"
            },
            {
                "task_id": "compact_module-metrics-netcompass_v1_dodcaedo_2024-04-03T19:43:56.917Z"
            },
            {
                "task_id": "compact_module-metrics-netcompass_v1_pjgikpok_2024-04-03T23:51:29.360Z"
            }
        ]
    """
    print("DATA:",data)
    print("TASKS EM EXECUCAO:",tasks_in_execution)
    validado = False
    for task in tasks_in_execution:
        if DEV:
            print(f"Validando TASK_ID: {task['task_id']}")

        url = f"{DRUID_URL}/druid/indexer/v1/task/{task['task_id']}"
        if DEV:
            print("Validando URL",url)
        response = requests.get(url, verify=False)

        response_json = response.json()
        
        interval = response_json["payload"]["ioConfig"]["inputSpec"]["interval"]

        # Dividir a string em duas datas
        datas = interval.split('/')

        # Converter cada parte da string para um objeto de data
        data1 = datetime.fromisoformat(datas[0].split('T')[0]).date()
        data2 = datetime.fromisoformat(datas[1].split('T')[0]).date()

        # Imprimir as datas
        if DEV:
            print("Data encontra(interval) 1:", data1)
            print("Data encontra(interval) 2:", data2)
       
        if verificar_data_no_periodo(data_para_validar=data1,
                data_inicio=data['menor_data'],
                data_fim=data['maior_data']
            ) or verificar_data_no_periodo(
                data_para_validar=data2,
                data_inicio=data['menor_data'],
                data_fim=data['maior_data']):
            kwargs['ti'].xcom_push(key='skip_tasks_druid', value=True)
            validado = True
            break
    if validado == False:
    	    kwargs['ti'].xcom_push(key='skip_tasks_druid', value=False)
    
# Retorna para taks do airflow que irá startar as taks no Druid se deverá ou não ser executada (True irá pular o processo).
def validate_start_taks_druid(**kwargs):
    ti = kwargs['ti']
    return ti.xcom_pull(task_ids='receive_tasks_in_execution_and_campare', key='skip_tasks_druid')

def start_tasks_compact_in_druid(**kwargs):
    status_running_taks = kwargs.get('status_running_taks',False)
    if status_running_taks:
        raise AirflowSkipException('Estapa pulada por não ser necessária a intervenção automática.')
    else:
        ti = kwargs['ti']
        datas_do_druid = ti.xcom_pull(task_ids='get_dates_from_druid', key='datas_do_druid')
        """
        Retorno esperado:
            {
                'menor_data': f'{min_valid_date}',
                'maior_data' f'{max_valid_date}'
            }
        """
        maior_data = datas_do_druid['maior_data']
        menor_data = datas_do_druid['menor_data']
        
        # Melhoria, pegar url da variável do Airflow
        url = f"{DRUID_URL}/druid/indexer/v1/task"
        headers = {
            'Content-Type': 'application/json'
        }
        # Melhoria, pegar url da variável do Airflow
        payload = json.dumps(
            {
                "type": "compact",
                "dataSource": DATASOURCE,
                "interval": f"{menor_data}/{maior_data}",
                "tuningConfig": {
                    "type": "index_parallel",
                    "partitionsSpec": {
                    "type": "dynamic",
                    "maxRowsPerSegment": DRUID_MAX_ROWS_CAMPACT,
                    "maxTotalRows": DRUID_TOTAL_ROWS_CAMPACT
                    },
                    "maxRowsInMemory": DRUID_MAX_ROWS_IN_MEMORY
                }
            }
        )

        response = requests.request("POST", url, headers=headers, data=payload)
        print(f"Task enviada. Id retorno: {response.status_code} | Mensagem: {response.text}")
# Definindo argumentos padrão da DAG
"""
{
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': days_ago(2),
    'email': ['airflow@example.com'],
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
    # 'queue': 'bash_queue',
    # 'pool': 'backfill',
    # 'priority_weight': 10,
    # 'end_date': datetime(2016, 1, 1),
    # 'wait_for_downstream': False,
    # 'dag': dag,
    # 'sla': timedelta(hours=2),
    # 'execution_timeout': timedelta(seconds=300),
    # 'on_failure_callback': some_function,
    # 'on_success_callback': some_other_function,
    # 'on_retry_callback': another_function,
    # 'sla_miss_callback': yet_another_function,
    # 'trigger_rule': 'all_success'
}
"""
default_args = {
    'owner': 'Leandro',
    'depends_on_past': False,
    'start_date': days_ago(1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5) 
}

# Definindo a DAG
with DAG(
    'druid_auto_compactation_netcompass_v1',
    default_args=default_args,
    description="""
    
    Consulta o período do dia antetior, 
    coleta as datas de compactação com volume de registros menor do que 10mi (milhões) 
    e realiza o agendamento da compactação automática.
    
    """,
    schedule_interval='0 0 * * *',
    tags=["druid"],
    max_active_runs=1  
) as dag:
    start_task = DummyOperator(task_id='start_task', dag=dag)

    get_date_druid_ = PythonOperator(
        task_id='get_dates_from_druid',
        python_callable=get_date_druid,
        provide_context=True,
    )
    
    get_tasks_in_execution = PythonOperator(
        task_id='get_tasks_in_execution',
        python_callable=get_tasks_in_execution,
        provide_context=True,
    )

    receive_tasks_in_execution_and_campare = PythonOperator(
        task_id='receive_tasks_in_execution_and_campare',
        python_callable=receive_tasks_in_execution_and_campare,
        provide_context=True,
    )
    validate_start_taks_druid = PythonOperator(
        task_id='validate_start_taks_druid',
        python_callable=validate_start_taks_druid,
        provide_context=True,
    )
    
    start_tasks_compact_in_druid = PythonOperator(
        task_id='start_tasks_compact_in_druid',
        python_callable=start_tasks_compact_in_druid,
        provide_context=True,
         op_kwargs={"status_running_taks": validate_start_taks_druid.output}
    )
 
    end_task = DummyOperator(task_id='end_task', dag=dag)
    # Definindo a ordem das tarefas
    start_task >> get_date_druid_ >> get_tasks_in_execution >> receive_tasks_in_execution_and_campare >> validate_start_taks_druid >> start_tasks_compact_in_druid >> end_task