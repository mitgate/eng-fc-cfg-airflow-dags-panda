# Nome da DAG: warmup_core_monitoramento
# Owner / responsável: Sadir
# Descrição do objetivo da DAG: DAG para filtrar dashboards contendo (5G) e enviar dados para aquecimento de cache
# Usa Druid?: Não
# Principais tabelas / consultas Druid acessadas: 
# Frequência de execução (schedule): 20 1-23/2 * * *
# Dag Activo?: 
# Autor: Sadir
# Data de modificação: 2025-05-26

from airflow import DAG
from airflow.operators.python import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.models import Variable
from airflow.utils.dates import datetime, timedelta
import requests
import json
from airflow.exceptions import AirflowSkipException

# Obtendo variáveis do Airflow
TAG_FILTRO = Variable.get("tag_warmup_monitoramento")
SUPERSET_URL = Variable.get("superset_prd_host")

default_args = {
    'owner': 'Sadir',
    'depends_on_past': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}
# Instanciando a DAG
dag = DAG(
    'Monitoramento',
    default_args=default_args,
    description='DAG para filtrar dashboards contendo (5G) e enviar dados para aquecimento de cache',
    schedule_interval='20 1-23/2 * * *',  # Executa a cada 2 horas
    start_date=datetime(2024, 9, 9),
    catchup=False,
    max_active_runs=1, 
    tags=['warmup', 'superset'],
)
def process_dashboards(**kwargs):
    # Função para obter e processar os dashboards
    url = f"http://{SUPERSET_URL}/api/v1/chart/data?form_data=%7B%22slice_id%22%3A806%7D"
    
    payload = json.dumps({
        "datasource": {
            "id": 61,
            "type": "table"
        },
        "force": True,
        "queries": [
            {
                "applied_time_extras": {},
                "columns": [
                    "dashboard_id",
                    "dashboard_name",
                    "chart_id",
                    "chart_name"
                ],
                "row_limit": 1000000,
                "series_limit": 0,
                "order_desc": True,
                "url_params": {
                    "datasource_id": "61",
                    "datasource_type": "table",
                    "save_action": "saveas",
                    "slice_id": "806"
                }
            }
        ],
        "result_format": "json"
    })
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }
    response = requests.post(url, headers=headers, data=payload)

    if response.status_code == 200:
        print("✅ | Sucesso ao exportar lista de dashboards... 👌")
        dashboards = response.json()['result'][0]['data']
        # Filtrar dashboards com "(5G)"
        filtered_data = [item for item in dashboards if any(TAG_FILTRO in str(value) for value in item.values())]

        # Processar cada dashboard filtrado
        for dashboard in filtered_data:
            chart_id = dashboard['chart_id']
            dashboard_id = dashboard['dashboard_id']
            chart_name = dashboard['chart_name']
            dashboard_name = dashboard['dashboard_name']
            
            try:
                warm_up_cache(chart_id, dashboard_id, chart_name, dashboard_name)
            except AirflowSkipException as e:
                print(f'Skipped warming up cache for dashboard {dashboard_id}: {e}')
    else:
        raise Exception("❌ | Erro ao extrair lista de dashboards!")

def warm_up_cache(chart_id, dashboard_id, chart_name, dashboard_name):
    # Função para aquecer o cache
    url = f"http://{SUPERSET_URL}/api/v1/chart/warm_up_cache"
    
    payload = json.dumps({
        "chart_id": int(chart_id),
        "dashboard_id": int(dashboard_id)
    })
    headers = {
        'Accept': 'application/json',
        'Content-Type': 'application/json'
    }

    response = requests.put(url, headers=headers, data=payload)
    if response.status_code == 200:
        print(f'✅ | Dash: {dashboard_name}({dashboard_id}) | Graf: {chart_name}({chart_id})')
    else:
        print(f'❌ | Dash: {dashboard_name}({dashboard_id}) | Graf: {chart_name}({chart_id}) - Skipped')
        raise AirflowSkipException(f"Failed to warm up cache for dashboard {dashboard_id}")



# Definindo as tarefas
start = DummyOperator(
    task_id='start',
    dag=dag
)

process_dashboards_task = PythonOperator(
    task_id='process_dashboards',
    python_callable=process_dashboards,
    provide_context=True,
    execution_timeout=timedelta(minutes=60),  # Limita a execução da task a 60 minutos
    dag=dag
)

end = DummyOperator(
    task_id='end',
    dag=dag
)

# Definindo as dependências das tarefas
start >> process_dashboards_task >> end
