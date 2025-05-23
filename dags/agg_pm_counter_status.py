import json
import logging
import tempfile
from datetime import datetime, timedelta, timezone

import pandas as pd
import requests
from airflow import DAG
from airflow.exceptions import AirflowSkipException
from airflow.hooks.base import BaseHook
from airflow.hooks.postgres_hook import PostgresHook
from airflow.models import Variable
from airflow.operators.python import PythonOperator
from confluent_kafka import Producer

#
# 1. Parâmetros da DAG
#
DAG_ID = 'agg_pm_counter_status'
DEFAULT_ARGS = {
    'owner': 'leandro',
    'depends_on_past': False,
    'retries': 0,
    'execution_timeout': timedelta(minutes=30),
    'email_on_failure': False,
    'email_on_retry': False,
}

dag = DAG(
    dag_id=DAG_ID,
    default_args=DEFAULT_ARGS,
    description=(
        "Monitora diariamente o status de coleta de cada equipamento PAI "
        "(AdditionalDN) no tópico Druid fastoss-pm-counters; mantém histórico "
        "full/incremental em Postgres via UPSERT; marca OFFLINE quem não retornar; "
        "fallback por sourceSystem; publica em Kafka para Druid/Superset."
    ),
    schedule_interval='0 2 * * *',
    start_date=datetime(2024, 5, 10),
    catchup=False,
    max_active_runs=1,
    tags=['pm-counters', 'monitoring'],
)

#
# 2. Conexões e Variáveis
#
DRUID_CONN_ID = 'druid_broker_default'        # ajuste se necessário
POSTGRES_CONN_ID = 'postgres_prd'             # ajuste para seu banco de histórico
KAFKA_CONN_ID = 'kafka_default'

DRUID_URL = BaseHook.get_connection(DRUID_CONN_ID).host
PM_TOPIC      = Variable.get('agg_counters_topic_pmsu')
STATUS_TOPIC  = Variable.get('agg_pm_collector_status_topic', 'fastoss-pm-collector-status')
KAFKA_URL     = Variable.get('prod_kafka_url')
KAFKA_PORT    = Variable.get('prod_kafka_port')
PEM_CONTENT   = Variable.get('pem_content')

# controla first vs incremental
FULL_LOAD_FLAG = 'agg_pm_counter_status_full_load_done'


def query_druid(full_load: bool) -> pd.DataFrame:
    """
    Executa a query no Druid via HTTP/SQL. Se falhar com 500, refaz com filtro
    sourceSystem em ('OSS-RC','Taishan Core').
    """
    interval = '7' if full_load else '2'
    base_where = f"__time > CURRENT_TIMESTAMP - INTERVAL '{interval}' DAY"
    sql = (
        f"SELECT additionalDn AS device, MAX(__time) AS lastSeen "
        f"FROM \"{PM_TOPIC}\" "
        f"WHERE {base_where} "
        "GROUP BY additionalDn"
    )
    payload = {"query": sql, "resultFormat": "object"}
    headers = {'Content-Type': 'application/json'}
    url = f"{DRUID_URL}/druid/v2/sql"

    def _run_query(query_payload):
        r = requests.post(url, headers=headers, data=json.dumps(query_payload), verify=False, timeout=60)
        r.raise_for_status()
        return pd.DataFrame(r.json())

    try:
        df = _run_query(payload)
    except requests.HTTPError as e:
        logging.warning("Druid returned error, tentando fallback por sourceSystem...")
        payload['query'] = sql + " AND sourceSystem IN ('OSS-RC','Taishan Core')"
        df = _run_query(payload)

    logging.info(f"Registros retornados do Druid: {len(df)}")
    return df


def process_and_upsert(df: pd.DataFrame):
    """
    Calcula offlineHours/status, faz UPSERT em tabela auxiliar e detecta
    equipamentos ausentes para marcar OFFLINE.
    """
    if df.empty:
        raise AirflowSkipException("Nenhum registro do Druid para processar.")

    now_utc = datetime.now(timezone.utc)
    df['lastSeen'] = pd.to_datetime(df['lastSeen'], utc=True)
    df['offlineHours'] = ((now_utc - df['lastSeen']).dt.total_seconds() / 3600).round(3)
    df['status'] = df['offlineHours'].apply(lambda h: 'ONLINE' if h <= 1 else 'OFFLINE')
    df['dt_load'] = now_utc

    # Conecta no Postgres
    pg = PostgresHook(postgres_conn_id=POSTGRES_CONN_ID)
    conn = pg.get_conn()
    cur = conn.cursor()

    # Tabela auxiliar
    cur.execute("""
        CREATE TABLE IF NOT EXISTS pm_collector_status_history (
            device TEXT PRIMARY KEY,
            lastSeen TIMESTAMP,
            offlineHours FLOAT,
            status TEXT,
            dt_load TIMESTAMP
        )
    """)
    conn.commit()

    # UPSERT batch
    for _, row in df.iterrows():
        cur.execute("""
            INSERT INTO pm_collector_status_history
              (device, lastSeen, offlineHours, status, dt_load)
            VALUES (%s,%s,%s,%s,%s)
            ON CONFLICT (device) DO UPDATE
              SET lastSeen      = EXCLUDED.lastSeen,
                  offlineHours  = EXCLUDED.offlineHours,
                  status        = EXCLUDED.status,
                  dt_load       = EXCLUDED.dt_load
        """, (row.device, row.lastSeen.to_pydatetime(), row.offlineHours, row.status, row.dt_load))
    conn.commit()

    # Detecta ausentes (que não apareceram nesta batch)
    cur.execute("SELECT device, lastSeen FROM pm_collector_status_history")
    all_current = {r[0]: r[1] for r in cur.fetchall()}
    missing = set(all_current) - set(df['device'])
    for device in missing:
        last_seen = all_current[device]
        offline_h = ((now_utc - last_seen).total_seconds() / 3600).round(3)
        cur.execute("""
            UPDATE pm_collector_status_history
               SET offlineHours=%s, status='OFFLINE', dt_load=%s
             WHERE device=%s
        """, (offline_h, now_utc, device))
    conn.commit()
    cur.close()
    conn.close()

    logging.info(f"Upsert concluído ({len(df)} online/offline recalculados, {len(missing)} ausentes marcados).")
    return df[['device','lastSeen','offlineHours','status','dt_load']]


def publish_to_kafka(df: pd.DataFrame):
    """
    Publica cada linha do DataFrame no tópico Kafka para streaming ao Druid.
    """
    # salva PEM num arquivo temporário
    with tempfile.NamedTemporaryFile(delete=False) as f:
        f.write(PEM_CONTENT.encode())
        pem_path = f.name

    producer = Producer({
        'bootstrap.servers': f"{KAFKA_URL}:{KAFKA_PORT}",
        'security.protocol': 'SSL',
        'ssl.ca.location': pem_path,
    })

    def cb(err, msg):
        if err:
            logging.error(f"Erro Kafka: {err}")

    for _, row in df.iterrows():
        payload = row.to_json(date_format='iso')
        producer.produce(STATUS_TOPIC, payload.encode('utf-8'), callback=cb)
    producer.flush()
    logging.info(f"Publicado {len(df)} mensagens no Kafka ({STATUS_TOPIC}).")


def workflow(**context):
    # 1) controla first vs incremental via Variable
    full_load_done = Variable.get(FULL_LOAD_FLAG, default_var='false') == 'true'
    full_load = not full_load_done

    # 2) query
    df = query_druid(full_load=full_load)

    # 3) processa e upserta, recebe DataFrame final
    df_final = process_and_upsert(df)

    # 4) publica no Kafka
    publish_to_kafka(df_final)

    # 5) marca que o full load já aconteceu
    if full_load:
        Variable.set(FULL_LOAD_FLAG, 'true')


with dag:
    run = PythonOperator(
        task_id='run_all',
        python_callable=workflow,
        provide_context=True,
    )
    run
