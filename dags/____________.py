from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime

# Função que sempre falha
def fail_task():
    raise Exception("Esta tarefa falhou!")

# Definindo os argumentos padrão
default_args = {
    'owner': 'airflow',
    'start_date': datetime(2025, 3, 19),
    'email_on_failure': True,  # Habilita o envio de e-mail em caso de falha
    'email': ['leandro.rodriguezlopez@emeal.nttdata.com'],  # Substitua pelo seu e-mail
}

# Criando a DAG
dag = DAG(
    'test_email_on_failure',
    default_args=default_args,
    description='Uma DAG para testar o envio de e-mail em caso de falha',
    schedule_interval=None#'@once',  # Executa uma vez
)

# Tarefa que sempre falha
task_fail = PythonOperator(
    task_id='fail_task',
    python_callable=fail_task,
    dag=dag,
)

# Tarefa dummy que pode ser usada para encadear outras tarefas
task_start = DummyOperator(
    task_id='start',
    dag=dag,
)

# Definindo a ordem das tarefas
task_start >> task_fail