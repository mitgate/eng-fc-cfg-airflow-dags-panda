from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.operators.dummy import DummyOperator
from airflow.utils.task_group import TaskGroup
from datetime import datetime, timedelta
import time
import os
import threading
import boto3 

# Função para deletar objetos no S3
def delete_objects():
    # Configurações do S3
    s3_config = {
        "access_key": "9I24H1Q8D3CAO0WWNDXJ",
        "secret_key": "n5Pbp68CzwX9jrXsK6hwT82bCnRsDKS7vQLNmaB0",
        "bucket_name": "panda-druid-dev-3b31c2d1-f1d3-4d0a-b34f-8a2364e45df9",
        "region": "",  # Preencha com a região do seu bucket, se aplicável
        "endpoint_url": "http://10.215.44.100:8080",  # Endpoint personalizado
        "use_path_style": True,  # Definido como True conforme as configurações fornecidas
        "baseKey": "druid/segments/"
    }

    # Criar cliente S3
    s3_client = boto3.client(
        "s3",
        region_name=s3_config["region"],
        aws_access_key_id=s3_config["access_key"],
        aws_secret_access_key=s3_config["secret_key"],
        endpoint_url=s3_config["endpoint_url"],
        use_ssl=False,  # Desabilitar SSL para o protocolo HTTP
        config=boto3.session.Config(signature_version="s3v4")
    )

    # Deletar todos os objetos no diretório especificado no bucket
    count = 0
    count_response = 1
    while count_response > 0: 
        try: 
            response = s3_client.list_objects(Bucket=s3_config["bucket_name"], Prefix=s3_config["baseKey"])
            if "Contents" in response:
                count_response = len(response["Contents"]) 
                objects_to_delete = [{"Key": obj["Key"]} for obj in response["Contents"]]
                deleted_objects = s3_client.delete_objects(Bucket=s3_config["bucket_name"], Delete={"Objects": objects_to_delete})
                print(f"{len(deleted_objects['Deleted'])} objetos foram deletados.")
                count += count_response
            else:
                count_response = 0
                print("Nenhum objeto encontrado no diretório especificado.")
        except:
            print('Algo deu errado... Tentando novamente em 1 segundo...')    
            time.sleep(1)
    print("Deleção de objetos concluída.")

default_args = {
    'owner': 'Sadir',
    'depends_on_past': False,
    'start_date': datetime(2024, 4, 5),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 0,
    'retry_delay': timedelta(minutes=5),
}
with DAG(
    'kill_bucket_ceph',
    default_args=default_args,
    description='Deletar objetos no bucket S3',
    schedule_interval=None,
) as dag:
    start_task = DummyOperator(task_id='start_task', dag=dag)
    end_task = DummyOperator(task_id='end_task', dag=dag)
    with TaskGroup('delete_ceph_group', dag=dag) as tasks_delete_group:
        
        task1_delete_objects = PythonOperator(
            task_id='delete_objects_1',
            python_callable=delete_objects,
            dag=dag,
        )
        task2_delete_objects = PythonOperator(
            task_id='delete_objects_2',
            python_callable=delete_objects,
            dag=dag,
        )
        task3_delete_objects = PythonOperator(
            task_id='delete_objects_3',
            python_callable=delete_objects,
            dag=dag,
        )
        task4_delete_objects = PythonOperator(
            task_id='delete_objects_4',
            python_callable=delete_objects,
            dag=dag,
        )
        task1_delete_objects, task2_delete_objects, task3_delete_objects, task4_delete_objects
    start_task >> tasks_delete_group >> end_task
