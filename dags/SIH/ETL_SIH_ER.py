from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import boto3
import botocore
import os

def verificar_conexao_minio(**kwargs):
    try:
        # MinIO client configuration
        minio_client = boto3.client(
            's3',
            endpoint_url='http://10.100.100.61:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            region_name='us-east-1',
        )
        
        bucket_name = 'bronze-delta'
        
        # Attempting to head the bucket
        print(f"Verificando a existência do bucket '{bucket_name}'.")
        minio_client.head_bucket(Bucket=bucket_name)
        print(f"Bucket '{bucket_name}' existe e está acessível.")
        
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"Erro ao acessar o MinIO: {error_code} - {e}")
        if error_code == 'NoSuchBucket':
            print(f"Bucket '{bucket_name}' não existe.")
        elif error_code == 'InvalidRequest':
            print(f"Solicitação inválida. Verifique o nome do bucket e as configurações.")
        else:
            print(f"Erro desconhecido: {error_code} - {e}")
        raise  # Re-raise the exception to fail the task

def baixar_arquivos(**kwargs):
    try:
        # MinIO client configuration
        minio_client = boto3.client(
            's3',
            endpoint_url='http://10.100.100.61:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            region_name='us-east-1',
        )

        bucket_name = 'bronze-delta'
        prefix = 'datasus_sia_delta/ACFMG2001.delta/'
        download_dir = '/home/jamilsonfs/airflow/dags/SIA/tmp/DELTA'
        
        # Ensure the base download directory exists
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)

        # Create a subdirectory based on the prefix
        subdir_name = prefix.split('/')[1]  # Extracting 'ACFMG2101.delta' from the prefix
        subdir_path = os.path.join(download_dir, subdir_name)
        if not os.path.exists(subdir_path):
            os.makedirs(subdir_path)

        # List objects with the specified prefix
        print(f"Listando objetos com o prefixo '{prefix}' no bucket '{bucket_name}'.")
        response = minio_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
        
        if 'Contents' in response:
            for obj in response['Contents']:
                key = obj['Key']
                print(f"Objeto encontrado: {key}")
                
                # Define the local file path within the subdirectory
                file_name = os.path.basename(key)
                file_path = os.path.join(subdir_path, file_name)
                
                # Download the object
                print(f"Baixando o objeto '{key}' para '{file_path}'.")
                minio_client.download_file(Bucket=bucket_name, Key=key, Filename=file_path)
                print(f"Objeto '{file_name}' baixado com sucesso.")
        else:
            print(f"Nenhum objeto encontrado com o prefixo '{prefix}' no bucket '{bucket_name}'.")
        
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"Erro ao acessar o MinIO: {error_code} - {e}")
        if error_code == 'NoSuchBucket':
            print(f"Bucket '{bucket_name}' não existe.")
        elif error_code == 'InvalidRequest':
            print(f"Solicitação inválida. Verifique o nome do bucket e o prefixo.")
        else:
            print(f"Erro desconhecido: {error_code} - {e}")
        raise  # Re-raise the exception to fail the task

# Define the DAG (Directed Acyclic Graph) in Airflow
with DAG(
    dag_id='ETL_SIH_ER',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 8, 14),
        'email_on_failure': False,
        'email_on_retry': False,
    },
    description='DAG para baixar arquivos relacionados a ACFMG2101 do bucket do MinIO',
    schedule_interval='@daily',  # Adjust the schedule as needed
    catchup=False,
) as dag:
    # Task to check MinIO connection
    verificar_conexao_task = PythonOperator(
        task_id='verificar_conexao',
        python_callable=verificar_conexao_minio,
        provide_context=True,
    )
    verificar_conexao_task.ui_color = '#FF6600'

    # Task to download files from MinIO
    baixar_arquivos_task = PythonOperator(
        task_id='baixar_arquivos',
        python_callable=baixar_arquivos,
        provide_context=True,
    )
    baixar_arquivos_task.ui_color = '#3366ff'

    verificar_conexao_task >> baixar_arquivos_task
