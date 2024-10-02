from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import boto3
import botocore
import os
import pandas as pd
import json
import itertools

estados = ['PB']
anos = [2021]


def baixar_arquivos(parquet_dir, **kwargs):
    try:
        minio_client = boto3.client(
            's3',
            endpoint_url='http://10.100.100.61:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            region_name='us-east-1',
        )

        bucket_name = 'bronze-delta'
        download_dir = parquet_dir
        
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)

        # Iterar pelos estados e anos para buscar os arquivos no MinIO
        for estado, ano in itertools.product(estados, anos):
            prefix = f'datasus_sih_delta/RD{estado}{str(ano)[-2:]}01.delta/'
            subdir_name = prefix.split('/')[1]
            subdir_path = os.path.join(download_dir, subdir_name)
            
            if not os.path.exists(subdir_path):
                os.makedirs(subdir_path)
            
            print(f"Listando objetos com o prefixo '{prefix}' no bucket '{bucket_name}'.")
            response = minio_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)

            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    file_name = os.path.basename(key)
                    file_path = os.path.join(subdir_path, file_name)

                    print(f"Baixando o objeto '{key}' para '{file_path}'.")
                    minio_client.download_file(Bucket=bucket_name, Key=key, Filename=file_path)
                    print(f"Objeto '{file_name}' baixado com sucesso.")
            else:
                print(f"Nenhum objeto encontrado com o prefixo '{prefix}' no bucket '{bucket_name}'.")
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"Erro ao acessar o MinIO: {error_code} - {e}")
        raise


def drop_columns_and_save(parquet_dir, json_file):
    with open(json_file, 'r') as f:
        drop_columns = json.load(f)

    for root, _, files in os.walk(parquet_dir):
        for file in files:
            if file.endswith('.parquet'):
                parquet_file_path = os.path.join(root, file)
                try:
                    print(f"Removendo colunas de {parquet_file_path}")
                    df = pd.read_parquet(parquet_file_path)
                    
                    df = df.drop(columns=drop_columns, errors='ignore')
                    
                    df.to_parquet(parquet_file_path, index=False)
                    print(f"Colunas removidas e dados salvos em Parquet: {parquet_file_path}")
                except Exception as e:
                    print(f"Erro ao processar o arquivo {parquet_file_path}: {e}")


# Definição da DAG
with DAG(
    dag_id='ETL_SIH_RD',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 8, 14),
        'email_on_failure': False,
        'email_on_retry': False,
    },
    description='DAG para baixar arquivos do MinIO e remover colunas específicas',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    baixar_arquivos_task = PythonOperator(
        task_id='baixar_arquivos',
        python_callable=baixar_arquivos,
        op_kwargs={
            'parquet_dir':  f'/home/jamilsonfs/airflow/dags/SIH/tmp/DELTA/RDPB2201.delta',
        }
    )

    drop_columns_task = PythonOperator(
        task_id='drop_columns',
        python_callable=drop_columns_and_save,
        op_kwargs={
            'parquet_dir': f'/home/jamilsonfs/airflow/dags/SIH/tmp/DELTA/RDPB2201.delta',
            'json_file': '/home/jamilsonfs/airflow/dags/SIH/files/column_names_to_drop.json'
        }
    )

    # Definir o fluxo das tasks
    baixar_arquivos_task >> drop_columns_task


baixar_arquivos_task.ui_color = '#3366ff'
drop_columns_task.ui_color = '#ffd966'
