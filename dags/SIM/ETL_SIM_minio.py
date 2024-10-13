from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import boto3
import botocore
import os
import itertools
import pandas as pd
import json
import requests
from requests.auth import HTTPBasicAuth

anos = [2021]
estados = ['PB']

def baixar_arquivos_e_registrar_atlas(parquet_dir, atlas_url, atlas_username, atlas_password, **kwargs):
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

        for estado, ano in itertools.product(estados, anos):
            prefix = f'datasus_sia_delta/ACF{estado}{str(ano)[-2:]}01.delta/'
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
                    
                    # Chama a função de registrar no Atlas
                    guid_entidade = criar_entidade_atlas(file_path, atlas_url, 'admin', 'admin')
            else:
                print(f"Nenhum objeto encontrado com o prefixo '{prefix}' no bucket '{bucket_name}'.")
        
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"Erro ao acessar o MinIO: {error_code} - {e}")
        raise