from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import boto3
import botocore
import os
import itertools
import pyarrow.parquet as pq
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
            subdir_path = os.path.join(download_dir)

            
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
                    #guid_entidade = criar_entidade_atlas(file_path, atlas_url, 'admin', 'admin')
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

                    #criar_entidade_atlas_process(parquet_dir, 'http://10.100.100.61:21000', 'admin', 'admin', 'Drop de Colunas')
                except Exception as e:
                    print(f"Erro ao processar o arquivo {parquet_file_path}: {e}")

def group_transform(parquet_dir, json_file):
    with open(json_file, 'r') as f:
        transform_rules = json.load(f)

    for root, _, files in os.walk(parquet_dir):
        for file in files:
            if file.endswith('.parquet'):
                parquet_file_path = os.path.join(root, file)
                try:
                    print(f"Transformando {parquet_file_path}")
                    df = pd.read_parquet(parquet_file_path)

                    for column_name, mappings in transform_rules.items():
                        if column_name in df.columns:
                            df[column_name] = df[column_name].map(mappings).fillna(df[column_name])

                    df.to_parquet(parquet_file_path, index=False)
                    print(f"Dados transformados e salvos em Parquet: {parquet_file_path}")
                    #criar_entidade_atlas_process(parquet_file_path, 'http://10.100.100.61:21000', 'admin', 'admin', 'Group transform')
                except Exception as e:
                    print(f"Erro ao processar o arquivo {parquet_file_path}: {e}")

def names_column(parquet_dir, json_file_path):
    # Carrega o mapeamento de colunas do arquivo JSON
    with open(json_file_path, 'r') as f:
        column_mapping = json.load(f)

    for root, _, files in os.walk(parquet_dir):
        for file in files:
            if file.endswith('.parquet'):
                parquet_file_path = os.path.join(root, file)
                try:
                    print(f"Renomeando colunas no arquivo: {parquet_file_path}")
                    # Lê o arquivo Parquet
                    df = pd.read_parquet(parquet_file_path)

                    # Renomeia as colunas com base no mapeamento do JSON
                    df.rename(columns=column_mapping, inplace=True)

                    # Salva de volta no formato Parquet
                    df.to_parquet(parquet_file_path, index=False)
                    print(f"Colunas renomeadas e arquivo salvo: {parquet_file_path}")
                except Exception as e:
                    print(f"Erro ao processar o arquivo {parquet_file_path}: {e}")

def subir_arquivos_para_silver(parquet_dir, estado, ano, bucket_name='silver'):
    try:
        # Criação do cliente MinIO
        minio_client = boto3.client(
            's3',
            endpoint_url='http://10.100.100.61:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            region_name='us-east-1',
        )
        
        # Verifica se o diretório existe
        if not os.path.exists(parquet_dir):
            print(f"O diretório '{parquet_dir}' não existe.")
            return
        
        # Listar todos os arquivos no diretório especificado
        for root, _, files in os.walk(parquet_dir):
            for file in files:
                if file.endswith('.parquet'):
                    file_path = os.path.join(root, file)
                    s3_key = f'nordeste/DO{estado}{str(ano)}.parquet/{file}'  # Define o caminho no bucket
                    
                    print(f"Enviando o arquivo '{file_path}' para o bucket '{bucket_name}' com a chave '{s3_key}'.")
                    minio_client.upload_file(Filename=file_path, Bucket=bucket_name, Key=s3_key)
                    print(f"Arquivo '{file}' enviado com sucesso para o bucket '{bucket_name}'.")
    
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"Erro ao acessar o MinIO: {error_code} - {e}")
        raise





def criar_entidade_atlas(parquet_file_path, atlas_url, atlas_username, atlas_password):
    directory_name = os.path.basename(os.path.dirname(parquet_file_path))
  
    base_name = directory_name.split('.')[0]  

    qualified_name = f"{base_name}@sia_v1"  
    name = base_name  
    filename = directory_name 

    payload = {
        "entity": {
            "typeName": "file_metadata", 
            "attributes": {
                "qualifiedName": qualified_name, 
                "name": name,  
                "description": "Descrição da entidade de teste",  
                "owner": "Airflow", 
                "filename": filename  
            }
        }
    }

    response = requests.post(
        url=f"{atlas_url}/api/atlas/v2/entity",
        json=payload,
        auth=HTTPBasicAuth(atlas_username, atlas_password)
    )

    if response.status_code == 200:
        print(f"Entidade criada com sucesso: {response.json()}")
    else:
        print(f"Erro ao criar entidade: {response.status_code} - {response.text}")
        response.raise_for_status()

def criar_entidade_atlas_task(**kwargs):
    parquet_dir = kwargs['parquet_dir']
    atlas_url = kwargs['atlas_url']
    atlas_username = kwargs['atlas_username']
    atlas_password = kwargs['atlas_password']
    
    for root, _, files in os.walk(parquet_dir):
        for file in files:
            if file.endswith('.parquet'):
                parquet_file_path = os.path.join(root, file)
                criar_entidade_atlas(parquet_file_path, atlas_url, atlas_username, atlas_password)



# DAG
with DAG(
    dag_id='ETL_SIA_AN',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 8, 14),
        'email_on_failure': False,
        'email_on_retry': False,
    },
    description='DAG para baixar arquivos relacionados a ACF dos estados e anos específicos, aplicar transformações e sobrescrever arquivos Delta',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    for estado, ano in itertools.product(estados, anos):
        estado_ano_suffix = f'{estado}_{ano}'

        baixar_arquivos_task = PythonOperator(
            task_id=f'baixar_arquivos_{estado_ano_suffix}',
            python_callable=baixar_arquivos_e_registrar_atlas,
            provide_context=True
        )

        drop_columns_task = PythonOperator(
            task_id=f'drop_columns_{estado_ano_suffix}',
            python_callable=drop_columns_and_save,
            op_kwargs={
                'parquet_dir': f'/home/jamilsonfs/airflow/dags/SIA/tmp/DELTA/AN{estado}{str(ano)[-2:]}01.delta',
                'json_file': '/home/jamilsonfs/airflow/dags/SIA/files/column_names_to_drop.json'
            }
        )

        group_transform_task = PythonOperator(
            task_id=f'group_transform_{estado_ano_suffix}',
            python_callable=group_transform,
            op_kwargs={
                'parquet_dir': f'/home/jamilsonfs/airflow/dags/SIA/tmp/DELTA/AN{estado}{str(ano)[-2:]}01.delta',
                'json_file': '/home/jamilsonfs/airflow/dags/SIA/files/contextual_meanings.json'
            }
        )

        criar_entidade_atlas_operator = PythonOperator(
            task_id=f'criar_entidade_atlas_{estado_ano_suffix}',
            python_callable=criar_entidade_atlas_task,
            op_kwargs={
                'parquet_dir': f'/home/jamilsonfs/airflow/dags/SIA/tmp/DELTA/AN{estado}{str(ano)[-2:]}01.delta',
                'atlas_url': 'http://10.100.100.61:21000',
                'atlas_username': 'admin',
                'atlas_password': 'admin'
            }
        )

        baixar_arquivos_task  >> drop_columns_task >> group_transform_task >> criar_entidade_atlas_operator
