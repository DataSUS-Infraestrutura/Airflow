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

anos = [2022]
estados = ['SP']
meses = [f"{i:02}" for i in range(1, 13)]

def baixar_arquivos_e_registrar_atlas(parquet_dir, estado, ano, mes, **kwargs):
    try:
        minio_client = boto3.client(
            's3',
            endpoint_url='http://10.100.100.61:9000',
            aws_access_key_id='minioadmin',
            aws_secret_access_key='minioadmin',
            region_name='us-east-1',
        )

        bucket_name = 'bronze'
        download_dir = parquet_dir

        if not os.path.exists(download_dir):
            os.makedirs(download_dir)

        prefix = f'sia-parquet/AR/AR{estado}{str(ano)[-2:]}{mes}.parquet/'
        # Usa o penúltimo diretório no prefixo para evitar duplicação
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

def subir_arquivos_para_silver(parquet_dir, estado, ano, mes, bucket_name='silver'):
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
                    s3_key = f'SIA/AR/AR{estado}{str(ano)[-2:]}{mes}.parquet/{file}'
                    print(f"Enviando o arquivo '{file_path}' para o bucket '{bucket_name}' com a chave '{s3_key}'.")
                    minio_client.upload_file(Filename=file_path, Bucket=bucket_name, Key=s3_key)
                    print(f"Arquivo '{file}' enviado com sucesso para o bucket '{bucket_name}'.")

    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"Erro ao acessar o MinIO: {error_code} - {e}")
        raise


# Esta seção do código lida com a definição da DAG
with DAG(
    dag_id='ETL_SIA_AR',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 8, 14),
        'email_on_failure': False,
        'email_on_retry': False,
    },
    description='DAG para baixar arquivos, aplicar transformações e registrar no Atlas',
    schedule_interval='@daily',
    catchup=False,
) as dag:

    for estado, ano, mes in itertools.product(estados, anos, meses):
        estado_ano_mes_suffix = f'{estado}_{ano}_{mes}'

        baixar_arquivos_task = PythonOperator(
            task_id=f'baixar_arquivos{estado_ano_mes_suffix}',
            python_callable=baixar_arquivos_e_registrar_atlas,
            op_kwargs={
                'parquet_dir': f'/home/saude/airflow/dags/SIA/tmp/PARQUET/AM{estado}{str(ano)[-2:]}{mes}.parquet',
                'estado': estado,
                'ano': ano,
                'mes': mes
            }
        )

        drop_columns_task = PythonOperator(
            task_id=f'drop_columns_{estado_ano_mes_suffix}',
            python_callable=drop_columns_and_save,
            op_kwargs={
                 'parquet_dir': f'/home/saude/airflow/dags/SIA/tmp/PARQUET/AM{estado}{str(ano)[-2:]}{mes}.parquet',
                'json_file': '/home/saude/airflow/dags/SIA/files/column_names_to_drop.json'
            }
        )

        group_transform_task = PythonOperator(
            task_id=f'group_transform_{estado_ano_mes_suffix}',
            python_callable=group_transform,
            op_kwargs={
                 'parquet_dir': f'/home/saude/airflow/dags/SIA/tmp/PARQUET/AM{estado}{str(ano)[-2:]}{mes}.parquet',
                'json_file': '/home/saude/airflow/dags/SIA/files/contextual_meanings.json'
            }
        )

        renomear_columns = PythonOperator(
            task_id=f'renomear_colunas__{estado_ano_mes_suffix}',
            python_callable=names_column,
            op_kwargs={'parquet_dir': f'/home/saude/airflow/dags/SIA/tmp/PARQUET/AM{estado}{str(ano)[-2:]}{mes}.parquet','json_file_path':'/home/saude/airflow/dags/SIA/files/column_names_to_remap.json' },
        )


        upload_to_silver_task = PythonOperator(
            task_id=f'subir_arquivos_para_silver_{estado_ano_mes_suffix}',
            python_callable=subir_arquivos_para_silver,
            op_kwargs={
                'parquet_dir': f'/home/saude/airflow/dags/SIA/tmp/PARQUET/AM{estado}{str(ano)[-2:]}{mes}.parquet',
                'estado': estado,
                'ano': ano,
                'mes': mes
            }
        )

        baixar_arquivos_task >> drop_columns_task >> group_transform_task >> renomear_columns >> upload_to_silver_task


baixar_arquivos_task.ui_color = '#3366ff'

drop_columns_task.ui_color = '#ffd966'
group_transform_task.ui_color = '#ffd966'
renomear_columns.ui_color = '#ffd966'

upload_to_silver_task.ui_color = '#3366ff'