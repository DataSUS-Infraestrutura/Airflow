from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import boto3
import botocore
import os
from pyspark.sql import SparkSession
import pyarrow.parquet as pq
import pandas as pd
import json

# INICIANDO A SESSÃO SPARK
spark = SparkSession.builder \
    .appName("LerDeltaFile") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0") \
    .config("spark.driver.memory", "2g") \
    .config("spark.executor.memory", "4g") \
    .getOrCreate()


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
        raise

def baixar_arquivos(**kwargs):
    try:
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
        
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)

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

def converter_delta_df(parquet_dir):
    # Diretório que contém os arquivos Delta
    parquet_files = [os.path.join(parquet_dir, f) for f in os.listdir(parquet_dir) if f.endswith('.parquet')]

    dataset = pq.ParquetDataset(parquet_files)
    table = dataset.read()
    
    df = table.to_pandas()
    print(df.head())  # Exibe as primeiras linhas do dataframe para verificação
    return df

def drop_columns_and_save(parquet_dir, json_file):
    # Carrega o JSON com as colunas para remover
    with open(json_file, 'r') as f:
        drop_columns = json.load(f)  # A lista de colunas a serem removidas

    # Percorre todos os arquivos no diretório fornecido
    for root, _, files in os.walk(parquet_dir):
        for file in files:
            if file.endswith('.parquet'):
                parquet_file_path = os.path.join(root, file)
                try:
                    print(f"Removendo colunas de {parquet_file_path}")
                    df = pd.read_parquet(parquet_file_path)

                    # Remove as colunas do DataFrame
                    df = df.drop(columns=drop_columns, errors='ignore')

                    # Sobrescreve o arquivo parquet com o dataframe transformado
                    df.to_parquet(parquet_file_path, index=False)
                    print(f"Colunas removidas e dados salvos em Parquet: {parquet_file_path}")
                except Exception as e:
                    print(f"Erro ao processar o arquivo {parquet_file_path}: {e}")

def group_transform(parquet_dir, json_file):
    # Carrega o JSON com as regras de transformação
    with open(json_file, 'r') as f:
        transform_rules = json.load(f)

    # Percorre todos os arquivos no diretório fornecido
    for root, _, files in os.walk(parquet_dir):
        for file in files:
            if file.endswith('.parquet'):
                parquet_file_path = os.path.join(root, file)
                try:
                    print(f"Transformando {parquet_file_path}")
                    df = pd.read_parquet(parquet_file_path)

                    # Aplica as transformações de acordo com o JSON
                    for column_name, mappings in transform_rules.items():
                        if column_name in df.columns:
                            # Substitui os valores da coluna com base no mapeamento
                            df[column_name] = df[column_name].map(mappings).fillna(df[column_name])

                    # Sobrescreve o arquivo parquet com o dataframe transformado
                    df.to_parquet(parquet_file_path, index=False)
                    print(f"Dados transformados e salvos em Parquet: {parquet_file_path}")
                except Exception as e:
                    print(f"Erro ao processar o arquivo {parquet_file_path}: {e}")


# DAG
with DAG(
    dag_id='ETL_SIA_ACF',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 8, 14),
        'email_on_failure': False,
        'email_on_retry': False,
    },
    description='DAG para baixar arquivos relacionados a ACFMG2101 do bucket do MinIO, aplicar transformações e sobrescrever arquivos Delta',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    
    verificar_conexao_task = PythonOperator(
        task_id='verificar_conexao',
        python_callable=verificar_conexao_minio,
    )
    
    baixar_arquivos_task = PythonOperator(
        task_id='baixar_arquivos',
        python_callable=baixar_arquivos,
    )

    converter_delta_task = PythonOperator(
        task_id='converter_delta',
        python_callable=converter_delta_df,
        op_kwargs={'parquet_dir': '/home/jamilsonfs/airflow/dags/SIA/tmp/DELTA/ACFMG2001.delta'},
    )

    drop_columns_task = PythonOperator(
        task_id='drop_columns',
        python_callable=drop_columns_and_save,
        op_kwargs={
            'parquet_dir': '/home/jamilsonfs/airflow/dags/SIA/tmp/DELTA/ACFMG2001.delta',
            'json_file': '/home/jamilsonfs/airflow/dags/SIA/files/column_names_to_drop.json'
        }
    )

    group_transform = PythonOperator(
        task_id='group_transform',
        python_callable=group_transform,
        op_kwargs={
            'parquet_dir': '/home/jamilsonfs/airflow/dags/SIA/tmp/DELTA/ACFMG2001.delta',
            'json_file': '/home/jamilsonfs/airflow/dags/SIA/files/contextual_meanings.json'
        }
    )

    verificar_conexao_task >> baixar_arquivos_task >> converter_delta_task >> drop_columns_task >> group_transform
