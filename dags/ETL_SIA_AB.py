from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import pyspark
from delta import *
import os
import sys
from pysus.ftp.databases.sim import SIM
from enum import Enum
import pandas as pd
import json


regions = {
    #'SUL': ['PR', 'SC', 'RS'],  # Região Sul
    'SUDESTE': ['SP', 'RJ', 'MG', 'ES'],  # Região Sudeste
    #'CENTRO_OESTE': ['DF', 'GO', 'MT', 'MS'],  # Região Centro-Oeste
    #'NORDESTE': ['MA', 'PI', 'CE', 'RN', 'PB', 'PE', 'AL', 'SE', 'BA'],  # Região Nordeste
    #'NORTE': ['AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO']  # Região Norte
}
ano = [2023]

def create_spark_session():
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

def get_dataframe_for_uf(region, uf, year, **kwargs):
    try:
        sia = SIA().load()
        files = sia.get_files("AB", uf, year)
        dfs = sia.download(files, local_dir='/home/jamilsonfs/airflow/dados/dados_parquet')
        result = {uf: [df.to_json() for df in dfs]}
        print(f"Dados para a UF {uf} e ano {year}: {result}")
        return result
    except Exception as e:
        print(f"Erro ao processar UF {uf} e ano {year}: {e}")
        return {uf: None}

def extract_and_format_filename(parquet_file_path):
    filename = os.path.basename(parquet_file_path)
    formatted_name = filename.replace(".parquet", "")
    return formatted_name

def convert_parquet_to_delta(directory_path, delta_path, **kwargs):
    spark = create_spark_session()

    def convert_spark_to_delta(spark_df, delta_file_path):
        spark_df.write.format("delta").mode("overwrite").save(delta_file_path)

    delta_files = []
    for root, _, files in os.walk(directory_path):
        for file in files:
            if file.endswith('.parquet'):
                parquet_file_path = os.path.join(root, file)
                try:
                    print(f"Convertendo {parquet_file_path} para Delta")
                    spark_df = spark.read.parquet(parquet_file_path)
                    formatted_name = extract_and_format_filename(parquet_file_path)
                    delta_file_path = os.path.join(delta_path, f'{formatted_name}.delta')
                    convert_spark_to_delta(spark_df, delta_file_path)
                    print(f"Arquivo Parquet {parquet_file_path} convertido com sucesso para {delta_file_path}")
                    delta_files.append(delta_file_path)
                except Exception as e:
                    print(f"Erro ao processar arquivo {parquet_file_path}: {e}")

    spark.stop()
    return delta_files

def inserir_minio():
    return

with DAG(
    dag_id='ETL_SIA_AB',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 6, 13),
        'email_on_failure': False,
        'email_on_retry': False,
    },
    description='DAG com múltiplas tarefas e uma ramificação',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    start = DummyOperator(
        task_id='start',
    )
    end = DummyOperator(
        task_id='end',
    )

    gerar_delta = PythonOperator(
       task_id='gerar_delta',
       python_callable=convert_parquet_to_delta,
       op_kwargs={'directory_path': '/home/jamilsonfs/airflow/dados/dados_parquet', 'delta_path':'/home/jamilsonfs/airflow/dados/dados_delta'},
       provide_context=True,
    )

    for region, ufs in regions.items():
        for year in ano:
            baixar_region_task = PythonOperator(
                task_id=f'baixar_{region}_{year}',
                python_callable=lambda: None,  # Implement the logic for region download if needed
                op_kwargs={'region': region},
                provide_context=True,
            )
            baixar_region_task.ui_color = '#3366ff'
            start >> baixar_region_task

            baixar_uf_tasks = []

            for uf in ufs:
                baixar_uf_task = PythonOperator(
                    task_id=f'baixar_SIA_AB_{uf}_{year}',
                    python_callable=get_dataframe_for_uf,
                    op_kwargs={'region': region, 'uf': uf, 'year': year},
                    provide_context=True,
                )
                baixar_uf_task.ui_color = '#3366ff'
                baixar_region_task >> baixar_uf_task
                baixar_uf_tasks.append(baixar_uf_task)

            baixar_uf_tasks >> gerar_delta >> end

start.ui_color = '#3366ff'

gerar_delta.ui_color = '#93c47d'
end.ui_color = '#77aaff'
