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

from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults
from atlasclient.client import Atlas
import requests


class AtlasConnectionCheckOperator(BaseOperator):
    @apply_defaults
    def __init__(self, atlas_url, atlas_user, atlas_password, *args, **kwargs):
        super(AtlasConnectionCheckOperator, self).__init__(*args, **kwargs)
        self.atlas_url = atlas_url
        self.atlas_user = atlas_user
        self.atlas_password = atlas_password

    def check_connection(self):
        try:
            response = requests.get(self.atlas_url, auth=(self.atlas_user, self.atlas_password))
            if response.status_code == 200:
                self.log.info("Conexão bem-sucedida com o Apache Atlas.")
                return True
            else:
                self.log.error(f"Falha na conexão com o Apache Atlas. Status Code: {response.status_code}")
                return False
        except Exception as e:
            self.log.error(f"Erro ao conectar-se ao Apache Atlas: {e}")
            return False

    def execute(self, context):
        if not self.check_connection():
            raise Exception("Falha na conexão com o Apache Atlas.")

class UnidadeTempo(Enum):
    MINUTOS = 0
    HORAS = 1
    DIAS = 2
    MESES = 3
    ANOS = 4
    MAIS_DE_100_ANOS = 5

def tradutor_idade(codigo):
    unidade = int(codigo[0])
    quantidade = int(codigo[1:])

    if unidade == UnidadeTempo.MINUTOS.value and quantidade == 00:
        return "Idade não relatada"
    elif unidade == UnidadeTempo.MINUTOS.value:
        return f"{quantidade} minutos"
    elif unidade == UnidadeTempo.HORAS.value:
        return f"{quantidade} horas"
    elif unidade == UnidadeTempo.DIAS.value:
        return f"{quantidade} dias"
    elif unidade == UnidadeTempo.MESES.value:
        return f"{quantidade} meses"
    elif unidade == UnidadeTempo.ANOS.value and quantidade == 00:
        return "Menor de 1 ano, mas não se sabe o número de horas, dias ou meses"
    elif unidade == UnidadeTempo.ANOS.value:
        return f"{quantidade} anos"
    elif unidade == UnidadeTempo.MAIS_DE_100_ANOS.value:
        return f"{quantidade + 100} anos"
    else:
        return "Código inválido"

def load_pysus_databases():
    try:
        sim = SIM().load()
        print("SIM e databases carregados com sucesso.")
        return {
            'sim': 'SIM - Sistema de Informação sobre Mortalidade'
        }
    except Exception as e:
        print(f"Ocorreu um erro ao carregar os bancos de dados: {e}")
        sys.exit(1)

regions = {
    #'SUL': ['PR', 'SC', 'RS'],  # Região Sul
    'SUDESTE': ['SP', 'RJ', 'MG', 'ES'],  # Região Sudeste
    #'CENTRO_OESTE': ['DF', 'GO', 'MT', 'MS'],  # Região Centro-Oeste
    #'NORDESTE': ['MA', 'PI', 'CE', 'RN', 'PB', 'PE', 'AL', 'SE', 'BA'],  # Região Nordeste
    #'NORTE': ['AC', 'AP', 'AM', 'PA', 'RO', 'RR', 'TO']  # Região Norte
}
ano = [2020]

def create_spark_session():
    builder = pyspark.sql.SparkSession.builder.appName("MyApp") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
        .config("spark.jars.packages", "io.delta:delta-core_2.12:2.1.0")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()
    return spark

def get_dataframe_for_uf(region, uf, year, **kwargs):
    try:
        sim = SIM().load()
        files = sim.get_files("CID10", uf, year)
        dfs = sim.download(files, local_dir='/home/jamilsonfs/airflow/dados/dados_parquet')
        result = {uf: [df.to_json() for df in dfs]}
        print(f"Dados para a UF {uf} e ano {year}: {result}")
        return result
    except Exception as e:
        print(f"Erro ao processar UF {uf} e ano {year}: {e}")
        return {uf: None}

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

                    for rule in transform_rules:
                        column_name = rule['column_name']
                        to_replace = rule['to_replace']
                        value = rule['value']

                        if column_name in df.columns:
                            df[column_name] = df[column_name].replace(to_replace, value)

                    df.to_parquet(parquet_file_path, index=False)
                    print(f"Dados transformados e salvos em Parquet: {parquet_file_path}")
                except Exception as e:
                    print(f"Erro ao processar o arquivo {parquet_file_path}: {e}")

def transforme2(parquet_dir):
    for root, _, files in os.walk(parquet_dir):
        for file in files:
            if file.endswith('.parquet'):
                parquet_file_path = os.path.join(root, file)
                try:
                    print(f"Transformando {parquet_file_path}")
                    df = pd.read_parquet(parquet_file_path)
                    if 'IDADE' in df.columns:
                        df['IDADE'] = df['IDADE'].apply(tradutor_idade)
                        df.to_parquet(parquet_file_path, index=False)
                        print(f"Dados transformados e salvos em Parquet: {parquet_file_path}")
                    else:
                        print(f"Coluna 'IDADE' não encontrada nos dados: {parquet_file_path}")
                except Exception as e:
                    print(f"Erro ao processar o arquivo {parquet_file_path}: {e}")

def transforme3(parquet_dir, json_file):
    with open(json_file, 'r') as f:
        codigos = json.load(f)
    
    codigo_dict = {}
    for item in codigos:
        codigo_dict[item['code']] = item['description']

    def substituir_codigo_por_descricao(codigo):
        descricao = codigo_dict.get(codigo, codigo)  
        if descricao == codigo:  
            for key in codigo_dict:
                if codigo.startswith(key):
                    return codigo_dict[key]
            return codigo
        return descricao

    for root, _, files in os.walk(parquet_dir):
        for file in files:
            if file.endswith('.parquet'):
                parquet_file_path = os.path.join(root, file)
                try:
                    print(f"Transformando {parquet_file_path}")
                    df = pd.read_parquet(parquet_file_path)
                    if 'CAUSABAS' in df.columns:
                        df['CAUSABAS'] = df['CAUSABAS'].astype(str).apply(substituir_codigo_por_descricao)
                    if 'CAUSABAS_O' in df.columns:
                        df['CAUSABAS_O'] = df['CAUSABAS_O'].astype(str).apply(substituir_codigo_por_descricao)
                    df.to_parquet(parquet_file_path, index=False)
                    print(f"Dados transformados e salvos em Parquet: {parquet_file_path}")
                except Exception as e:
                    print(f"Erro ao processar o arquivo {parquet_file_path}: {e}")

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

def drop_columns(parquet_dir, json_file):
    with open(json_file, 'r') as f:
        drop_rules = json.load(f)

    to_drop_columns = {rule['id']: rule['to_drop'] for rule in drop_rules}

    for root, _, files in os.walk(parquet_dir):
        for file in files:
            if file.endswith('.parquet'):
                parquet_file_path = os.path.join(root, file)
                try:
                    print(f"Removendo colunas de {parquet_file_path}")
                    df = pd.read_parquet(parquet_file_path)

                    for drop_id, columns in to_drop_columns.items():
                        df = df.drop(columns=columns, errors='ignore')

                    df.to_parquet(parquet_file_path, index=False)
                    print(f"Colunas removidas e dados salvos em Parquet: {parquet_file_path}")
                except Exception as e:
                    print(f"Erro ao processar o arquivo {parquet_file_path}: {e}")


def verificacao_inicial(parquet_dir):
    resultados_iniciais = []

    for arquivo in os.listdir(parquet_dir):
        if arquivo.endswith(".parquet"):
            caminho_arquivo = os.path.join(parquet_dir, arquivo)
            df = pd.read_parquet(caminho_arquivo)
            
            #quais metadados sao importantes?
            resultados_iniciais.append({
                'Arquivo': arquivo,
                'Número de Registros': len(df),
                'Colunas': ', '.join(df.columns)
            })
    
    df_resultados_iniciais = pd.DataFrame(resultados_iniciais)
    df_resultados_iniciais.to_csv(os.path.join(parquet_dir, 'verificacao_inicial.csv'), index=False)

def verificacao_final(parquet_dir):
    resultados_finais = []

    for arquivo in os.listdir(parquet_dir):
        if arquivo.endswith(".parquet"):
            caminho_arquivo = os.path.join(parquet_dir, arquivo)
            df = pd.read_parquet(caminho_arquivo)
            
            resultados_finais.append({
                'Arquivo': arquivo,
                'Número de Registros': len(df),
                'Colunas': ', '.join(df.columns)
            })
    
    df_resultados_finais = pd.DataFrame(resultados_finais)
    df_resultados_finais.to_csv(os.path.join(parquet_dir, 'verificacao_final.csv'), index=False)


def inserir_minio():
    return

with DAG(
    dag_id='ETL_SIM',
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
    configuracoes = PythonOperator(
        task_id='configuracoes',
        python_callable=load_pysus_databases,
        provide_context=True,
    )
    start >> configuracoes

    gerar_delta = PythonOperator(
       task_id='gerar_delta',
       python_callable=convert_parquet_to_delta,
       op_kwargs={'directory_path': '/home/jamilsonfs/airflow/dados/dados_parquet', 'delta_path':'/home/jamilsonfs/airflow/dados/dados_delta'},
       provide_context=True,
    )

    drop_columns_task = PythonOperator(
        task_id='drop_columns',
        python_callable=drop_columns,
        op_kwargs={'parquet_dir': '/home/jamilsonfs/airflow/dados/dados_parquet', 'json_file':'/home/jamilsonfs/airflow/dados/json_files/drop_columns.json'},
    )

    group_transform_task = PythonOperator(
        task_id='group_transform',
        python_callable=group_transform,
        op_kwargs={'parquet_dir': '/home/jamilsonfs/airflow/dados/dados_parquet', 'json_file':'/home/jamilsonfs/airflow/dados/json_files/grupo_tranform.json'},
    )

    transforme2_task = PythonOperator(
        task_id='t_idade',
        python_callable=transforme2,
        op_kwargs={'parquet_dir': '/home/jamilsonfs/airflow/dados/dados_parquet'},
    )

    transforme3_task = PythonOperator(
        task_id='t_cid10',
        python_callable=transforme3,
        op_kwargs={'parquet_dir': '/home/jamilsonfs/airflow/dados/dados_parquet', 'json_file':'/home/jamilsonfs/airflow/dados/json_files/cid10_data.json'},
    ) 


    atlas_task = AtlasConnectionCheckOperator(
        task_id='atlas_connection',
        atlas_url='http://10.100.100.61:21000/',
        atlas_user='admin',
        atlas_password='admin',
        
        dag=dag
    )

    verificacao_inicial = PythonOperator(
        task_id='v_inicial',
        python_callable=verificacao_inicial,
        op_kwargs={'parquet_dir': '/home/jamilsonfs/airflow/dados/dados_parquet'},
    ) 

    verificacao_final = PythonOperator(
        task_id='v_final',
        python_callable=verificacao_final,
        op_kwargs={'parquet_dir': '/home/jamilsonfs/airflow/dados/dados_parquet'},
    ) 


    for region, ufs in regions.items():
        for year in ano:
            baixar_region_task = PythonOperator(
                task_id=f'baixar_{region}_{year}',
                python_callable=lambda: None,  
                op_kwargs={'region': region},
                provide_context=True,
            )
            baixar_region_task.ui_color = '#3366ff'
            configuracoes >> baixar_region_task

            baixar_uf_tasks = []

            for uf in ufs:
                baixar_uf_task = PythonOperator(
                    task_id=f'baixar_SIM_{uf}_{year}',
                    python_callable=get_dataframe_for_uf,
                    op_kwargs={'region': region, 'uf': uf, 'year': year},
                    provide_context=True,
                )
                baixar_uf_task.ui_color = '#3366ff'
                baixar_region_task >> baixar_uf_task
                baixar_uf_tasks.append(baixar_uf_task)

            baixar_uf_tasks >>  atlas_task >> verificacao_inicial >> drop_columns_task >> group_transform_task

    group_transform_task >> transforme2_task
    transforme2_task >> transforme3_task
    transforme3_task >> verificacao_final >> gerar_delta>> end

start.ui_color = '#3366ff'
configuracoes.ui_color = '#3366ff'



verificacao_final.ui_color = '#808080'
verificacao_inicial.ui_color = '#808080'
drop_columns_task.ui_color = '#ffd966'
group_transform_task.ui_color = '#ffd966'
transforme2_task.ui_color = '#ffd966'
transforme3_task.ui_color = '#ffd966'

gerar_delta.ui_color = '#93c47d'
end.ui_color = '#77aaff'
