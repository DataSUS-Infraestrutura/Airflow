import requests
import pandas as pd
import os
from pysus.utilities.readdbc import read_dbc
from delta import configure_spark_with_delta_pip
from pyspark.sql import SparkSession

def check_atlas_connection():
    url = "http://localhost:21000/api/atlas/v2/entity/bulk"
    response = requests.get(url)
    if response.status_code == 200:
        print("Conexão com Apache Atlas bem-sucedida.")
    else:
        raise Exception("Falha na conexão com o Apache Atlas.")

def load_pysus_databases():
    try:
        # Carregar dados do SIM para a UF especificada (exemplo: São Paulo e ano 2022)
        df = read_dbc('path_to_file.dbc', encoding='utf-8')
        df.to_parquet('output_path.parquet')
    except Exception as e:
        raise Exception(f"Erro ao carregar bases do Pysus: {e}")

def transform_age_data():
    # Função para aplicar a transformação de idade com base nos códigos
    parquet_path = 'output_path.parquet'
    df = pd.read_parquet(parquet_path)

    def tradutor_idade(idade):
        # Lógica para traduzir a idade em uma unidade de tempo
        if idade < 2000:
            return f'{idade} anos'
        return 'Idade desconhecida'

    df['idade_traduzida'] = df['IDADE'].apply(tradutor_idade)
    df.to_parquet(parquet_path)

def apply_group_transform():
    # Exemplo de transformação de grupo usando um arquivo JSON
    parquet_path = 'output_path.parquet'
    df = pd.read_parquet(parquet_path)

    # Exemplo de um arquivo JSON de transformação
    transform_rules = {
        "CAUSABAS": {"A00": "Cólera", "B00": "Varíola"},
        "IDADE": {"000": "Recém-nascido", "100": "1 ano"}
    }

    for col, rules in transform_rules.items():
        df[col] = df[col].replace(rules)
    
    df.to_parquet(parquet_path)

def convert_parquet_to_delta():
    # Configurando o Spark com Delta Lake
    builder = SparkSession.builder.appName("Convert Parquet to Delta") \
        .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
        .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog")

    spark = configure_spark_with_delta_pip(builder).getOrCreate()

    parquet_path = 'output_path.parquet'
    delta_path = 'delta_table_path'

    df = spark.read.parquet(parquet_path)
    df.write.format('delta').save(delta_path)

def initial_verification():
    # Verificação inicial de arquivos Parquet
    parquet_path = 'output_path.parquet'
    df = pd.read_parquet(parquet_path)
    print(f"Número de registros iniciais: {len(df)}")

def final_verification():
    # Verificação final após as transformações
    parquet_path = 'output_path.parquet'
    df = pd.read_parquet(parquet_path)
    print(f"Número de registros finais: {len(df)}")
