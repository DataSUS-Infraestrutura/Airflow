import os
from pyspark.sql import SparkSession
import pandas as pd
from sim_utils import tradutor_idade

def transforme1(parquet_dir):
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

def convert_parquet_to_delta(spark, parquet_dir, delta_dir):
    def convert_spark_to_delta(spark_df, delta_file_path):
        spark_df.write.format("delta").mode("overwrite").save(delta_file_path)

    if not os.path.exists(delta_dir):
        os.makedirs(delta_dir)

    delta_files = []
    for root, _, files in os.walk(parquet_dir):
        for file in files:
            if file.endswith('.parquet'):
                parquet_file_path = os.path.join(root, file)
                try:
                    print(f"Convertendo {parquet_file_path} para Delta")
                    spark_df = spark.read.parquet(parquet_file_path)
                    delta_file_path = os.path.join(delta_dir, os.path.basename(file).replace('.parquet', '.delta'))
                    convert_spark_to_delta(spark_df, delta_file_path)
                    print(f"Arquivo Parquet {parquet_file_path} convertido com sucesso para {delta_file_path}")
                    delta_files.append(delta_file_path)
                except Exception as e:
                    print(f"Erro ao processar arquivo {parquet_file_path}: {e}")

    return delta_files
