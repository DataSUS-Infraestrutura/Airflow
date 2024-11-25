import boto3
import botocore
import os
import itertools

estados = ['SP']
anos = [2021]
meses = [f"{i:02}" for i in range(1, 13)] 


def baixar_arquivos_minio(parquet_dir, estados, anos, meses):
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

        # Cria o diretório de download, caso não exista
        if not os.path.exists(download_dir):
            os.makedirs(download_dir)

        # Itera sobre combinações de estados, anos e meses
        for estado, ano, mes in itertools.product(estados, anos, meses):
            prefix = f'sih-parquet/RD/RD{estado}{str(ano)[-2:]}{mes}.parquet/'
            subdir_path = os.path.join(download_dir, f'{estado}_{ano}_{mes}')
            
            # Verifica se o subdiretório já existe e cria se necessário
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

# Chama a função com os parâmetros corrigidos
baixar_arquivos_minio('/home/jamilsonfs/airflow/dags/SIH/tmp', estados, anos, meses)
