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

anos = [2020]
estados = ['PE', 'PI']


#==============================================================================
#================================ TRANSFORMAÇÕES ==============================
#==============================================================================

def baixar_arquivos_e_registrar_atlas(parquet_dir, atlas_url, atlas_username, atlas_password, **kwargs):
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

        for estado, ano in itertools.product(estados, anos):
            prefix = f'sim-parquet/DO{estado}{str(ano)}.parquet/'
            subdir_path = os.path.join(download_dir)
            
            # Verifica se o diretório do subdir já existe
            if not os.path.exists(subdir_path):
                os.makedirs(subdir_path)

            print(f"Listando objetos com o prefixo '{prefix}' no bucket '{bucket_name}'.")
            response = minio_client.list_objects_v2(Bucket=bucket_name, Prefix=prefix)
            
            if 'Contents' in response:
                for obj in response['Contents']:
                    key = obj['Key']
                    file_name = os.path.basename(key)
                    
                    # O caminho deve terminar no subdir_path
                    file_path = subdir_path

                    print(f"Baixando o objeto '{key}' para '{file_path}'.")
                    minio_client.download_file(Bucket=bucket_name, Key=key, Filename=os.path.join(file_path, file_name))
                    print(f"Objeto '{file_name}' baixado com sucesso.")
                    
                    # Chama a função de registrar no Atlas
                    # guid_entidade = criar_entidade_atlas(file_path, atlas_url, 'admin', 'admin')
            else:
                print(f"Nenhum objeto encontrado com o prefixo '{prefix}' no bucket '{bucket_name}'.")
        
    except botocore.exceptions.ClientError as e:
        error_code = e.response['Error']['Code']
        print(f"Erro ao acessar o MinIO: {error_code} - {e}")
        raise

def drop_columns_and_save(parquet_dir, json_file):
    with open(json_file, 'r') as f:
        # Carregar o conteúdo do JSON
        data = json.load(f)

    # Acessar a lista de colunas a serem removidas
    drop_columns = data[0]['to_drop']  # Acessando a primeira entrada e a chave 'to_drop'

    for root, _, files in os.walk(parquet_dir):
        for file in files:
            if file.endswith('.parquet'):
                parquet_file_path = os.path.join(root, file)
                try:
                    print(f"Removendo colunas de {parquet_file_path}")
                    df = pd.read_parquet(parquet_file_path)

                    # Remover as colunas especificadas
                    df = df.drop(columns=drop_columns, errors='ignore')

                    # Salvar o DataFrame atualizado de volta em formato Parquet
                    df.to_parquet(parquet_file_path, index=False)
                    print(f"Colunas removidas e dados salvos em Parquet: {parquet_file_path}")

                    # Chamar a função para criar entidade no Atlas (se necessário)
                    # criar_entidade_atlas_process(parquet_dir, 'http://10.100.100.61:21000', 'admin', 'admin', 'Drop de Colunas')
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

                    # Aplicando as transformações com base nas regras
                    for rule in transform_rules:  # Itera sobre a lista de regras
                        column_name = rule['column_name']
                        to_replace = rule['to_replace']
                        value = rule['value']
                        
                        if column_name in df.columns:
                            # Cria um mapeamento a partir das listas to_replace e value
                            mapping = dict(zip(to_replace, value))
                            df[column_name] = df[column_name].map(mapping).fillna(df[column_name])

                    df.to_parquet(parquet_file_path, index=False)
                    print(f"Dados transformados e salvos em Parquet: {parquet_file_path}")
                    # criar_entidade_atlas_process(parquet_file_path, 'http://10.100.100.61:21000', 'admin', 'admin', 'Group transform')
                except Exception as e:
                    print(f"Erro ao processar o arquivo {parquet_file_path}: {e}")

def traduzir_idade_e_transformar(parquet_dir):
    MINUTOS = 0
    HORAS = 1
    DIAS = 2
    MESES = 3
    ANOS = 4
    MAIS_DE_100_ANOS = 5


    def tradutor_idade(codigo):
        unidade = int(codigo[0])
        quantidade = int(codigo[1:])

        if unidade == MINUTOS and quantidade == 0:
            return "Idade não relatada"
        elif unidade == MINUTOS:
            return f"{quantidade} minutos"
        elif unidade == HORAS:
            return f"{quantidade} horas"
        elif unidade == DIAS:
            return f"{quantidade} dias"
        elif unidade == MESES:
            return f"{quantidade} meses"
        elif unidade == ANOS and quantidade == 0:
            return "Menor de 1 ano, mas não se sabe o número de horas, dias ou meses"
        elif unidade == ANOS:
            return f"{quantidade} anos"
        elif unidade == MAIS_DE_100_ANOS:
            return f"{quantidade + 100} anos"
        else:
            return "Código inválido"

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

def cid10_para_descricao(parquet_dir, json_file):
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




# Exemplo de uso
# subir_arquivos_para_silver('tmp/PARQUET')




#=========================================================================== 
#=========================== INTEGRAÇÃO COM O ATLAS ======================== 
#=========================================================================== 

# Criando entidade no Atlas 
def criar_entidade_atlas(parquet_file_path, atlas_url, atlas_username, atlas_password):
    directory_name = os.path.basename(os.path.dirname(parquet_file_path))
    base_name = directory_name.split('.')[0]  

    qualified_name = f"{base_name}@sia_v1"  
    name = f"{base_name}Teste"  
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
        guid = response.json().get("guid")  # Obter o GUID da entidade criada
        logging.info(f"Entidade criada com sucesso: {response.json()}")
        return guid  # Retornar o GUID da entidade
    else:
        logging.error(f"Erro ao criar entidade: {response.status_code} - {response.text}")
        response.raise_for_status()
# Entidade process
def criar_entidade_atlas_process(parquet_file_path, atlas_url, atlas_username, atlas_password, transformacao):
    directory_name = os.path.basename(os.path.dirname(parquet_file_path))
    base_name = directory_name.split('.')[0]

    qualified_name = f"process.{transformacao}.DataSUS@{base_name}"
    name = f'{transformacao} do arquivo {base_name}'
    description = f"Transformação de {transformacao}"

    payload = {
        "entity": {
            "typeName": "Process",
            "attributes": {
                "qualifiedName": qualified_name,
                "name": name,
                "owner": "Jamilson",
                "description": description,
                "added_columns": [],  # Adicione as colunas aqui
                "deleted_columns": []  # Adicione as colunas aqui
            },
            "relationshipAttributes": {
                "outputs": [],  # Defina as saídas se necessário
                "belongs_timeline": None,  # Defina a linha do tempo, se necessário
                "inputs": [],  # Defina as entradas se necessário
                "meanings": []  # Defina os significados, se necessário
            }
        }
    }

    response = requests.post(
        url=f"{atlas_url}/api/atlas/v2/entity",
        json=payload,
        auth=HTTPBasicAuth(atlas_username, atlas_password)
    )

    if response.status_code == 200:
        guid = response.json().get("guid")  # Obter o GUID da entidade criada
        logging.info(f"Entidade de processo criada com sucesso: {response.json()}")
        return guid  # Retornar o GUID da entidade
    else:
        logging.error(f"Erro ao criar entidade de processo: {response.status_code} - {response.text}")
        response.raise_for_status()
# Criando relacionamento
def criar_relacionamento_lineage(atlas_url, atlas_username, atlas_password, guid_origem, guid_destino, tipo_relacionamento):
    url = f"{atlas_url}/api/atlas/v2/relationship"

    payload = {
        "relationship": {
            "typeName": tipo_relacionamento,
            "end1": {
                "guid": guid_origem
            },
            "end2": {
                "guid": guid_destino
            }
        }
    }

    response = requests.post(url, json=payload, auth=HTTPBasicAuth(atlas_username, atlas_password))

    if response.status_code == 200:
        logging.info(f"Relacionamento criado com sucesso: {response.json()}")
    else:
        logging.error(f"Erro ao criar relacionamento: {response.status_code} - {response.text}")

atlas_url = "http://10.100.100.61:21000/"
atlas_username = "admin"
atlas_password = "admin"

transformacao = "exemplo_transformacao"  


#=========================================================================== 
#================================= DAG ===================================== 
#=========================================================================== 


with DAG(
    dag_id='ETL_SIM',
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

    for estado, ano in itertools.product(estados, anos):
        estado_ano_suffix = f'{estado}_{ano}'

        baixar_arquivos_task = PythonOperator(
            task_id=f'baixar_arquivos_{estado_ano_suffix}',
            python_callable=baixar_arquivos_e_registrar_atlas,
            op_kwargs={
                'parquet_dir': f'/home/jamilsonfs/airflow/dags/SIM/tmp/PARQUET/DO{estado}{str(ano)}.parquet',
                'atlas_url': 'http://10.100.100.61:21000',
                'atlas_username': 'admin',
                'atlas_password': 'admin'
            }
        )

        drop_columns_task = PythonOperator(
            task_id=f'drop_columns_{estado_ano_suffix}',
            python_callable=drop_columns_and_save,
            op_kwargs={
                'parquet_dir': f'/home/jamilsonfs/airflow/dags/SIM/tmp/PARQUET/DO{estado}{str(ano)}.parquet',
                'json_file': '/home/jamilsonfs/airflow/dags/SIM/files/drop_columns.json'
            }
        )

        group_transform_task = PythonOperator(
            task_id=f'group_transform_{estado_ano_suffix}',
            python_callable=group_transform,
            op_kwargs={
                'parquet_dir': f'/home/jamilsonfs/airflow/dags/SIM/tmp/PARQUET/DO{estado}{str(ano)}.parquet',
                'json_file': '/home/jamilsonfs/airflow/dags/SIM/files/grupo_tranform.json'
            }
        )

        idade_transform = PythonOperator(
            task_id=f't_idade_{estado_ano_suffix}',
            python_callable= traduzir_idade_e_transformar,
            op_kwargs={'parquet_dir': f'/home/jamilsonfs/airflow/dags/SIM/tmp/PARQUET/DO{estado}{str(ano)}.parquet'},
        )

        cid_transform = PythonOperator(
            task_id=f't_cid10__{estado_ano_suffix}',
            python_callable=cid10_para_descricao,
            op_kwargs={'parquet_dir': f'/home/jamilsonfs/airflow/dags/SIM/tmp/PARQUET/DO{estado}{str(ano)}.parquet','json_file':'/home/jamilsonfs/airflow/dags/SIM/files/cid10_data.json' },
        ) 
        
        renomear_columns = PythonOperator(
            task_id=f'renomear_colunas__{estado_ano_suffix}',
            python_callable=names_column,
            op_kwargs={'parquet_dir': f'/home/jamilsonfs/airflow/dags/SIM/tmp/PARQUET/DO{estado}{str(ano)}.parquet','json_file_path':'/home/jamilsonfs/airflow/dags/SIM/files/name_columns.json' },
        ) 


        subir_os_arquivos = PythonOperator(
            task_id=f'camada_prata__{estado_ano_suffix}',
            python_callable=subir_arquivos_para_silver,
            op_kwargs={'parquet_dir': f'/home/jamilsonfs/airflow/dags/SIM/tmp/PARQUET/DO{estado}{str(ano)}.parquet', 'estado': estado,'ano': ano},
        )

        # Definir fluxo das tasks
        baixar_arquivos_task >> drop_columns_task >> group_transform_task >> idade_transform >> cid_transform >> renomear_columns >> subir_os_arquivos

        #Extract
        baixar_arquivos_task.ui_color = '#3366ff'

        #Transform
        drop_columns_task.ui_color = '#ffd966'
        group_transform_task.ui_color = '#ffd966'
        idade_transform.ui_color = '#ffd966'
        cid_transform.ui_color = '#ffd966'
        renomear_columns.ui_color = '#ffd966'

        #Load
        subir_os_arquivos.ui_color = '#ff8c00'

