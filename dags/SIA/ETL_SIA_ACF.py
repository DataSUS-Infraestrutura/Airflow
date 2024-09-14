from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
import boto3
import os

def baixar_dados(region, uf, year, **kwargs):
    local_dir = '/tmp/dados_parquet'  # Temporary directory to store files
    os.makedirs(local_dir, exist_ok=True)
    try:
        print(f"Baixando dados do MinIO para UF {uf} e ano {year} na região {region}")
        
        # Download files from MinIO
        download_from_minio(local_dir, uf, year)

    except Exception as e:
        print(f"Erro ao baixar dados do MinIO para UF {uf} e ano {year} na região {region}: {e}")

def download_from_minio(local_dir, uf, year):
    # MinIO client configuration
    minio_client = boto3.client(
        's3',
        endpoint_url='http://10.100.100.61:9001',
        aws_access_key_id='minioadmin',
        aws_secret_access_key='minioadmin',
        region_name='us-east-1',
    )

    bucket_name = 'bronze-delta'
    base_path = 'datasus_sia_delta'

    s3_file_path = f'{base_path}/ACF{uf}{str(year)[2:]}01.delta'  # Assuming the file naming pattern as you mentioned
    
    try:
        local_file_path = os.path.join(local_dir, f'ACF{uf}{str(year)[2:]}.delta')
        minio_client.download_file(bucket_name, s3_file_path, local_file_path)
        print(f"Arquivo baixado do MinIO para {local_file_path}")
    except Exception as e:
        print(f"Erro ao baixar o arquivo do MinIO: {e}")

regions = {
    'SUDESTE': ['MG', 'ES'],
}
anos = [2020]  

with DAG(
    dag_id='ETL_SIA_ACF',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 8, 14),
        'email_on_failure': False,
        'email_on_retry': False,
    },
    description='DAG para baixar dados do MinIO',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    configuracoes = PythonOperator(
        task_id='configuracoes',
        python_callable=lambda: None,
        provide_context=True,
    )
    configuracoes.ui_color = '#FF6600'

    for region, ufs in regions.items():
        for year in anos:
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
                    task_id=f'baixar_{region}_{uf}_{year}',
                    python_callable=baixar_dados,
                    op_kwargs={'region': region, 'uf': uf, 'year': year},
                    provide_context=True,
                )
                baixar_uf_task.ui_color = '#3366ff'
                baixar_region_task >> baixar_uf_task
                baixar_uf_tasks.append(baixar_uf_task)
