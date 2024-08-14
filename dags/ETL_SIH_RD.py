from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pysus.ftp.databases.sim import SIM
import os

def baixar_dados(region, ufs, year, **kwargs):
    sim = SIM()
    for uf in ufs:
        try:
            print(f"Baixando dados para UF {uf} e ano {year}")
            files = sim.get_files("CID10", uf, year)
            sim.download(files, local_dir='/home/jamilsonfs/airflow/dados/dados_parquet')
            print(f"Dados baixados para UF {uf} e ano {year}")
        except Exception as e:
            print(f"Erro ao baixar dados para UF {uf} e ano {year}: {e}")

regions = {
    'SUDESTE': ['SP', 'RJ', 'MG', 'ES'],  # Região Sudeste
}
ano = 2020

with DAG(
    dag_id='baixar_dados_SIM',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 8, 14),
        'email_on_failure': False,
        'email_on_retry': False,
    },
    description='DAG para baixar dados do SIM',
    schedule_interval='@daily',
    catchup=False,
) as dag:
    baixar_dados_task = PythonOperator(
        task_id='baixar_dados_task',
        python_callable=baixar_dados,
        op_kwargs={'region': 'SUDESTE', 'ufs': regions['SUDESTE'], 'year': ano},
        provide_context=True,
    )