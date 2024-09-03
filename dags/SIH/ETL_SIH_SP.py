from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from datetime import datetime
from pysus.ftp.databases.sih import SIH

def baixar_dados(region, uf, year, **kwargs):
    sih = SIH()
    try:
        print(f"Baixando dados para UF {uf} e ano {year} na região {region}")
        files = sih.get_files(region, uf, year)
        sih.download(files, local_dir='/home/jamilsonfs/airflow/dados/dados_parquet')
        print(f"Dados baixados para UF {uf} e ano {year} na região {region}")
    except Exception as e:
        print(f"Erro ao baixar dados para UF {uf} e ano {year} na região {region}: {e}")

regions = {
    'SUDESTE': ['SP', 'RJ', 'MG', 'ES'],
}
anos = [2020]  

with DAG(
    dag_id='ETL_SIH_SP',
    default_args={
        'owner': 'airflow',
        'depends_on_past': False,
        'start_date': datetime(2024, 8, 14),
        'email_on_failure': False,
        'email_on_retry': False,
    },
    description='DAG para baixar dados do SIH',
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