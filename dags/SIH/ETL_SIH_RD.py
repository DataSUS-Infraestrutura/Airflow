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
        
        # Chamar a função para registrar metadados no Atlas
        registrar_metadados_atlas(region, uf, year)
        
    except Exception as e:
        print(f"Erro ao baixar dados para UF {uf} e ano {year} na região {region}: {e}")

def registrar_metadados_atlas(region, uf, year):
    # Conectar ao Apache Atlas usando o AtlasHook
    atlas_hook = AtlasHook(atlas_conn_id='apache_atlas_default')
    
    # Definir a entidade de metadados para o Atlas
    entity = {
        "typeName": "airflow_process",
        "attributes": {
            "qualifiedName": f"baixar_{region}_{uf}_{year}@my_cluster",
            "name": f"baixar_{region}_{uf}_{year}",
            "description": f"Processo de ETL para baixar dados do SIH para {uf} no ano {year} na região {region}",
            "inputs": [
                {
                    "typeName": "airflow_dataset",
                    "uniqueAttributes": {
                        "qualifiedName": f"sih_data@my_cluster"
                    }
                }
            ],
            "outputs": [
                {
                    "typeName": "airflow_dataset",
                    "uniqueAttributes": {
                        "qualifiedName": f"/home/jamilsonfs/airflow/dados/dados_parquet/{region}_{uf}_{year}.parquet@my_cluster"
                    }
                }
            ]
        }
    }
    
    # Criar ou atualizar a entidade no Atlas
    atlas_hook.create_or_update(entity)

regions = {
    'SUDESTE': ['SP', 'RJ', 'MG', 'ES'],
}
anos = [2020]  

with DAG(
    dag_id='ETL_SIH_RD',
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
        python_callable=lambda: None,  # Placeholder task
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
