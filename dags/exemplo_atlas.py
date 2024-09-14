from airflow import DAG
from airflow.operators.python_operator import PythonOperator
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.utils.dates import days_ago
from datetime import datetime
import json
import os

# Define o nome do arquivo a ser criado
file_name = "/tmp/teste_file.txt"

# Função para criar o arquivo .txt
def create_file():
    with open(file_name, "w") as f:
        f.write("Este é um arquivo de teste para Apache Atlas.")
    print(f"Arquivo {file_name} criado com sucesso.")

# Função para criar metadados e enviá-los para o Apache Atlas
def register_file_metadata():
    # Metadados do arquivo
    file_metadata = {
        "entity": {
            "typeName": "file_metadata",
            "attributes": {
                "qualifiedName": file_name,
                "name": "TESTE",
                "description": "Arquivo de teste registrado no Apache Atlas",
                "owner": "airflow_user",
                "filePath": file_name,
                "fileSize": os.path.getsize(file_name),
                "createTime": datetime.now().isoformat()
            },
            "status": "ACTIVE"
        }
    }

    # Configurações da requisição HTTP
    headers = {
        "Content-Type": "application/json",
        "Accept": "application/json"
    }

    # Envia a requisição para o Atlas
    response = requests.post(
        url="http://your-atlas-host:21000/api/atlas/v2/entity",
        headers=headers,
        data=json.dumps(file_metadata),
        auth=('admin', 'admin')
    )

    if response.status_code == 200:
        print("Metadados registrados com sucesso no Apache Atlas.")
    else:
        print(f"Erro ao registrar metadados no Apache Atlas: {response.status_code} - {response.text}")

# Definição do DAG
default_args = {
    'owner': 'airflow',
    'start_date': days_ago(1),
    'retries': 1
}

dag = DAG(
    'register_test_file_metadata',
    default_args=default_args,
    description='Cria um arquivo e registra seus metadados no Apache Atlas',
    schedule_interval=None,
)

# Tarefas do DAG
create_file_task = PythonOperator(
    task_id='create_file',
    python_callable=create_file,
    dag=dag,
)

register_metadata_task = PythonOperator(
    task_id='register_file_metadata',
    python_callable=register_file_metadata,
    dag=dag,
)

# Ordem de execução das tarefas
create_file_task >> register_metadata_task
