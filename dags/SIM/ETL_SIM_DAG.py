# Diretórios específicos
import sys
sys.path.append('/home/jamilsonfs/airflow/dags/SIM')
sys.path.append('/home/jamilsonfs/.local/lib/python3.10/site-packages')

# Bibliotecas Airflow
from airflow import DAG
from airflow.operators.dummy_operator import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.models import BaseOperator
from airflow.utils.decorators import apply_defaults

# Bibliotecas de tempo
from datetime import datetime, timedelta

# Bibliotecas PySUS
from pysus.utilities.readdbc import read_dbc
from pysus.ftp.databases.sim import SIM

# Bibliotecas PySpark e Delta Lake
import pyspark
from delta import *

# Manipulação de dados
import os
import pandas as pd
import json

# Outras bibliotecas
from enum import Enum
from atlasclient.client import Atlas
import requests

# Funções customizadas
from FUNCTIONS import (
    load_pysus_databases,
    transform_age_data,
    apply_group_transform,
    convert_parquet_to_delta,
    initial_verification,
    final_verification,
    check_atlas_connection,
)

default_args = {
    'owner': 'airflow',
    'depends_on_past': False,
    'start_date': datetime(2023, 1, 1),
    'email_on_failure': False,
    'email_on_retry': False,
    'retries': 1,
    'retry_delay': timedelta(minutes=5),
}

with DAG(
    'ETL_SIM_Melhorado',
    default_args=default_args,
    description='DAG para ETL do SIM usando Pysus e Delta Lake',
    schedule_interval=timedelta(days=1),
    catchup=False,
) as dag:

    start = DummyOperator(task_id='start')

    atlas_check = PythonOperator(
        task_id='check_atlas_connection',
        python_callable=check_atlas_connection,
    )

    load_pysus = PythonOperator(
        task_id='load_pysus_databases',
        python_callable=load_pysus_databases,
    )

    transform_age = PythonOperator(
        task_id='transform_age_data',
        python_callable=transform_age_data,
    )

    apply_transform = PythonOperator(
        task_id='apply_group_transform',
        python_callable=apply_group_transform,
    )

    convert_to_delta = PythonOperator(
        task_id='convert_parquet_to_delta',
        python_callable=convert_parquet_to_delta,
    )

    verify_initial = PythonOperator(
        task_id='initial_verification',
        python_callable=initial_verification,
    )

    verify_final = PythonOperator(
        task_id='final_verification',
        python_callable=final_verification,
    )

    end = DummyOperator(task_id='end')

    # Definindo a sequência de tarefas
    start >> atlas_check >> load_pysus >> verify_initial >> transform_age >> apply_transform >> convert_to_delta >> verify_final >> end
