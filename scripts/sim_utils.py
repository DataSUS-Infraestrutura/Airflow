import sys
from pysus.ftp.databases.sim import SIM
from enum import Enum

class UnidadeTempo(Enum):
    MINUTOS = 0
    HORAS = 1
    DIAS = 2
    MESES = 3
    ANOS = 4
    MAIS_DE_100_ANOS = 5

def tradutor_idade(codigo):
    unidade = int(codigo[0])
    quantidade = int(codigo[1:])

    if unidade == UnidadeTempo.MINUTOS.value and quantidade == 00:
        return "Idade não relatada"
    elif unidade == UnidadeTempo.MINUTOS.value:
        return f"{quantidade} minutos"
    elif unidade == UnidadeTempo.HORAS.value:
        return f"{quantidade} horas"
    elif unidade == UnidadeTempo.DIAS.value:
        return f"{quantidade} dias"
    elif unidade == UnidadeTempo.MESES.value:
        return f"{quantidade} meses"
    elif unidade == UnidadeTempo.ANOS.value and quantidade == 00:
        return "Menor de 1 ano, mas não se sabe o número de horas, dias ou meses"
    elif unidade == UnidadeTempo.ANOS.value:
        return f"{quantidade} anos"
    elif unidade == UnidadeTempo.MAIS_DE_100_ANOS.value:
        return f"{quantidade + 100} anos"
    else:
        return "Código inválido"

regions = {
    'SUL': ['PR']
}

def load_pysus_databases():
    try:
        sim = SIM().load()
        print("SIM e databases carregados com sucesso.")
        return {
            'sim': 'SIM - Sistema de Informação sobre Mortalidade'
        }
    except Exception as e:
        print(f"Ocorreu um erro ao carregar os bancos de dados: {e}")
        sys.exit(1)

def get_dataframe_for_uf(region, uf, **kwargs):
    try:
        sim = SIM().load()
        files = sim.get_files("CID10", uf, 2022)
        dfs = sim.download(files, local_dir='/home/jamilsonfs/airflow/dados/dados_parquet')
        result = {uf: [df.to_json() for df in dfs]}
        print(f"Dados para a UF {uf}: {result}")
        return result
    except Exception as e:
        print(f"Erro ao processar UF {uf}: {e}")
        return {uf: None}
