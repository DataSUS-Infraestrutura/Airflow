import requests
import json
import random
import pandas as pd
import math
import os
from datetime import datetime

# Configurações
ATLAS_URL = "http://10.100.100.61:21000/api/atlas/v2"
USERNAME = "admin"
PASSWORD = "admin"


#Scripts para a criação de Entidades
def getEntityColumn(column, columns_entities):
    return { "guid": columns_entities[column.upper()] } 

def tamanho_arquivo_em_mb(caminho_arquivo):
    tamanho_bytes = os.path.getsize(caminho_arquivo)
    tamanho_mb = tamanho_bytes / (1024 * 1024)
    return f"{tamanho_mb:.2f} MB"

def obter_nome_estado(sigla):
    return {
        'BR': 'Brasil',
        "AC": "Acre",
        "AL": "Alagoas",
        "AP": "Amapá",
        "AM": "Amazonas",
        "BA": "Bahia",
        "CE": "Ceará",
        "DF": "Distrito Federal",
        "ES": "Espírito Santo",
        "GO": "Goiás",
        "MA": "Maranhão",
        "MT": "Mato Grosso",
        "MS": "Mato Grosso do Sul",
        "MG": "Minas Gerais",
        "PA": "Pará",
        "PB": "Paraíba",
        "PR": "Paraná",
        "PE": "Pernambuco",
        "PI": "Piauí",
        "RJ": "Rio de Janeiro",
        "RN": "Rio Grande do Norte",
        "RS": "Rio Grande do Sul",
        "RO": "Rondônia",
        "RR": "Roraima",
        "SC": "Santa Catarina",
        "SP": "São Paulo",
        "SE": "Sergipe",
        "TO": "Tocantins"
    }[sigla]


#types
nome_data_repository = 'dt_data_repository'
nome_conjunto_base_dados = 'dt_database'
nome_base_dados = "db_file"
nome_coluna_base_dados = "dt_table_column"
nome_lineage_table = 'dt_anual_table'
nome_citacao = "dt_references"
nome_files = "dt_file"
nome_csv = 'dt_csv'
nome_processo_alteracao_colunas = 'dt_column_change_process'
nome_arquivo_table = 'dt_table_file'
nome_timeline_table = 'dt_timeline'

author_entities = 'Jamilson'

end_table_to_column = ('columns_table', 'belongs_to_table')
end_lineage_to_column = ('columns_anual_table', 'belongs_to_table_anual')
end_table_to_reference = ('references', 'is_reference_table')
end_table_to_file = ('files', 'is_file_table')
end_table_to_documentation_file = ('documentation_files', 'is_files_documentation_tables')
end_database_to_table = ('tables', 'belongs_database')
end_database_to_source = ('sources', 'is_database_source')
end_file_to_column_lineage = ('columns_file_table', 'is_column_table_file')
end_timeline_to_table = ('table_interval', 'belongs_timeline')
end_repository_data = ('database_repository', 'belongs_data_repository')

base_dados_type = {
  "enumDefs": [],
  "structDefs": [],
  "classificationDefs": [],
  "relationshipDefs": [
        {
            "name": "rsh_table_column",
            "relationshipCategory": "COMPOSITION",
            "propagateTags": "NONE",
            "endDef1": {
                "type": nome_base_dados,
                "name": end_table_to_column[0],
                "cardinality": "SET",
                "isContainer": True,
                "isLegacyAttribute": False
            },
            "endDef2": {
                "type": nome_coluna_base_dados,
                "name": end_table_to_column[1],
                "cardinality": "SINGLE",
                "isContainer": False,
                "isLegacyAttribute": False
            }
        },
        {
            "name": "rsh_table_column_lineage",
            "relationshipCategory": "AGGREGATION",
            "propagateTags": "NONE",
            "endDef1": {
                "type": nome_lineage_table,
                "name": end_lineage_to_column[0],
                "cardinality": "SET",
                "isContainer": True,
                "isLegacyAttribute": False
            },
            "endDef2": {
                "type": nome_coluna_base_dados,
                "name": end_lineage_to_column[1],
                "cardinality": "SET",
                "isContainer": False,
                "isLegacyAttribute": False
            }
        },
        {
            "name": "rsh_references_table",
            "relationshipCategory": "AGGREGATION",
            "propagateTags": "NONE",
            "endDef1": {
                "type": nome_base_dados,
                "name": end_table_to_reference[0],
                "cardinality": "SET",
                "isContainer": True,
                "isLegacyAttribute": False
            },
            "endDef2": {
                "type": nome_citacao,
                "name": end_table_to_reference[1],
                "cardinality": "SET",
                "isContainer": False,
                "isLegacyAttribute": False
            }
        },
        {
            "name": "rsh_files_table",
            "relationshipCategory": "COMPOSITION",
            "propagateTags": "NONE",
            "endDef1": {
                "type": nome_base_dados,
                "name": end_table_to_file[0],
                "cardinality": "SET",
                "isContainer": True,
                "isLegacyAttribute": False
            },
            "endDef2": {
                "type": nome_arquivo_table,
                "name": end_table_to_file[1],
                "cardinality": "SINGLE",
                "isContainer": False,
                "isLegacyAttribute": False
            }
        },
        {
            "name": "rsh_documentation_files",
            "relationshipCategory": "AGGREGATION",
            "propagateTags": "NONE",
            "endDef1": {
                "type": nome_base_dados,
                "name": end_table_to_documentation_file[0],
                "cardinality": "SET",
                "isContainer": True,
                "isLegacyAttribute": False
            },
            "endDef2": {
                "type": nome_files,
                "name": end_table_to_documentation_file[1],
                "cardinality": "SET",
                "isContainer": False,
                "isLegacyAttribute": False
            }
        },
        {
            "name": "rsh_database_tables",
            "relationshipCategory": "COMPOSITION",
            "propagateTags": "NONE",
            "endDef1": {
                "type": nome_conjunto_base_dados,
                "name": end_database_to_table[0],
                "cardinality": "SET",
                "isContainer": True,
                "isLegacyAttribute": False
            },
            "endDef2": {
                "type": nome_base_dados,
                "name": end_database_to_table[1],
                "cardinality": "SINGLE",
                "isContainer": False,
                "isLegacyAttribute": False
            }
        },
        {
            "name": "rsh_database_sources",
            "relationshipCategory": "AGGREGATION",
            "propagateTags": "NONE",
            "endDef1": {
                "type": nome_conjunto_base_dados,
                "name": end_database_to_source[0],
                "cardinality": "SET",
                "isContainer": True,
                "isLegacyAttribute": False
            },
            "endDef2": {
                "type": nome_citacao,
                "name": end_database_to_source[1],
                "cardinality": "SET",
                "isContainer": False,
                "isLegacyAttribute": False
            }
        },
        {
            "name": "rsh_columns_table_file",
            "relationshipCategory": "AGGREGATION",
            "propagateTags": "NONE",
            "endDef1": {
                "type": nome_arquivo_table,
                "name": end_file_to_column_lineage[0],
                "cardinality": "SINGLE",
                "isContainer": True,
                "isLegacyAttribute": False
            },
            "endDef2": {
                "type": nome_lineage_table,
                "name": end_file_to_column_lineage[1],
                "cardinality": "SET",
                "isContainer": False,
                "isLegacyAttribute": False
            }
        },
        {
            "name": "rsh_table_timeline",
            "relationshipCategory": "AGGREGATION",
            "propagateTags": "NONE",
            "endDef1": {
                "type": nome_timeline_table,
                "name": end_timeline_to_table[0],
                "cardinality": "SET",
                "isContainer": True,
                "isLegacyAttribute": False
            },
            "endDef2": {
                "type": nome_processo_alteracao_colunas,
                "name": end_timeline_to_table[1],
                "cardinality": "SINGLE",
                "isContainer": False,
                "isLegacyAttribute": False
            }
        },
        {
            "name": "rsh_repository_database",
            "relationshipCategory": "COMPOSITION",
            "propagateTags": "NONE",
            "endDef1": {
                "type": nome_data_repository,
                "name": end_repository_data[0],
                "cardinality": "SET",
                "isContainer": True,
                "isLegacyAttribute": False
            },
            "endDef2": {
                "type": nome_conjunto_base_dados,
                "name": end_repository_data[1],
                "cardinality": "SINGLE",
                "isContainer": False,
                "isLegacyAttribute": False
            }
        },
    ],
  "entityDefs": [
    {
      "name": nome_citacao,
      "description": "Contém informações de referência sobre diferentes fontes relacionadas a temas variados, facilitando o rastreamento de dados e metadados utilizados em outras entidades.",
      "superTypes": ["DataSet"],
      "attributeDefs": [
        {
          "name": "url",
          "typeName": "string",
          "isOptional": True
        }
      ]  
    },
    {
      "name": nome_files,
      "description": "Armazena metadados sobre arquivos genéricos, fornecendo informações como tamanho, extensão e localização do arquivo.",
      "superTypes": ["DataSet"],
      "attributeDefs": [
        {
          "name": "file_size",
          "typeName": "string",
          "isOptional": True
        },
        {
          "name": "extension",
          "typeName": "string",
          "isOptional": True
        },
        {
          "name": "location",
          "typeName": "string",
          "isOptional": True
        }
      ]
    },
    {
      "name": nome_coluna_base_dados,
      "description": "Representa metadados sobre uma coluna específica de uma tabela em uma base de dados, com atributos como chave primária, domínio, tipo de dado, observações e características específicas.",
      "superTypes": ["DataSet"],
      "attributeDefs": [
        {
          "name": "primary_key",
          "typeName": "boolean",
          "isOptional": True,
          "isUnique": False,
          "isIndexable": False
        },
        {
          "name": "domain",
          "typeName": "string",
          "isOptional": True,
          "isUnique": False,
          "isIndexable": False
        },
        {
          "name": "type",
          "typeName": "string",
          "isOptional": True,
          "isUnique": False,
          "isIndexable": False
        },
        {
          "name": "observation",
          "typeName": "string",
          "isOptional": True,
          "isUnique": False,
          "isIndexable": False
        },
        {
          "name": "characteristics",
          "typeName": "string",
          "isOptional": True,
          "isUnique": False,
          "isIndexable": False
        }
      ]
    },
    {
      "name": nome_lineage_table,
      "description": "Captura o estado de uma tabela em um determinado período de tempo, com foco na evolução das colunas e outros metadados.",
      "superTypes": ["DataSet"],
      "attributeDefs": [
         {
          "name": "year",
          "typeName": f"int",
          "isOptional": True,
        },
      ]
    },
    {
      "name": nome_base_dados,
      "description": "Armazena dados e metadados detalhados sobre uma tabela específica de uma base de dados, incluindo acrônimos, referências externas, arquivos e estrutura das colunas.",
      "superTypes": ["DataSet"],
      "attributeDefs": [
        {
          "name": "acronymus",
          "typeName": "string",
          "isOptional": True
        },
      ]
    },
    {
      "name": nome_conjunto_base_dados,
      "description": "Define metadados gerais sobre uma base de dados, incluindo acrônimos, fontes de dados e agrupamentos de tabelas associadas.",
      "superTypes": ["DataSet"],
      "attributeDefs": [
        {
          "name": "acronymus",
          "typeName": "string",
          "isOptional": True
        },
      ]
    },
    {
      "name": nome_csv,
      "description": "Descreve os metadados específicos de arquivos CSV, com detalhes sobre a estrutura de colunas e os dados armazenados.",
      "superTypes": [nome_files],
      "attributeDefs": [
        {
          "name": "columns",
          "typeName": "array<string>",
          "isOptional": True
        }
      ]  
    },
    {
      "name": nome_arquivo_table,
      "description": "Armazena dados sobre um arquivo que contém uma tabela, incluindo colunas, ano de referência, estado da tabela e o total de linhas.",
      "superTypes": [nome_files],
      "attributeDefs": [
        {
          "name": "year",
          "typeName": "int",
          "isOptional": True,
          "cardinality": "SINGLE",
          "valuesMinCount": 1,
          "valuesMaxCount": 1,
          "isUnique": False,
          "isIndexable": False
        },
        {
          "name": "state",
          "typeName": "string",
          "isOptional": True,
          "cardinality": "SINGLE",
          "valuesMinCount": 1,
          "valuesMaxCount": 1,
          "isUnique": False,
          "isIndexable": False
        },
        {
          "name": "total_lines",
          "typeName": "int",
          "isOptional": True,
          "cardinality": "SINGLE",
          "valuesMinCount": 1,
          "valuesMaxCount": 1,
          "isUnique": False,
          "isIndexable": False
        },
      ]  
    },
    {
      "name": nome_processo_alteracao_colunas,
      "description": "Descreve o processo de alteração estrutural das colunas de uma tabela, incluindo a adição e remoção de colunas.",
      "superTypes": ['Process'],
      "attributeDefs": [
         {
            "name": "added_columns",
            "typeName": f"array<{nome_coluna_base_dados}>",
            "isOptional": True,
        },
        {
          "name": "deleted_columns",
          "typeName": f"array<{nome_coluna_base_dados}>",
          "isOptional": True,
        }
      ]
    },
    {
      "name": nome_timeline_table,
      "description": "Linha do tempo de uma tabela.",
      "superTypes": ["DataSet"],
      "attributeDefs": []
    },
    {
      "name": nome_data_repository,
      "description": "Repositório de dados de banco de dados",
      "superTypes": ["DataSet"],
      "attributeDefs": []
    }
  ]
}

url = f"{ATLAS_URL}/types/typedefs"
headers = {
    "Content-Type": "application/json",
    "Authorization": "Basic YWRtaW46YWRtaW4="
}

response = requests.post(url, headers=headers, data=json.dumps(base_dados_type ))

if response.status_code == 200:
    print("Types created successfully")
else:
    print(f"Failed to create entity: {response.status_code} - {response.text}")

#repositorio dos dados 
data_repository = { 
      "typeName": nome_data_repository,
      "qualifiedName": f"{nome_data_repository}@DataSUS",
      "attributes": {
          "qualifiedName": f"{nome_data_repository}@DataSUS",
          "name": "Sistema de Informações sobre Mortalidade ",
          'owner': author_entities,
          'description': 'O DATASUS é o Departamento de Informática do Sistema Único de Saúde (SUS) no Brasil, responsável por gerenciar e disponibilizar informações de saúde essenciais para a formulação de políticas públicas e para a gestão do sistema de saúde no país. Criado em 1991, o DATASUS opera sob a coordenação do Ministério da Saúde e desempenha um papel crucial na coleta, armazenamento, processamento e disseminação de dados relacionados à saúde.',
      }
}