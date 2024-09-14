import requests

# Configurações
atlas_base_url = "http://10.100.100.61:21000/api/atlas/v2"
headers = {
    'Content-Type': 'application/json',
    'Authorization': 'Bearer <your-token>'
}

# Criação de Tipo
type_def = {
    "entityDefs": [
        {
            "name": "file_si",
            "typeVersion": "1.0",
            "superTypes": [
                "DataSet"
            ],
            "attributeDefs": [
                {
                    "name": "description",
                    "typeName": "string",
                    "isOptional": True
                }
            ]
        }
    ],
    "relationshipDefs": []
}

response = requests.post(f"{atlas_base_url}/types/typedefs", json=type_def, headers=headers)
print(response.json())

# Criação de Entidade
entity = {
    "entities": [
        {
            "typeName": "my_custom_type",
            "attributes": {
                "name": "Sample Dataset",
                "description": "This is a sample dataset"
            }
        }
    ]
}

response = requests.post(f"{atlas_base_url}/entity", json=entity, headers=headers)
print(response.json())
