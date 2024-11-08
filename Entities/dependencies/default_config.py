from typing import Dict
from getpass import getuser

default:Dict[str, Dict[str,object]] = {
    'credential': {
        'crd': 'XRM_API_PRD'
    },
    'log': {
        'hostname': 'Patrimar-RPA',
        'port' : 80,
        'token': ''
    },
    "paths": {
        'file_save_path_tickets': f"C:/Users/{getuser()}/PATRIMAR ENGENHARIA S A/RPA - Documentos/RPA - Dados/XRM - Relacionamento Com Cliente/json/all_tickets.json",
        'file_save_path_empreendimentos': f"C:/Users/{getuser()}/PATRIMAR ENGENHARIA S A/RPA - Documentos/RPA - Dados/XRM - Relacionamento Com Cliente/json/empreendimentos.json",
        'file_save_path_unidades': f"C:/Users/{getuser()}/PATRIMAR ENGENHARIA S A/RPA - Documentos/RPA - Dados/XRM - Relacionamento Com Cliente/json/unidades.json",
        'file_save_clientes' : f'C:/Users/{getuser()}/PATRIMAR ENGENHARIA S A/RPA - Documentos/RPA - Dados/Relatorio_Imobme_Financeiro/ClientesContratos.json'
    }
}

