from Entities.apiXrm import ApiXrm
from Entities.dependencies.config import Config
from Entities.dependencies.credenciais import Credential
import pandas as pd
from Entities.contatos import Contatos

if __name__ == "__main__":
    crd:dict = Credential(Config()['credential']['crd']).load()
    api = ApiXrm(username=crd["user"], password=crd["password"], url=crd["url"])
    
    
    df = pd.read_excel(r'R:\#Prontos\ConsumoApiCRM_ORACLE\#material\ClientesContratos_Final.xlsx', dtype=str)
    for row, value in df.iterrows():
        contato = Contatos(value)
        print(contato)
        api.cadastrar_contatos(contato.payload())
        import json
        print(json.dumps(contato.payload()))
        break