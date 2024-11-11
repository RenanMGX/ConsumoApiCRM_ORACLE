from Entities.apiXrm import ApiXrm
from Entities.dependencies.config import Config
from Entities.dependencies.credenciais import Credential
import pandas as pd
from Entities.contatos import Contatos
from Entities.apartamento import Apartamento
from Entities.dependencies.arguments import Arguments
from datetime import datetime
from dateutil.relativedelta import relativedelta
from Entities.dependencies.logs import Logs, traceback
from Entities.dependencies.functions import P
import os
import json

class RegistroCPFCadastrados:
    file_path = os.path.join(os.getcwd(), 'lista_cpf_cadastrados.json')
    if not os.path.exists(file_path):
        with open(file_path, 'w')as _file:
            json.dump([], _file)
    
    @staticmethod
    def load() -> list:
        with open(RegistroCPFCadastrados.file_path, 'r')as _file:
            return json.load(_file)
        
    @staticmethod
    def add(value) -> None:
        reg = RegistroCPFCadastrados.load()
        reg.append(value)
        with open(RegistroCPFCadastrados.file_path, 'w')as _file:
            json.dump(reg, _file)

          

class Execute:
    @staticmethod
    def start():
        crd:dict = Credential(Config()['credential']['crd']).load()
        api = ApiXrm(username=crd["user"], password=crd["password"], url=crd["url"])
        apartamento = Apartamento(Config()['paths']['file_save_path_unidades'])
        
        df = pd.read_json(Config()['paths']['file_save_clientes'])
        df['Data Criação'] = pd.to_datetime(df['Data Criação'], errors='coerce', format='%Y-%m-%dT%H:%M:%S.%f')
        df = df[
            (df['Data Criação'] >= datetime(2024,1,1)) &
            (df['Principal (Sim ou Não)'] == "Não") &
            (~df['CPF/ CNPJ'].isin(RegistroCPFCadastrados.load())) 
        ]
        num_cadastros:int = 0
        for row, value in df.iterrows():
            try:
                contato = Contatos(value)
            
                apartamento_id = apartamento.find(
                    empreendimento=contato.Empreendimento,
                    bloco=contato.Bloco,
                    unidade=contato.Unidade
                )
                
                result = api.cadastrar_contatos(contato.payload())
                if not result.status_code == 201:
                    if "Já existe um contato com o CPF inserido".lower() in result.text.lower():
                        print(P(f"{contato} já esta cadastrado", color='red'))
                        RegistroCPFCadastrados.add(str(value['CPF/ CNPJ']))
                        continue
                    raise Exception(f"erro ao cadastrar contato: {result.status_code=}; {result.reason}; {result.text}")
                else:
                    print(f"cadastrado:{result.json().get("PartyNumber")}")
                
                data:dict = result.json()
                contato.PartyNumber = data['PartyNumber']
                
                if apartamento_id != -1:
                    result = api.registrar_unidade(
                        PartyNumber=contato.PartyNumber,
                        apartamento_id=apartamento_id,
                        principal_comprador=contato.Principal_Comprador
                    )
                
                    if not result.status_code == 201:
                        raise Exception(f"erro ao registrar unidade para o {contato=}: {result.status_code=}; {result.reason}; {result.text}")
                    
                RegistroCPFCadastrados.add(str(value['CPF/ CNPJ']))
                
                num_cadastros += 1
                               
            except Exception as err:
                print(P(err))
                Logs(name="Cadastro Segundo Preponente").register(status='Error', description=f"cpf:{df['CPF/ CNPJ']} - {str(err)}", exception=traceback.format_exc())
                continue
        
        Logs(name="Cadastro Segundo Preponente").register(status='Concluido', description=f"Automação Finalizada! '{num_cadastros}' contados registrados!")
        
        


if __name__ == "__main__":
    Arguments({
        'start' : Execute.start
    })